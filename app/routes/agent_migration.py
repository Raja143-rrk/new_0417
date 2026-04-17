from datetime import datetime
import json
import logging
from queue import Queue
import re
import threading
import traceback
from uuid import uuid4

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from app.agents import rag_agent
from app.agents.migration_agent import (
    SQL_BUNDLE_DELIMITER,
    build_mysql_trigger_to_snowflake_fallback_sql,
    generate_repaired_sql,
    generate_transformed_sql,
    get_llm_runtime_info,
    review_transformed_sql,
    validate_mysql_trigger_to_snowflake_bundle,
)
from app.models.request_models import (
    AgentMigrationRequest,
    BulkAgentMigrationRequest,
    CapabilityRequest,
    ConnectionTestRequest,
    JobScheduleRequest,
    MetadataRequest,
    SqlMigrationRequest,
)
from app.models.response_models import (
    JobScheduleConfig,
    MigrationJobSummary,
    MigrationObjectStats,
    MigrationRunStats,
    MigrationRunSummary,
    ObjectCounters,
    RagAgentStatusResponse,
    SqlMigrationResponse,
)
from app.agents.rag_agent import get_rag_agent_runtime_info
from app.adapters.registry import get_adapter
from app.services.db_connector import get_connection
from app.services.metadata_service import (
    get_object_summary,
    list_databases,
    list_objects,
    list_schemas,
)
from app.services.data_migration import (
    drop_target_table,
    get_table_count_summary,
    migrate_table_data,
    target_table_exists,
    truncate_target_table,
)
from app.services.extractor import extract_table_ddl
from app.services.migration_history import (
    append_migration_run,
    get_migration_run,
    list_migration_runs,
)
from app.services.migration_diagnostics import (
    append_migration_diagnostics,
    get_migration_diagnostics,
    list_migration_diagnostics,
)
from app.services.job_schedule import (
    get_job_schedule,
    list_job_schedules,
    record_schedule_event,
    update_job_schedule,
    upsert_job_schedule,
)
from app.services.job_scheduler import trigger_job_run
from app.services.migration_orchestrator import migrate_sql
from app.services.deterministic_transformer import transform_deterministically
from app.services.deterministic_transformer import supports_deterministic_transform
from app.services.sql_validator import validate
from app.services.target_validators import validate_target_sql_semantics

logger = logging.getLogger(__name__)
from app.utils.rule_loader import get_error_repair_rules, get_unsupported_object_rule

router = APIRouter()
SHOW_SQL_LOGS = False

OBJECT_EXECUTION_ORDER = [
    "sequence",
    "synonym",
    "table",
    "view",
    "function",
    "storedprocedure",
    "trigger",
    "event",
    "cursor",
]
SQL_REPAIR_ATTEMPTS = 2
_run_controls = {}
_run_controls_lock = threading.Lock()
DEPENDENCY_BLOCKERS = {
    "view": {"table"},
    "function": {"table", "view"},
    "storedprocedure": {"table", "view", "function"},
    "trigger": {"table", "view", "function", "storedprocedure"},
}


def _register_run_control(run_id):
    with _run_controls_lock:
        stop_event = threading.Event()
        _run_controls[run_id] = stop_event
        return stop_event


def _get_run_control(run_id):
    with _run_controls_lock:
        return _run_controls.get(run_id)


def _clear_run_control(run_id):
    with _run_controls_lock:
        _run_controls.pop(run_id, None)


def _build_stats_by_type(object_types):
    return {object_type: ObjectCounters() for object_type in object_types}


def _build_object_stats_result(
    run_id,
    result,
    source_db,
    target_db,
    started_at,
    completed_at,
):
    return MigrationObjectStats(
        run_id=run_id,
        object_name=result["object_name"],
        object_type=result["object_type"],
        status=result["status"],
        source_db=source_db,
        target_db=target_db,
        rows_migrated=result.get("rows_migrated", 0),
        source_row_count=result.get("source_row_count"),
        target_row_count=result.get("target_row_count"),
        missing_row_count=result.get("missing_row_count"),
        retry_count=result.get("retry_count", 0),
        error_type=result.get("error_type"),
        error_message=result.get("error"),
        remediation=result.get("remediation"),
        transformed_sql=result.get("transformed_sql"),
        started_at=started_at,
        completed_at=completed_at,
    )


def _build_run_stats(object_results, known_types):
    stats = MigrationRunStats(by_type=_build_stats_by_type(known_types))
    for item in object_results:
        counters = stats.by_type.setdefault(item.object_type, ObjectCounters())
        counters.total += 1
        stats.total_objects += 1
        stats.total_rows_migrated += item.rows_migrated
        stats.total_source_rows += item.source_row_count or 0
        stats.total_target_rows += item.target_row_count or 0
        stats.total_missing_rows += item.missing_row_count or 0
        stats.total_retries += item.retry_count
        if item.status == "success":
            counters.success += 1
            stats.success_objects += 1
        elif item.status == "error":
            counters.error += 1
            stats.error_objects += 1
        elif item.status == "skipped":
            counters.skipped += 1
            stats.skipped_objects += 1
    return stats


def _build_run_summary(
    run_id,
    status,
    source_db,
    target_db,
    execution_order,
    object_results,
    started_at,
    completed_at,
):
    return MigrationRunSummary(
        run_id=run_id,
        status=status,
        source_db=source_db,
        target_db=target_db,
        execution_order=execution_order,
        stats=_build_run_stats(object_results, OBJECT_EXECUTION_ORDER),
        object_results=object_results,
        started_at=started_at,
        completed_at=completed_at,
    )


def _build_history_record(run_summary, logs, results=None, summary=None):
    return {
        "run_summary": run_summary.model_dump(mode="json"),
        "logs": logs,
        "results": results or [],
        "summary": summary or {},
    }


def _build_queries_payload(record):
    if not isinstance(record, dict):
        return {"full_script": "", "objects": []}

    raw_results = record.get("results") or []
    if not raw_results:
        raw_results = (record.get("run_summary") or {}).get("object_results") or []
    if not raw_results:
        raw_results = record.get("transformed_queries") or []
    objects = []
    for item in raw_results:
        query = str((item or {}).get("query") or (item or {}).get("transformed_sql") or "").strip()
        if not query:
            continue
        objects.append(
            {
                "name": str((item or {}).get("name") or (item or {}).get("object_name") or "").strip(),
                "type": str((item or {}).get("type") or (item or {}).get("object_type") or "").strip(),
                "query": query,
            }
        )

    full_script = "\n\n".join(
        f"-- {entry['type'] or 'object'}: {entry['name'] or 'unnamed'}\n{entry['query']}"
        for entry in objects
    ).strip()
    return {
        "full_script": full_script,
        "objects": objects,
    }


def _build_diagnostics_record(
    run_id,
    mode,
    source_db,
    target_db,
    execution_order,
    results,
    started_at,
    completed_at,
):
    return {
        "run_id": run_id,
        "mode": mode,
        "source_db": source_db,
        "target_db": target_db,
        "execution_order": execution_order,
        "started_at": started_at.isoformat() if isinstance(started_at, datetime) else str(started_at),
        "completed_at": completed_at.isoformat() if isinstance(completed_at, datetime) else str(completed_at),
        "objects": [
            {
                "object_type": item.get("object_type"),
                "object_name": item.get("object_name"),
                "status": item.get("status"),
                "error_type": item.get("error_type"),
                "remediation": item.get("remediation"),
                "diagnostics": item.get("diagnostics"),
            }
            for item in (results or [])
        ],
    }


def _serialize_request(req, mode, parent_job_id=None, trigger_type="manual", event_name=None):
    payload = req.model_dump(mode="json")
    return {
        "mode": mode,
        "payload": payload,
        "parent_job_id": parent_job_id,
        "trigger_type": trigger_type,
        "event_name": event_name,
        "captured_at": datetime.utcnow().isoformat(),
    }


def _collect_transformed_queries(results):
    items = []
    for result in results or []:
        transformed_sql = str((result or {}).get("transformed_sql") or "").strip()
        if not transformed_sql:
            continue
        items.append(
            {
                "object_name": str((result or {}).get("object_name") or ""),
                "object_type": str((result or {}).get("object_type") or ""),
                "transformed_sql": transformed_sql,
            }
        )
    return items


def _with_job_request(history_record, job_request):
    history_record["job_request"] = job_request
    return history_record


def _parse_datetime_or_none(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _build_job_summary(record, schedule_record=None):
    run_summary = record.get("run_summary") or {}
    task_records = run_summary.get("object_results") or []
    tasks = [MigrationObjectStats(**task_record) for task_record in task_records]
    stats = run_summary.get("stats") or {}
    schedule = JobScheduleConfig(**schedule_record) if schedule_record else None
    return MigrationJobSummary(
        job_id=run_summary.get("run_id"),
        run_id=run_summary.get("run_id"),
        job_name=f"{run_summary.get('source_db', 'Unknown')} -> {run_summary.get('target_db', 'Unknown')}",
        status=run_summary.get("status", "success"),
        source_db=run_summary.get("source_db", ""),
        target_db=run_summary.get("target_db", ""),
        task_count=len(task_records),
        successful_tasks=int(stats.get("success_objects", 0)),
        failed_tasks=int(stats.get("error_objects", 0)),
        skipped_tasks=int(stats.get("skipped_objects", 0)),
        execution_order=run_summary.get("execution_order") or [],
        started_at=_parse_datetime_or_none(run_summary.get("started_at")) or datetime.utcnow(),
        completed_at=_parse_datetime_or_none(run_summary.get("completed_at")) or datetime.utcnow(),
        schedule=schedule,
        tasks=tasks,
    )


def rerun_saved_job(job_id: str, trigger_type: str = "manual", event_name: str | None = None):
    record = get_migration_run(job_id)
    if not record:
        raise ValueError(f"Job not found: {job_id}")
    job_request = record.get("job_request") or {}
    mode = job_request.get("mode")
    payload = job_request.get("payload") or {}
    if mode == "single":
        req = AgentMigrationRequest(**payload)
        return _run_agent_migrate(
            req,
            job_context={
                "parent_job_id": job_id,
                "trigger_type": trigger_type,
                "event_name": event_name,
            },
        )
    if mode == "bulk":
        req = BulkAgentMigrationRequest(**payload)
        return _run_agent_migrate_bulk(
            req,
            job_context={
                "parent_job_id": job_id,
                "trigger_type": trigger_type,
                "event_name": event_name,
            },
        )
    raise ValueError(f"Saved job {job_id} does not contain replay metadata.")


def _build_execution_order(selected_types):
    ordered = [item for item in OBJECT_EXECUTION_ORDER if item in selected_types]
    remaining = sorted(item for item in selected_types if item not in OBJECT_EXECUTION_ORDER)
    return ordered + remaining


def _build_error_guidance(target_db, execution_error, object_type):
    error_text = str(execution_error)
    error_text_lower = error_text.lower()
    target_name = str(target_db)
    hints = []

    if "MySQL" in target_name and "Invalid default value" in error_text:
        hints.append(
            "Fix invalid MySQL defaults, especially DATETIME/TIMESTAMP defaults."
        )
    if "MySQL" in target_name and "error in your SQL syntax" in error_text:
        hints.append("Return a single MySQL-compatible statement with correct delimiters.")
    if "Snowflake" in target_name and "does not exist or not authorized" in error_text:
        hints.append(
            "Fix object references, fully qualified names, and routine signatures for Snowflake."
        )
    if "Snowflake" in target_name and object_type == "table" and "does not exist or not authorized" in error_text:
        hints.append(
            "The repaired SQL must create the exact target table name in the active target database and schema. Do not invent placeholder database names such as MYDB, MY_DATABASE, DEMO_DB, or YOUR_DATABASE."
        )
    if "Snowflake" in target_name and object_type == "table" and "invalid identifier" in error_text:
        hints.append(
            "Preserve the source table column names needed for data copy. Do not rename or drop source columns unless the target dialect truly requires it."
        )
    if (
        "Snowflake" in target_name
        and object_type == "storedprocedure"
        and any(token in error_text for token in ["RESULTSET", "JAVASCRIPT", "unexpected 'END'", "unexpected 'SQL'"])
    ):
        hints.append(
            "Return one valid Snowflake CREATE OR REPLACE PROCEDURE statement. Do not mix LANGUAGE SQL and LANGUAGE JAVASCRIPT, and do not emit invalid RESULTSET declarations."
        )
    if "Snowflake" in target_name and object_type == "trigger":
        hints.append(
            "Return a Snowflake stream/task bundle for trigger migration. Do not emit CREATE TRIGGER."
        )
    if object_type == "view":
        hints.append(
            "Ensure all referenced tables or views already exist in the target schema."
        )

    for rule in get_error_repair_rules(target_db):
        contains_tokens = [str(token).lower() for token in rule.get("contains", [])]
        rule_object_type = rule.get("object_type")
        if contains_tokens and not all(token in error_text_lower for token in contains_tokens):
            continue
        if rule_object_type and rule_object_type != object_type:
            continue
        hint = str(rule.get("hint") or "").strip()
        if hint and hint not in hints:
            hints.append(hint)

    return " ".join(hints).strip()


def _classify_error(error_text):
    text = (error_text or "").lower()
    if "not authorized" in text or "permission" in text or "access denied" in text:
        return "authorization"
    if (
        "does not exist" in text
        or "doesn't exist" in text
        or "unknown table" in text
        or "unknown column" in text
        or "base table or view not found" in text
    ):
        return "dependency"
    if "syntax" in text or "compilation error" in text or "parse" in text:
        return "syntax"
    if "invalid default value" in text or "data type" in text or "datatype" in text:
        return "transformation"
    if "timeout" in text or "network" in text or "connection" in text:
        return "connectivity"
    return "execution"


def _build_remediation_hint(target_db, object_type, error_text):
    guidance = _build_error_guidance(target_db, error_text, object_type)
    error_type = _classify_error(error_text)
    generic = {
        "authorization": "Verify target object privileges and source object visibility.",
        "dependency": "Migrate prerequisite objects first and validate referenced object names.",
        "syntax": "Review generated target SQL for dialect-specific syntax and delimiters.",
        "transformation": "Review type/default conversions for the target database.",
        "connectivity": "Check network connectivity and database session health.",
        "execution": "Inspect the exact database error and generated SQL for this object.",
    }[error_type]
    return " ".join(part for part in [generic, guidance] if part).strip()


def _build_skipped_result(object_type, object_name, reason):
    return {
        "status": "skipped",
        "object_type": object_type,
        "object_name": object_name,
        "logs": [f"{datetime.now()} - SKIPPED: {reason}"],
        "error_type": "dependency",
        "remediation": reason,
        "retry_count": 0,
        "rows_migrated": 0,
    }


def _is_retryable_table_dependency_error(result, target_db_type):
    if not result or result.get("status") != "error":
        return False
    if result.get("object_type") != "table":
        return False
    if target_db_type != "Snowflake":
        return False
    error_text = str(result.get("error") or "").lower()
    return "does not exist or not authorized" in error_text


def _is_already_exists_error(error_text):
    text = str(error_text or "").lower()
    return (
        "already exists" in text
        or "there is already an object named" in text
        or "table '" in text and "exists" in text
        or "view '" in text and "exists" in text
        or "routine" in text and "already exists" in text
    )


def _canonicalize_target_object_sql(
    sql_text,
    target_db,
    object_type,
    object_name,
    target_connection_details,
):
    text = str(sql_text or "")
    normalized_target = str(target_db or "").strip().lower()
    if normalized_target == "mysql":
        text = _canonicalize_mysql_sql(text, object_type, object_name)
        return text
    if normalized_target in {"sql server", "sqlserver", "sql_server", "azure sql", "azuresql", "azure_sql"} and object_type in {"table", "view"}:
        target_adapter = get_adapter(target_db)
        canonical_name = target_adapter.qualify_table_name(
            object_name,
            target_connection_details,
        )
        text = _strip_delimiter_wrappers(text)
        create_pattern = re.compile(
            rf"(?is)\b(create\s+(?:or\s+replace\s+)?{object_type}\s+(?:if\s+not\s+exists\s+)?)"
            r"((?:\[[^\]]+\]|\"[^\"]+\"|[A-Za-z0-9_$]+)(?:\.(?:\[[^\]]+\]|\"[^\"]+\"|[A-Za-z0-9_$]+)){0,2})"
        )
        match = create_pattern.search(text)
        if not match:
            return text
        current_name = match.group(2)
        if current_name == canonical_name:
            return text
        prefix = match.group(1)
        rewritten = f"{prefix}{canonical_name}"
        return text[:match.start()] + rewritten + text[match.end():]
    if normalized_target != "snowflake" or object_type not in {"table", "view"}:
        return _strip_delimiter_wrappers(text)

    target_adapter = get_adapter(target_db)
    canonical_name = target_adapter.qualify_table_name(
        object_name,
        target_connection_details,
    )
    create_pattern = re.compile(
        rf"(?is)\b(create\s+(?:or\s+replace\s+)?{object_type}\s+(?:if\s+not\s+exists\s+)?)"
        r"((?:\"[^\"]+\"|[A-Za-z0-9_$]+)(?:\.(?:\"[^\"]+\"|[A-Za-z0-9_$]+)){0,2})"
    )
    text = _strip_delimiter_wrappers(text)
    match = create_pattern.search(text)
    if not match:
        return text
    current_name = match.group(2)
    if current_name == canonical_name:
        return text
    return create_pattern.sub(rf"\1{canonical_name}", text, count=1)


def _strip_object_signature(object_name):
    text = str(object_name or "").strip()
    if not text:
        return text
    match = re.match(r"^(?P<name>[^()]+?)(?P<signature>\(.*\))$", text)
    if not match:
        return text
    return match.group("name").strip()


def _qualify_target_object_name(target_db, object_type, object_name, connection_details=None):
    target_adapter = get_adapter(target_db)
    normalized_target = str(target_db or "").strip().lower()
    normalized_object_type = str(object_type or "").strip().lower()
    if normalized_object_type in {"table", "view"}:
        return target_adapter.qualify_table_name(object_name, connection_details)
    if normalized_target == "snowflake" and normalized_object_type in {"function", "storedprocedure"}:
        qualify_routine = getattr(target_adapter, "_qualify_routine_name", None)
        if callable(qualify_routine):
            return qualify_routine(object_name, connection_details)
    base_name = _strip_object_signature(object_name)
    return target_adapter.qualify_table_name(base_name, connection_details)


def _drop_target_object_if_exists(target_cursor, target_db, object_type, object_name, connection_details=None):
    normalized_object_type = str(object_type or "").strip().lower()
    if normalized_object_type == "table":
        drop_target_table(
            target_cursor,
            target_db,
            object_name,
            connection_details,
        )
        return
    drop_keyword_map = {
        "view": "VIEW",
        "storedprocedure": "PROCEDURE",
        "function": "FUNCTION",
        "trigger": "TRIGGER",
    }
    drop_keyword = drop_keyword_map.get(normalized_object_type)
    if not drop_keyword:
        return
    qualified_name = _qualify_target_object_name(
        target_db,
        normalized_object_type,
        object_name,
        connection_details,
    )
    target_cursor.execute(f"DROP {drop_keyword} IF EXISTS {qualified_name}")


def _force_sqlserver_schema_binding(sql_text, object_type, object_name, target_connection_details):
    details = target_connection_details or {}
    schema = str(details.get("schema") or "").strip()
    if object_type not in {"table", "view"} or not schema:
        return str(sql_text or "")
    adapter = get_adapter("sqlserver")
    canonical_name = adapter.qualify_table_name(object_name, details)
    text = _strip_delimiter_wrappers(sql_text)
    create_pattern = re.compile(
        rf"(?is)\b(create\s+(?:or\s+replace\s+)?{object_type}\s+(?:if\s+not\s+exists\s+)?)"
        r"((?:\[[^\]]+\]|\"[^\"]+\"|[A-Za-z0-9_$]+)(?:\.(?:\[[^\]]+\]|\"[^\"]+\"|[A-Za-z0-9_$]+)){0,2})"
    )
    match = create_pattern.search(text)
    if not match:
        return text
    prefix = match.group(1)
    rewritten = f"{prefix}{canonical_name}"
    return text[:match.start()] + rewritten + text[match.end():]


def _strip_delimiter_wrappers(sql_text):
    text = str(sql_text or "").strip()
    if not text:
        return text
    delimiter_matches = re.findall(r"(?im)^\s*delimiter\s+(\S+)\s*$", text)
    for delimiter in delimiter_matches:
        if delimiter and delimiter != ";":
            text = text.replace(delimiter, ";")
    text = re.sub(r"(?im)^\s*delimiter\s+\S+\s*$", "", text)
    text = re.sub(r"(?im)^\s*use\s+[A-Za-z0-9_`\"$.]+\s*;\s*$", "", text)
    text = re.sub(r"(?im)^\s*create\s+database\s+[^\n;]+;\s*$", "", text)
    text = re.sub(r"(?im)^\s*drop\s+(table|view|procedure|function|trigger)\s+if\s+exists\s+[^\n;]+;\s*$", "", text)
    text = re.sub(r"(?im)^\s*drop\s+(table|view|procedure|function|trigger)\s+[^\n;]+;\s*$", "", text)
    text = re.sub(r"(?im)^\s*definer\s*=\s*[^ ]+\s*", "", text)
    text = re.sub(r";{2,}", ";", text)
    return text.strip()


def _standardize_sql_text(sql_text):
    text = str(sql_text or "")
    if not text.strip():
        return text
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\\+\n\s*", "\n", text)
    text = text.replace("\\r\\n", "\n")
    text = text.replace("\\n", "\n")
    text = text.replace("\\r", "\n")
    text = text.replace("\\t", "\t")
    text = text.replace("\\'", "'")
    text = text.replace('\\"', '"')
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def _extract_first_create_statement(sql_text, object_type):
    create_keyword = "view" if object_type == "view" else "table"
    pattern = re.compile(
        rf"(?is)\bcreate\s+(?:or\s+replace\s+)?{create_keyword}\b.*?;",
    )
    match = pattern.search(sql_text or "")
    if match:
        return match.group(0).strip()
    return str(sql_text or "").strip()


def _canonicalize_mysql_sql(sql_text, object_type, object_name):
    text = _strip_delimiter_wrappers(sql_text)
    if object_type in {"table", "view"}:
        text = _extract_first_create_statement(text, object_type)
        text = re.sub(
            rf"(?is)\bcreate\s+or\s+replace\s+{object_type}\b",
            f"CREATE {object_type.upper()}",
            text,
            count=1,
        )
    if object_type == "table":
        text = re.sub(r"(?is)\bcopy\s+grants\b", "", text)
        text = re.sub(r"(?is)\bcluster\s+by\s*\([^)]+\)", "", text)
        text = re.sub(r"(?is)\bcomment\s*=\s*'[^']*'", "", text)
        text = re.sub(r"(?is)\bdata_retention_time_in_days\s*=\s*\d+", "", text)
        text = re.sub(r"(?is)\bchange_tracking\s*=\s*(true|false)", "", text)
        text = re.sub(r"(?is)\benable_schema_evolution\s*=\s*(true|false)", "", text)
        text = re.sub(r",\s*\)", "\n)", text)
    if object_name and object_type in {"table", "view"}:
        mysql_name = f"`{object_name}`"
        create_pattern = re.compile(
            rf"(?is)\b(create\s+(?:or\s+replace\s+)?{object_type}\s+(?:if\s+not\s+exists\s+)?)"
            r"((?:`[^`]+`|[A-Za-z0-9_$]+)(?:\.(?:`[^`]+`|[A-Za-z0-9_$]+)){0,2})"
        )
        if create_pattern.search(text):
            text = create_pattern.sub(rf"\1{mysql_name}", text, count=1)
    return text.strip()


def _sanitize_mysql_table_sql(sql_text):
    text = str(sql_text or "")
    if not text.strip():
        return text

    text = _strip_delimiter_wrappers(text)
    text = re.sub(r"(?is)\bcopy\s+grants\b", "", text)
    text = re.sub(r"(?is)\bcluster\s+by\s*\([^)]+\)", "", text)
    text = re.sub(r"(?is)\bcomment\s*=\s*'[^']*'", "", text)
    text = re.sub(r"(?is)\bdata_retention_time_in_days\s*=\s*\d+", "", text)
    text = re.sub(r"(?is)\bchange_tracking\s*=\s*(true|false)", "", text)
    text = re.sub(r"(?is)\benable_schema_evolution\s*=\s*(true|false)", "", text)
    text = re.sub(r"(?is)\bautoincrement\b", "AUTO_INCREMENT", text)
    text = re.sub(r"(?is)\bstart\s+-?\d+\b", "", text)
    text = re.sub(r"(?is)\bincrement\s+-?\d+\b", "", text)
    text = re.sub(r"(?is)\bnoorder\b", "", text)
    text = re.sub(r"(?is)\border\b", "", text)
    text = re.sub(
        r"(?is)\bdefault\s+([A-Za-z0-9_$.]+)\.nextval\b",
        "",
        text,
    )
    text = re.sub(
        r"(?is)\bdefault\s+nextval\s*\([^)]*\)",
        "",
        text,
    )
    text = re.sub(r",\s*\)", "\n)", text)
    text = re.sub(r"\(\s*,", "(", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    return text.strip()


def _repair_mysql_handler_blocks(sql_text):
    lines = str(sql_text or "").split("\n")
    repaired = []
    index = 0

    while index < len(lines):
        line = lines[index]
        if not re.match(r"(?is)^\s*DECLARE\s+(?:EXIT|CONTINUE)\s+HANDLER\b", line):
            repaired.append(line)
            index += 1
            continue

        repaired.append(line)
        index += 1

        if ";" in str(line):
            continue

        blank_lines = []
        while index < len(lines) and not str(lines[index]).strip():
            blank_lines.append(lines[index])
            index += 1

        if index >= len(lines):
            repaired.extend(blank_lines)
            break

        next_line = str(lines[index]).strip()
        if re.match(r"(?is)^BEGIN\b", next_line):
            repaired.extend(blank_lines)
            continue
        if re.match(
            r"(?is)^(DECLARE\b|IF\b|LOOP\b|WHILE\b|REPEAT\b|CASE\b|OPEN\b|CLOSE\b|FETCH\b|START\b|COMMIT\b|ROLLBACK\b|LEAVE\b|ITERATE\b|SELECT\b|INSERT\b|UPDATE\b|DELETE\b|SET\b)",
            next_line,
        ) is None:
            repaired.extend(blank_lines)
            continue

        repaired.append("BEGIN")
        repaired.extend(blank_lines)

        statement_lines = []
        while index < len(lines):
            statement_lines.append(lines[index])
            current_line = str(lines[index]).strip()
            index += 1
            if ";" in current_line:
                break
        repaired.extend(statement_lines)
        repaired.append("END;")

    return "\n".join(repaired)


def _sanitize_mysql_routine_sql(sql_text):
    text = _strip_delimiter_wrappers(sql_text)
    text = _standardize_sql_text(text)
    text = _repair_mysql_handler_blocks(text)
    text = re.sub(r"(?im)^\s*language\s+sql\s*$", "", text)
    text = re.sub(r"(?im)^\s*as\s+\$\$\s*$", "", text)
    text = re.sub(r"(?im)^\s*\$\$\s*;?\s*$", "", text)
    return text.strip()


def _collect_mysql_declare_blocks(body: str) -> tuple[list[str], list[str]]:
    lines = str(body or "").split("\n")
    declare_blocks = []
    other_lines = []
    index = 0

    while index < len(lines):
        line = lines[index]
        stripped = str(line).strip()
        if not re.match(r"(?is)^declare\b", stripped):
            other_lines.append(line)
            index += 1
            continue

        block = [line]
        index += 1
        begin_depth = len(re.findall(r"(?is)\bbegin\b", stripped))
        begin_depth -= len(re.findall(r"(?is)\bend\b", stripped))
        while index < len(lines):
            current = lines[index]
            current_stripped = str(current).strip()
            block.append(current)
            begin_depth += len(re.findall(r"(?is)\bbegin\b", current_stripped))
            begin_depth -= len(re.findall(r"(?is)\bend\b", current_stripped))
            index += 1
            if ";" in current_stripped and begin_depth <= 0:
                break
        declare_blocks.append("\n".join(block).strip())

    return declare_blocks, other_lines


def _remove_invalid_mysql_procedure_constructs(sql_text: str) -> str:
    text = str(sql_text or "")
    patterns = [
        r"(?im)^\s*language\s+sql\s*$",
        r"(?im)^\s*execute\s+as\s+owner\s*$",
        r"(?im)^\s*execute\s+as\s+caller\s*$",
        r"(?im)^\s*strict\s*$",
        r"(?im)^\s*volatile\s*$",
        r"(?im)^\s*immutable\s*$",
        r"(?im)^\s*stable\s*$",
        r"(?im)^\s*returns\s+table\s*\(.*$",
        r"(?im)^\s*return\s+table\s*\(.*$",
    ]
    for pattern in patterns:
        text = re.sub(pattern, "", text)
    return text.strip()


def _ensure_mysql_procedure_body_wrapper(body: str) -> str:
    text = str(body or "").strip()
    text = re.sub(r"(?im)^\s*delimiter\s+\S+\s*$", "", text).strip()
    text = re.sub(r"(?is)\$\$\s*$", "", text).strip()
    text = text.rstrip(";").strip()
    if not text:
        return "BEGIN\nEND"
    if re.match(r"(?is)^begin\b", text):
        if re.search(r"(?is)\bend\s*$", text):
            return text
        return f"{text}\nEND"
    return f"BEGIN\n{text}\nEND"


def _detect_unsafe_mysql_procedure_sql(sql_text: str) -> str | None:
    text = str(sql_text or "")
    if not re.search(r"(?is)\bcreate\s+procedure\b", text):
        return None
    if re.search(r"(?is)\bselect\s*;", text):
        return "Generated SQL is invalid because a SELECT statement was terminated before its column list."
    if re.search(r"(?im);\s*$\s*\n\s*[A-Za-z_][A-Za-z0-9_$]*\s*,", text):
        return "Generated SQL is invalid because a semicolon split a SELECT column list."
    return None


def _find_matching_paren(text: str, start_index: int) -> int:
    """
    Finds the index of the matching closing parenthesis for the opening parenthesis at start_index.
    Returns -1 if not found.
    """
    if not isinstance(text, str) or start_index < 0 or start_index >= len(text):
        return -1
    if text[start_index] != "(":
        return -1

    stack = 0
    for i in range(start_index, len(text)):
        if text[i] == "(":
            stack += 1
        elif text[i] == ")":
            stack -= 1
            if stack == 0:
                return i
    return -1


def normalize_mysql_procedure(sql: str) -> str:
    text = _sanitize_mysql_routine_sql(sql)
    text = _remove_invalid_mysql_procedure_constructs(text)
    if not re.search(r"(?is)\bcreate\s+procedure\b", text):
        return text

    header_match = re.search(r"(?is)\bcreate\s+procedure\b", text)
    if not header_match:
        return text
    args_open = text.find("(", header_match.end())
    if args_open == -1:
        return text
    try:
        args_close = _find_matching_paren(text, args_open)
    except Exception:
        logger.exception("Failed to locate matching parenthesis while fixing MySQL procedure structure")
        return text
    if args_close == -1:
        return text

    header = text[: args_close + 1].strip()
    body = text[args_close + 1 :].strip()
    rebuilt_body = _ensure_mysql_procedure_body_wrapper(body)

    return f"DELIMITER $$\n{header}\n{rebuilt_body} $$\nDELIMITER ;"


def fix_mysql_procedure_structure(sql: str) -> str:
    text = normalize_mysql_procedure(sql)
    if not re.search(r"(?is)\bcreate\s+procedure\b", text):
        return text

    header_match = re.search(r"(?is)\bcreate\s+procedure\b", text)
    if not header_match:
        return text
    args_open = text.find("(", header_match.end())
    if args_open == -1:
        return text
    try:
        args_close = _find_matching_paren(text, args_open)
    except Exception:
        logger.exception("Failed to locate matching parenthesis while fixing MySQL procedure structure")
        return text
    if args_close == -1:
        return text

    header = text[: args_close + 1].strip()
    body = text[args_close + 1 :].strip()
    body = re.sub(r"(?im)^\s*delimiter\s+\S+\s*$", "", body).strip()
    body = re.sub(r"(?is)\$\$\s*$", "", body).strip()
    body = body.rstrip(";").strip()

    if not body:
        rebuilt_body = "BEGIN\nEND"
        return f"DELIMITER $$\n{header}\n{rebuilt_body} $$\nDELIMITER ;"

    if re.match(r"(?is)^begin\b", body):
        inner_body = re.sub(r"(?is)^begin\b\s*", "", body).strip()
    else:
        inner_body = body
    inner_body = re.sub(r"(?is)(?:\bend\b\s*;?\s*)+$", "", inner_body).strip()

    declare_blocks, remaining_lines = _collect_mysql_declare_blocks(inner_body)
    remaining_body = "\n".join(remaining_lines).strip()

    rebuilt_parts = ["BEGIN"]
    if declare_blocks:
        rebuilt_parts.append("\n\n".join(block for block in declare_blocks if block))
    if remaining_body:
        rebuilt_parts.append(remaining_body)
    rebuilt_parts.append("END")
    rebuilt_body = "\n".join(part for part in rebuilt_parts if part).strip()

    return f"DELIMITER $$\n{header}\n{rebuilt_body} $$\nDELIMITER ;"


def _validate_generated_object_sql(sql_text, object_type):
    normalized_sql = str(sql_text or "").strip().lower()
    if object_type == "trigger":
        return sql_text
    if object_type == "table":
        if "create table" not in normalized_sql:
            raise Exception(
                "Generated SQL does not define a table. Return exactly one CREATE TABLE statement."
            )
        return sql_text
    if object_type == "view":
        if "create view" not in normalized_sql and "create or replace view" not in normalized_sql:
            raise Exception(
                "Generated SQL does not define a view. Return exactly one CREATE VIEW statement."
            )
        return sql_text
    if object_type == "storedprocedure":
        if "trigger" in normalized_sql and "procedure" not in normalized_sql:
            raise Exception(
                "Generated SQL changed the object type from stored procedure to trigger."
            )
    if object_type == "function":
        if "create function" not in normalized_sql and "create or replace function" not in normalized_sql:
            raise Exception(
                "Generated SQL does not define a function. Return exactly one CREATE FUNCTION statement."
            )
    return sql_text


def _validate_generated_trigger_sql(sql_text, source_db, target_db):
    normalized_sql = str(sql_text or "").strip().lower()
    normalized_source_db = str(source_db or "").strip().lower()
    normalized_target_db = str(target_db or "").strip().lower()
    if normalized_target_db == "snowflake":
        has_stream = "create stream" in normalized_sql or "create or replace stream" in normalized_sql
        has_task = "create task" in normalized_sql or "create or replace task" in normalized_sql
        if not has_stream or not has_task:
            raise Exception(
                "Generated SQL does not define a Snowflake stream/task bundle for trigger migration."
            )
        return sql_text
    if normalized_source_db == "snowflake" and normalized_target_db != "snowflake":
        if "create trigger" not in normalized_sql and "create or replace trigger" not in normalized_sql:
            raise Exception(
                "Snowflake trigger-equivalent migration must return one CREATE TRIGGER statement for the target database."
            )
        if "create task" in normalized_sql or "create stream" in normalized_sql:
            raise Exception(
                "Snowflake trigger-equivalent migration returned stream/task SQL instead of a target trigger."
            )
        return sql_text
    if "create trigger" not in normalized_sql and "create or replace trigger" not in normalized_sql:
        raise Exception(
            "Generated SQL does not define a trigger. Return exactly one CREATE TRIGGER statement."
        )
    if "procedure" in normalized_sql or "function" in normalized_sql:
        raise Exception(
            "Generated SQL changed the object type from trigger to another routine type."
        )
    return sql_text


def _sanitize_snowflake_table_sql(sql_text):
    text = str(sql_text or "")
    if not text.strip():
        return text

    # Strip MySQL-only column clauses that Snowflake does not support.
    text = re.sub(
        r"(?is)\bautoincrement\b(?:\s*\(\s*\d+\s*,\s*\d+\s*\))?",
        "IDENTITY(1,1)",
        text,
    )
    text = re.sub(
        r"(?is)\bauto\s*increment\b(?:\s*\(\s*\d+\s*,\s*\d+\s*\))?",
        "IDENTITY(1,1)",
        text,
    )
    text = re.sub(
        r"(?is)\s+on\s+update\s+(?:current_timestamp(?:\s*\(\s*\))?|[A-Za-z_][A-Za-z0-9_]*\s*\([^)]*\)|[^,\n)]+)",
        "",
        text,
    )
    text = re.sub(r"(?is)\bcharacter\s+set\s+[A-Za-z0-9_]+", "", text)
    text = re.sub(r"(?is)\bcollate\s+[A-Za-z0-9_]+", "", text)
    text = re.sub(r"(?is)\bunsigned\b", "", text)

    # Remove table-level foreign key constraints that can fail due to creation order.
    text = re.sub(
        r"(?im)^\s*,?\s*constraint\s+[A-Za-z0-9_\"$]+\s+foreign\s+key\s*\([^)]+\)\s+references\s+[^\n,]+(?:,[^\n]*)?$",
        "",
        text,
    )
    text = re.sub(
        r"(?im)^\s*,?\s*foreign\s+key\s*\([^)]+\)\s+references\s+[^\n,]+(?:,[^\n]*)?$",
        "",
        text,
    )
    # Remove inline REFERENCES clauses on columns.
    text = re.sub(
        r"(?is)\s+references\s+[A-Za-z0-9_\"$.]+\s*\([^)]+\)",
        "",
        text,
    )
    # Clean comma placement before closing parentheses.
    text = re.sub(r",\s*\)", "\n)", text)
    text = re.sub(r"\(\s*,", "(", text)
    return text


def _sanitize_snowflake_routine_sql(sql_text):
    text = _strip_delimiter_wrappers(sql_text)
    text = _standardize_sql_text(text)
    text = re.sub(r"(?is)\bdeterministic\b", "", text)
    text = re.sub(r"(?is)\breturns\s+table\s*\([^)]+\)", "RETURNS STRING", text)
    return text.strip()


def _canonicalize_snowflake_trigger_sql(sql_text, connection_details=None):
    text = str(sql_text or "").strip()
    if not text:
        return text
    adapter = get_adapter("Snowflake")
    details = connection_details or {}
    warehouse = details.get("warehouse")

    def qualify(name):
        return adapter.qualify_table_name(str(name).strip().strip("`\""), details)

    statements = _split_sql_bundle(text)
    normalized_statements = []
    for statement in statements:
        current = str(statement or "").strip().rstrip(";")
        if not current:
            continue
        stream_match = re.match(
            r'(?is)^create\s+or\s+replace\s+stream\s+([`"\w$.]+)\s+on\s+table\s+([`"\w$.]+)$',
            current,
        )
        if stream_match:
            normalized_statements.append(
                f"CREATE OR REPLACE STREAM {qualify(stream_match.group(1))} ON TABLE {qualify(stream_match.group(2))};"
            )
            continue
        task_insert_match = re.match(
            r'(?is)^create\s+or\s+replace\s+task\s+([`"\w$.]+)\s+(.*?)\s+when\s+system\$stream_has_data\([\'"]([^\'"]+)[\'"]\)\s+as\s+insert\s+into\s+([`"\w$.]+)\s*\((.*?)\)\s*select\s+(.*?)\s+from\s+([`"\w$.]+)$',
            current,
        )
        if task_insert_match:
            task_name = qualify(task_insert_match.group(1))
            task_options = str(task_insert_match.group(2) or "").strip()
            stream_ref = qualify(task_insert_match.group(3))
            insert_table = qualify(task_insert_match.group(4))
            insert_columns = task_insert_match.group(5).strip()
            select_expr = task_insert_match.group(6).strip()
            from_stream = qualify(task_insert_match.group(7))
            if warehouse and re.fullmatch(r"[A-Za-z0-9_$.]+", str(warehouse)):
                task_options = re.sub(
                    r"(?is)\bUSER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE\s*=\s*'[^']+'\s*",
                    "",
                    task_options,
                ).strip()
                task_options = f"WAREHOUSE = {warehouse} {task_options}".strip()
            normalized_statements.append(
                f"CREATE OR REPLACE TASK {task_name} {task_options} "
                f"WHEN SYSTEM$STREAM_HAS_DATA('{stream_ref}') "
                f"AS INSERT INTO {insert_table} ({insert_columns}) "
                f"SELECT {select_expr} FROM {from_stream};"
            )
            continue
        task_select_match = re.match(
            r'(?is)^create\s+or\s+replace\s+task\s+([`"\w$.]+)\s+(.*?)\s+when\s+system\$stream_has_data\([\'"]([^\'"]+)[\'"]\)\s+as\s+select\s+(.*?)\s+from\s+([`"\w$.]+)$',
            current,
        )
        if task_select_match:
            task_name = qualify(task_select_match.group(1))
            task_options = str(task_select_match.group(2) or "").strip()
            stream_ref = qualify(task_select_match.group(3))
            select_expr = task_select_match.group(4).strip()
            from_stream = qualify(task_select_match.group(5))
            if warehouse and re.fullmatch(r"[A-Za-z0-9_$.]+", str(warehouse)):
                task_options = re.sub(
                    r"(?is)\bUSER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE\s*=\s*'[^']+'\s*",
                    "",
                    task_options,
                ).strip()
                task_options = f"WAREHOUSE = {warehouse} {task_options}".strip()
            normalized_statements.append(
                f"CREATE OR REPLACE TASK {task_name} {task_options} "
                f"WHEN SYSTEM$STREAM_HAS_DATA('{stream_ref}') "
                f"AS SELECT {select_expr} FROM {from_stream};"
            )
            continue
        normalized_statements.append(f"{current};")
    return SQL_BUNDLE_DELIMITER.join(normalized_statements)


def _sanitize_target_object_sql(sql_text, source_db, target_db, object_type, target_connection_details=None):
    text = _standardize_sql_text(sql_text)
    normalized_target = str(target_db or "").strip().lower()
    normalized_source = str(source_db or "").strip().lower()
    normalized_object_type = str(object_type or "").strip().lower()
    if normalized_target == "snowflake" and normalized_object_type == "trigger":
        return _canonicalize_snowflake_trigger_sql(text, target_connection_details)
    if normalized_object_type != "table":
        if normalized_target == "snowflake" and normalized_object_type in {"function", "storedprocedure"}:
            return _sanitize_snowflake_routine_sql(text)
        if normalized_target == "mysql" and normalized_object_type in {"function", "storedprocedure"}:
            return _sanitize_mysql_routine_sql(text)
        return text
    if normalized_target == "snowflake":
        return _sanitize_snowflake_table_sql(text)
    if normalized_target == "mysql":
        return _sanitize_mysql_table_sql(text)
    return text


def _sanitize_client_error_message(target_db, object_type, error_text):
    raw_text = str(error_text or "").strip()
    target_name = str(target_db or "").strip() or "target"
    normalized_target = target_name.lower()
    normalized_object_type = str(object_type or "object").strip().lower() or "object"
    error_type = _classify_error(raw_text)
    missing_dependency = _extract_missing_dependency_name(raw_text)

    lowered_raw_text = raw_text.lower()
    if "source sql extraction failed" in lowered_raw_text:
        return raw_text
    if "could not locate sql server table" in lowered_raw_text:
        return raw_text

    if error_type == "syntax":
        return (
            f"{target_name} {normalized_object_type} validation failed. "
            "The generated SQL was not compatible with the target dialect after automated repair."
        )
    if error_type == "dependency":
        if missing_dependency and normalized_object_type == "view":
            return (
                f"{target_name} view validation failed because referenced table or view "
                f"'{missing_dependency}' was not available in the target schema."
            )
        if missing_dependency:
            return (
                f"{target_name} {normalized_object_type} validation failed because required object "
                f"'{missing_dependency}' was not available in the target schema."
            )
        return (
            f"{target_name} {normalized_object_type} validation failed because a required dependency or reference was not available."
        )
    if error_type == "authorization":
        return (
            f"{target_name} access validation failed. Verify the configured privileges for the selected {normalized_object_type}."
        )
    if error_type == "connectivity":
        return f"{target_name} connectivity validation failed during {normalized_object_type} migration."
    if error_type == "transformation":
        return (
            f"{target_name} {normalized_object_type} transformation validation failed. "
            "A source definition could not be converted safely for the target dialect."
        )
    if "mysql" in normalized_target:
        return f"MySQL {normalized_object_type} execution failed after automated validation and repair."
    if "snowflake" in normalized_target:
        return f"Snowflake {normalized_object_type} execution failed after automated validation and repair."
    return f"Target {normalized_object_type} execution failed after automated validation and repair."


def _extract_missing_dependency_name(error_text):
    text = str(error_text or "").strip()
    if not text:
        return None

    patterns = [
        r"Table\s+'([^']+)'\s+doesn't exist",
        r"Table\s+'([^']+)'\s+does not exist",
        r"Unknown table\s+'([^']+)'",
        r"Base table or view not found:\s*\d+\s+([A-Za-z0-9_.$`\"]+)",
        r"relation\s+'([^']+)'\s+does not exist",
        r"object\s+'([^']+)'\s+does not exist",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        raw_name = str(match.group(1) or "").strip()
        if not raw_name:
            continue
        parts = [part.strip("`\" ") for part in raw_name.split(".") if part.strip("`\" ")]
        return parts[-1] if parts else raw_name.strip("`\" ")
    return None


def _create_schema_diagnostics(object_type, object_name):
    return {
        "object_type": object_type,
        "object_name": object_name,
        "deterministic_supported": False,
        "transform_strategy": None,
        "review_strategy": None,
        "repair_attempts": [],
        "agent_fix_attempts": [],
        "rule_errors": [],
        "raw_errors": [],
        "sql_snapshots": [],
    }


def _append_sql_snapshot(diagnostics, stage, sql_text):
    if diagnostics is None or not sql_text:
        return
    diagnostics["sql_snapshots"].append(
        {"stage": stage, "sql": str(sql_text)}
    )


def _append_raw_error(diagnostics, stage, error_text):
    if diagnostics is None or not error_text:
        return
    diagnostics["raw_errors"].append(
        {"stage": stage, "error": str(error_text)}
    )


def _record_rule_engine_failures(
    source_sql,
    candidate_sql,
    source_config,
    target_config,
    object_type,
    rule_errors,
    diagnostics=None,
):
    if not rule_errors:
        return []
    if diagnostics is not None:
        diagnostics.setdefault("rule_errors", []).extend(rule_errors)
    error_messages = []
    for item in rule_errors:
        message = (
            "Rule engine skipped faulty rule "
            f"'{item.get('name', 'unnamed_rule')}' "
            f"with pattern '{item.get('pattern', '')}': {item.get('error', '')}"
        )
        error_messages.append(message)
        _append_raw_error(diagnostics, "rule_engine", message)
    validation = {
        "is_valid": False,
        "errors": error_messages,
        "warnings": [],
    }
    try:
        suggestions = rag_agent.analyze(
            source_sql,
            candidate_sql,
            validation,
            source_config["database_type"],
            target_config["database_type"],
            object_type,
        )
    except Exception:
        suggestions = []
    if diagnostics is not None and suggestions:
        diagnostics.setdefault("rag_suggestions", []).extend(suggestions)
    return error_messages


def _build_failed_validation(error_text):
    return {
        "is_valid": False,
        "errors": [str(error_text or "Unknown validation error.")],
        "warnings": [],
    }


def _postprocess_target_sql(
    candidate_sql,
    source_config,
    target_config,
    object_type,
    diagnostics=None,
):
    sql_text = str(candidate_sql or "")
    normalized_target = str(target_config["database_type"] or "").strip().lower()
    if normalized_target == "mysql" and object_type in {"function", "storedprocedure"}:
        sql_text = _sanitize_mysql_routine_sql(sql_text)
    if normalized_target == "mysql" and object_type == "storedprocedure":
        try:
            sql_text = fix_mysql_procedure_structure(sql_text)
            if diagnostics is not None:
                diagnostics["storedprocedure_normalization_applied"] = True
        except Exception as procedure_structure_error:
            logger.exception("MySQL procedure structure normalization failed; continuing with unmodified SQL")
            _append_raw_error(
                diagnostics,
                "mysql_procedure_structure",
                procedure_structure_error,
            )
    return sql_text


def _request_agent_sql_fix(
    source_sql,
    candidate_sql,
    error_text,
    source_config,
    target_config,
    object_type,
    diagnostics=None,
    stage="validation",
):
    validation = _build_failed_validation(error_text)
    try:
        suggestions = rag_agent.analyze(
            source_sql,
            candidate_sql,
            validation,
            source_config["database_type"],
            target_config["database_type"],
            object_type,
        )
    except Exception:
        suggestions = []
    if diagnostics is not None and suggestions:
        diagnostics.setdefault("rag_suggestions", []).extend(suggestions)

    try:
        fixed_sql = rag_agent.fix_sql(
            candidate_sql,
            str(error_text or ""),
            source_config["database_type"],
            target_config["database_type"],
            object_type,
        )
    except Exception as fix_error:
        _append_raw_error(diagnostics, f"{stage}_rag_fix", fix_error)
        fixed_sql = candidate_sql

    record = {
        "stage": stage,
        "error": str(error_text or ""),
        "original_sql": str(candidate_sql or ""),
        "corrected_sql": str(fixed_sql or ""),
        "changed": _standardize_sql_text(fixed_sql) != _standardize_sql_text(candidate_sql),
    }
    if diagnostics is not None:
        diagnostics.setdefault("agent_fix_attempts", []).append(record)
        diagnostics["latest_agent_fix"] = record
    return fixed_sql


def _validate_sql_with_agent_retries(
    source_sql,
    candidate_sql,
    source_config,
    target_config,
    object_type,
    diagnostics=None,
):
    current_sql = str(candidate_sql or "")
    seen_sql = {_standardize_sql_text(current_sql)}
    last_error = None

    for attempt in range(SQL_REPAIR_ATTEMPTS + 1):
        try:
            validation = _run_target_sql_validation(
                source_sql,
                current_sql,
                source_config,
                target_config,
                object_type,
                diagnostics=diagnostics,
            )
            return current_sql, validation
        except Exception as validation_error:
            last_error = validation_error
            _append_raw_error(diagnostics, f"validation_attempt_{attempt}", validation_error)
            if attempt >= SQL_REPAIR_ATTEMPTS:
                break
            if object_type == "storedprocedure":
                logger.info("Retrying with AI fix")
            fixed_sql = _request_agent_sql_fix(
                source_sql,
                current_sql,
                validation_error,
                source_config,
                target_config,
                object_type,
                diagnostics=diagnostics,
                stage="validation",
            )
            fixed_sql = _postprocess_target_sql(
                fixed_sql,
                source_config,
                target_config,
                object_type,
                diagnostics=diagnostics,
            )
            normalized_fixed_sql = _standardize_sql_text(fixed_sql)
            if not normalized_fixed_sql or normalized_fixed_sql in seen_sql:
                break
            seen_sql.add(normalized_fixed_sql)
            current_sql = fixed_sql

    if last_error is not None:
        raise last_error
    raise Exception("SQL validation failed before execution.")


def _record_validation_failure(
    source_sql,
    candidate_sql,
    source_config,
    target_config,
    object_type,
    validation,
    diagnostics=None,
):
    if diagnostics is not None:
        diagnostics["validation"] = validation
    try:
        suggestions = rag_agent.analyze(
            source_sql,
            candidate_sql,
            validation,
            source_config["database_type"],
            target_config["database_type"],
            object_type,
        )
    except Exception:
        suggestions = []
    if diagnostics is not None and suggestions:
        diagnostics.setdefault("rag_suggestions", []).extend(suggestions)


def _run_target_sql_validation(
    source_sql,
    candidate_sql,
    source_config,
    target_config,
    object_type,
    diagnostics=None,
):
    if (
        str(target_config["database_type"] or "").strip().lower() == "mysql"
        and object_type == "storedprocedure"
    ):
        stability_error = _detect_unsafe_mysql_procedure_sql(candidate_sql)
        if stability_error:
            validation = _build_failed_validation(stability_error)
            _record_validation_failure(
                source_sql,
                candidate_sql,
                source_config,
                target_config,
                object_type,
                validation,
                diagnostics=diagnostics,
            )
            raise Exception(
                "SQL validator rejected generated SQL before execution: "
                + "; ".join(validation["errors"])
            )
    try:
        validation = validate(
            candidate_sql,
            target_config["database_type"],
            object_type,
        )
    except Exception as validation_error:
        validation = _build_failed_validation(
            f"Validator internal error: {validation_error}"
        )
        _record_validation_failure(
            source_sql,
            candidate_sql,
            source_config,
            target_config,
            object_type,
            validation,
            diagnostics=diagnostics,
        )
        raise Exception(
            "SQL validator failed before execution: "
            + "; ".join(validation["errors"])
        ) from validation_error
    if diagnostics is not None:
        diagnostics["validation"] = validation
    if not validation.get("is_valid"):
        _record_validation_failure(
            source_sql,
            candidate_sql,
            source_config,
            target_config,
            object_type,
            validation,
            diagnostics=diagnostics,
        )
        raise Exception(
            "SQL validator rejected generated SQL before execution: "
            + "; ".join(validation.get("errors") or ["Unknown validation error."])
        )
    try:
        validate_target_sql_semantics(
            candidate_sql,
            source_config["database_type"],
            target_config["database_type"],
            object_type,
        )
    except Exception as semantic_error:
        semantic_validation = _build_failed_validation(
            f"Target semantic validation failed: {semantic_error}"
        )
        _record_validation_failure(
            source_sql,
            candidate_sql,
            source_config,
            target_config,
            object_type,
            semantic_validation,
            diagnostics=diagnostics,
        )
        raise Exception(
            "SQL validator rejected generated SQL before execution: "
            + "; ".join(semantic_validation["errors"])
        )
    return validation


def _is_snowflake_table_sql_target(target_config, object_type):
    normalized_target = str((target_config or {}).get("database_type") or "").strip().lower()
    return normalized_target == "snowflake" and str(object_type or "").strip().lower() in {"table", "view"}


def _is_strict_deterministic_schema_target(target_config, object_type):
    normalized_target = str((target_config or {}).get("database_type") or "").strip().lower()
    normalized_object_type = str(object_type or "").strip().lower()
    return normalized_object_type in {"table", "view"} and normalized_target in {"snowflake", "sqlserver", "sql server", "sql_server", "azure sql", "azuresql", "azure_sql"}


def _prepare_target_object_sql(
    source_sql,
    source_config,
    target_config,
    object_type,
    object_name,
    special_trigger_to_snowflake,
    initial_sql=None,
    diagnostics=None,
):
    from app.services.rule_engine import apply_rules

    deterministic_sql = transform_deterministically(
        source_sql,
        source_config["database_type"],
        target_config["database_type"],
        object_type,
        object_name,
    )
    if diagnostics is not None:
        diagnostics["deterministic_supported"] = supports_deterministic_transform(
            source_config["database_type"],
            target_config["database_type"],
            object_type,
        )
    if initial_sql:
        if diagnostics is not None:
            diagnostics["transform_strategy"] = "history"
            diagnostics["review_strategy"] = "history"
        transformed_sql = initial_sql
    elif deterministic_sql:
        if diagnostics is not None:
            diagnostics["transform_strategy"] = "deterministic"
            diagnostics["review_strategy"] = "deterministic"
        transformed_sql = deterministic_sql
    else:
        if _is_strict_deterministic_schema_target(target_config, object_type):
            raise Exception(
                "Deterministic target SQL generation failed for the requested table/view. "
                "Heuristic AI repair is disabled for this target schema SQL."
            )
        if diagnostics is not None:
            diagnostics["transform_strategy"] = "agent"
        transformed_sql = generate_transformed_sql(
            source_sql,
            source_config["database_type"],
            target_config["database_type"],
            object_type,
            object_name,
        )
        if diagnostics is not None:
            diagnostics["review_strategy"] = "agent"
        transformed_sql = review_transformed_sql(
            source_sql,
            transformed_sql,
            source_config["database_type"],
            target_config["database_type"],
            object_type,
            object_name,
        )

    if special_trigger_to_snowflake and not validate_mysql_trigger_to_snowflake_bundle(transformed_sql):
        transformed_sql = build_mysql_trigger_to_snowflake_fallback_sql(
            source_sql,
            object_name,
            target_config["connection_details"],
        )
        if diagnostics is not None:
            diagnostics["review_strategy"] = "deterministic_trigger_fallback"

    if special_trigger_to_snowflake:
        transformed_sql = _canonicalize_snowflake_trigger_sql(
            transformed_sql,
            target_config["connection_details"],
        )

    if not special_trigger_to_snowflake:
        transformed_sql = _canonicalize_target_object_sql(
            transformed_sql,
            target_config["database_type"],
            object_type,
            object_name,
            target_config["connection_details"],
        )
        if str(target_config["database_type"] or "").strip().lower() in {"sqlserver", "sql server", "sql_server", "azure sql", "azuresql", "azure_sql"}:
            transformed_sql = _force_sqlserver_schema_binding(
                transformed_sql,
                object_type,
                object_name,
                target_config["connection_details"],
            )
        transformed_sql = _validate_generated_object_sql(
            transformed_sql,
            object_type,
        )
        transformed_sql = _sanitize_target_object_sql(
            transformed_sql,
            source_config["database_type"],
            target_config["database_type"],
            object_type,
            target_config["connection_details"],
        )

    if object_type == "trigger":
        transformed_sql = _validate_generated_trigger_sql(
            transformed_sql,
            source_config["database_type"],
            target_config["database_type"],
        )

    rule_result = apply_rules(
        transformed_sql,
        source_config["database_type"],
        target_config["database_type"],
    )
    rule_errors = rule_result.get("rule_errors") or []
    transformed_sql = rule_result.get("sql") or transformed_sql
    transformed_sql = _postprocess_target_sql(
        transformed_sql,
        source_config,
        target_config,
        object_type,
        diagnostics=diagnostics,
    )
    if diagnostics is not None and rule_result.get("applied_rules"):
        diagnostics["applied_rules"] = rule_result.get("applied_rules")
    _record_rule_engine_failures(
        source_sql,
        transformed_sql,
        source_config,
        target_config,
        object_type,
        rule_errors,
        diagnostics=diagnostics,
    )

    try:
        transformed_sql, validation = _validate_sql_with_agent_retries(
            source_sql,
            transformed_sql,
            source_config,
            target_config,
            object_type,
            diagnostics=diagnostics,
        )
    except Exception as validation_error:
        if object_type == "storedprocedure":
            raise Exception("Validation failed for stored procedure") from validation_error
        raise

    _append_sql_snapshot(diagnostics, "prepared", transformed_sql)
    return transformed_sql, validation


def _repair_target_object_sql(
    source_sql,
    current_sql,
    source_config,
    target_config,
    object_type,
    object_name,
    execution_error,
    special_trigger_to_snowflake,
    diagnostics=None,
):
    if special_trigger_to_snowflake:
        repaired_sql = build_mysql_trigger_to_snowflake_fallback_sql(
            source_sql,
            object_name,
            target_config["connection_details"],
        )
        repair_strategy = "deterministic_trigger_fallback"
        repaired_sql = _canonicalize_snowflake_trigger_sql(
            repaired_sql,
            target_config["connection_details"],
        )
    else:
        agent_fixed_sql = _request_agent_sql_fix(
            source_sql,
            current_sql,
            execution_error,
            source_config,
            target_config,
            object_type,
            diagnostics=diagnostics,
            stage="execution",
        )
        if _standardize_sql_text(agent_fixed_sql) != _standardize_sql_text(current_sql):
            repaired_sql = agent_fixed_sql
            repair_strategy = "rag_agent"
        else:
            deterministic_sql = transform_deterministically(
                source_sql,
                source_config["database_type"],
                target_config["database_type"],
                object_type,
                object_name,
            )
            if deterministic_sql and deterministic_sql != current_sql:
                repaired_sql = deterministic_sql
                repair_strategy = "deterministic"
            else:
                if _is_strict_deterministic_schema_target(target_config, object_type):
                    raise execution_error
                repair_guidance = _build_error_guidance(
                    target_config["database_type"],
                    execution_error,
                    object_type,
                )
                repaired_sql = generate_repaired_sql(
                    source_sql,
                    current_sql,
                    " ".join(
                        part for part in [str(execution_error), repair_guidance] if part
                    ),
                    source_config["database_type"],
                    target_config["database_type"],
                    object_type,
                    object_name,
                )
                repair_strategy = "agent"
        repaired_sql = _canonicalize_target_object_sql(
            repaired_sql,
            target_config["database_type"],
            object_type,
            object_name,
            target_config["connection_details"],
        )
        if str(target_config["database_type"] or "").strip().lower() in {"sqlserver", "sql server", "sql_server", "azure sql", "azuresql", "azure_sql"}:
            repaired_sql = _force_sqlserver_schema_binding(
                repaired_sql,
                object_type,
                object_name,
                target_config["connection_details"],
            )
        repaired_sql = _validate_generated_object_sql(
            repaired_sql,
            object_type,
        )
        repaired_sql = _sanitize_target_object_sql(
            repaired_sql,
            source_config["database_type"],
            target_config["database_type"],
            object_type,
            target_config["connection_details"],
        )

    if object_type == "trigger":
        repaired_sql = _validate_generated_trigger_sql(
            repaired_sql,
            source_config["database_type"],
            target_config["database_type"],
        )
    repaired_sql = _postprocess_target_sql(
        repaired_sql,
        source_config,
        target_config,
        object_type,
        diagnostics=diagnostics,
    )
    repaired_sql, _ = _validate_sql_with_agent_retries(
        source_sql,
        repaired_sql,
        source_config,
        target_config,
        object_type,
        diagnostics=diagnostics,
    )

    if diagnostics is not None:
        diagnostics["repair_attempts"].append(
            {
                "strategy": repair_strategy,
                "error": str(execution_error),
            }
        )
    _append_sql_snapshot(diagnostics, f"repair_{len(diagnostics['repair_attempts']) if diagnostics is not None else 0}", repaired_sql)
    return repaired_sql, repair_strategy


def _split_sql_bundle(sql_text):
    text = str(sql_text or "").strip()
    if not text:
        return []
    if SQL_BUNDLE_DELIMITER not in text:
        return [text]
    return [statement.strip() for statement in text.split(SQL_BUNDLE_DELIMITER) if statement.strip()]


def _execute_mysql_routine_sql(target_config, sql_text):
    connection, cursor = _connect_target_with_namespace_bootstrap(target_config)
    try:
        normalized_sql = _sanitize_mysql_routine_sql(sql_text)
        for statement in _split_sql_bundle(normalized_sql):
            cursor.execute(statement)
        connection.commit()
    except Exception:
        try:
            connection.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            connection.close()
        except Exception:
            pass


def _connect_target_with_namespace_bootstrap(target_config, log_callback=None):
    target_details = dict(target_config["connection_details"])
    adapter = get_adapter(target_config["database_type"])

    def emit(message):
        if log_callback:
            log_callback(f"{datetime.now()} - {message}")

    try:
        connection = get_connection(target_config["database_type"], target_details)
        cursor = connection.cursor()
    except Exception:
        fallback_details = dict(target_details)
        fallback_details.pop("database", None)
        fallback_details.pop("schema", None)
        connection = get_connection(target_config["database_type"], fallback_details)
        cursor = connection.cursor()
        emit("Target connection retried without database/schema for namespace bootstrap.")

    bootstrap = adapter.ensure_database_and_schema(connection, cursor, target_details)
    emit(bootstrap.get("message", "Target namespace bootstrap completed."))
    if bootstrap.get("reconnect_required"):
        connection.close()
        connection = get_connection(target_config["database_type"], target_details)
        cursor = connection.cursor()
        bootstrap = adapter.ensure_database_and_schema(connection, cursor, target_details)
        emit(bootstrap.get("message", "Target namespace bootstrap completed after reconnect."))
    return connection, cursor


def _stream_events(worker):
    stream_queue = Queue()
    sentinel = object()

    def publish_log(entry):
        stream_queue.put({"type": "log", "message": entry})

    def publish_event(event_type, payload):
        stream_queue.put({"type": event_type, **payload})

    def run_worker():
        try:
            result = worker(publish_log, publish_event)
            stream_queue.put({"type": "final", "data": result})
        except Exception as error:
            stream_queue.put(
                {
                    "type": "final",
                    "data": {
                        "status": "error",
                        "logs": [f"{datetime.now()} - ERROR: Migration pipeline failed before completion."],
                    },
                }
            )
        finally:
            stream_queue.put(sentinel)

    threading.Thread(target=run_worker, daemon=True).start()

    def generate():
        while True:
            item = stream_queue.get()
            if item is sentinel:
                break
            yield json.dumps(item) + "\n"

    return generate()


def _migrate_object(
    source_cursor,
    target_conn,
    target_cursor,
    source_config,
    target_config,
    object_type,
    object_name,
    migrate_data,
    data_only=False,
    data_migration_mode="insert",
    data_batch_size=1000,
    data_execution_engine="auto",
    spark_row_threshold=1000,
    validate_row_counts=True,
    spark_options=None,
    truncate_before_load=False,
    drop_and_create_if_exists=False,
    safe_replay_mode=False,
    log_callback=None,
):
    from app.services.history_service import get_history_match, save_history
    from app.services.rule_engine import apply_rules

    logs = []
    rows_migrated = 0
    source_row_count = None
    target_row_count = None
    missing_row_count = None
    retry_count = 0
    reused_existing_target = False
    resolved_target_object_name = None
    diagnostics = _create_schema_diagnostics(object_type, object_name)
    source_sql = None
    final_validation = None

    def log(msg):
        entry = f"{datetime.now()} - {msg}"
        logs.append(entry)
        if log_callback:
            log_callback(entry)

    try:
        normalized_target_db = str(target_config["database_type"] or "").strip().lower()
        normalized_source_db = str(source_config["database_type"] or "").strip().lower()
        special_trigger_to_snowflake = (
            normalized_target_db == "snowflake"
            and object_type == "trigger"
        )
        special_snowflake_trigger_source = (
            normalized_source_db == "snowflake"
            and object_type == "trigger"
        )
        unsupported_rule = ""
        if not special_trigger_to_snowflake and not special_snowflake_trigger_source:
            unsupported_rule = get_unsupported_object_rule(
                source_config["database_type"],
                target_config["database_type"],
                object_type,
            )
        if unsupported_rule:
            log(f"SKIPPED: {unsupported_rule}")
            return {
                "status": "skipped",
                "object_type": object_type,
                "object_name": object_name,
                "logs": logs,
                "error_type": "unsupported",
                "remediation": unsupported_rule,
                "retry_count": 0,
                "rows_migrated": 0,
            }

        final_sql = None
        if not data_only:
            log(f"Extracting source SQL for {object_type} {object_name}...")
            try:
                source_sql = extract_table_ddl(
                    source_cursor,
                    object_name,
                    source_config["database_type"],
                    object_type,
                    source_config["connection_details"],
                )
            except Exception as source_error:
                _append_raw_error(
                    diagnostics,
                    "source_extract",
                    source_error,
                )
                raise Exception(
                    f"Source SQL extraction failed for {object_type} {object_name}: {source_error}"
                ) from source_error
            log("Source SQL extracted")
            history_match = get_history_match(
                source_sql,
                source_config["database_type"],
                target_config["database_type"],
                object_type,
                object_name,
                source_connection_details=source_config["connection_details"],
                target_connection_details=target_config["connection_details"],
            )
            if history_match:
                log(
                    "History match found. Reusing previously successful transformed SQL "
                    f"({history_match.get('history_key', 'unknown')})."
                )
            log("Preparing target SQL...")
            try:
                transformed_sql, final_validation = _prepare_target_object_sql(
                    source_sql,
                    source_config,
                    target_config,
                    object_type,
                    object_name,
                    special_trigger_to_snowflake,
                    initial_sql=history_match.get("output_sql") if history_match else None,
                    diagnostics=diagnostics,
                )
            except Exception as prepare_error:
                _append_raw_error(
                    diagnostics,
                    "prepare_target_sql",
                    prepare_error,
                )
                prepare_error_text = str(prepare_error or "")
                if object_type == "storedprocedure" and (
                    "validation failed" in prepare_error_text.lower()
                    or "validator" in prepare_error_text.lower()
                ):
                    log("Validation failed")
                preparation_validation = _build_failed_validation(
                    f"Preparation failed: {prepare_error}"
                )
                diagnostics["validation"] = preparation_validation
                try:
                    suggestions = rag_agent.analyze(
                        source_sql or "",
                        "",
                        preparation_validation,
                        source_config["database_type"],
                        target_config["database_type"],
                        object_type,
                    )
                except Exception as rag_error:
                    _append_raw_error(
                        diagnostics,
                        "prepare_target_sql_rag_agent",
                        rag_error,
                    )
                    suggestions = []
                if suggestions:
                    diagnostics.setdefault("rag_suggestions", []).extend(suggestions)
                raise Exception(
                    f"Preparing target SQL failed for {object_type} {object_name}: {prepare_error}"
                ) from prepare_error
            if diagnostics.get("transform_strategy") == "history":
                log("History-based SQL reuse completed")
            elif diagnostics.get("transform_strategy") == "deterministic":
                log("Deterministic transformation completed")
            else:
                log("Agent-assisted transformation completed")
            if object_type == "storedprocedure" and diagnostics.get("storedprocedure_normalization_applied"):
                log("Stored procedure normalization applied")
            validation_warnings = final_validation.get("warnings") or []
            if validation_warnings:
                log(
                    "Target SQL validation passed with warnings: "
                    + "; ".join(validation_warnings)
                )
            else:
                log("Target SQL validation passed with no warnings")
            for rule_error in diagnostics.get("rule_errors") or []:
                log(
                    "Rule engine skipped faulty rule "
                    f"{rule_error.get('name', 'unnamed_rule')} "
                    f"pattern={rule_error.get('pattern', '')}: "
                    f"{rule_error.get('error', '')}"
                )

            final_sql = transformed_sql
            target_adapter = get_adapter(target_config["database_type"])
            if object_type in {"table", "view", "storedprocedure", "function", "trigger"}:
                resolved_target_object_name = _qualify_target_object_name(
                    target_config["database_type"],
                    object_type,
                    object_name,
                    target_config["connection_details"],
                )
                log(
                    f"Resolved target {object_type} name: "
                    f"{resolved_target_object_name}"
                )
            if object_type == "table" and drop_and_create_if_exists:
                if target_table_exists(
                    target_cursor,
                    target_config["database_type"],
                    object_name,
                    target_config["connection_details"],
                ):
                    log("Target table exists. Dropping before recreate.")
                    drop_target_table(
                        target_cursor,
                        target_config["database_type"],
                        object_name,
                        target_config["connection_details"],
                    )
            elif drop_and_create_if_exists and object_type in {"view", "storedprocedure", "function", "trigger"}:
                log(f"Drop-and-recreate enabled. Dropping target {object_type} before recreate.")
                _drop_target_object_if_exists(
                    target_cursor,
                    target_config["database_type"],
                    object_type,
                    object_name,
                    target_config["connection_details"],
                )
            last_error = None
            for attempt in range(SQL_REPAIR_ATTEMPTS + 1):
                try:
                    preflight_result = target_adapter.preflight_validate_sql(
                        target_cursor,
                        final_sql,
                        object_type,
                        object_name,
                        target_config["connection_details"],
                    )
                    preflight_status = preflight_result.get("status")
                    preflight_message = (
                        preflight_result.get("message")
                        or "Preflight validation failed."
                    )
                    if preflight_status == "success":
                        log(
                            "Preflight validation passed: "
                            f"{preflight_result.get('message')}"
                        )
                    elif preflight_status == "skipped":
                        log(
                            "Preflight validation skipped: "
                            f"{preflight_result.get('message')}"
                        )
                    else:
                        raise Exception(
                            f"Preflight validation failed: {preflight_message}"
                        )
                    if SHOW_SQL_LOGS:
                        _append_sql_snapshot(
                            diagnostics,
                            f"execute_attempt_{attempt}_final_sql",
                            final_sql,
                        )
                        log(f"Execution attempt {attempt} prepared")
                    log(
                        "Executing transformed SQL on target..."
                        if attempt == 0
                        else f"Executing repaired SQL on target (attempt {attempt})..."
                    )
                    if (
                        str(target_config["database_type"] or "").strip().lower() == "mysql"
                        and object_type in {"function", "storedprocedure"}
                    ):
                        _execute_mysql_routine_sql(target_config, final_sql)
                    else:
                        for statement in _split_sql_bundle(final_sql):
                            target_cursor.execute(statement)
                    if object_type == "table":
                        quoted_target_table = target_adapter.qualify_table_name(
                            object_name,
                            target_config["connection_details"],
                        )
                        target_cursor.execute(
                            f"SELECT * FROM {quoted_target_table} WHERE 1 = 0"
                        )
                    last_error = None
                    break
                except Exception as execution_error:
                    last_error = execution_error
                    _append_raw_error(
                        diagnostics,
                        f"execute_attempt_{attempt}",
                        execution_error,
                    )
                    try:
                        target_conn.rollback()
                    except Exception:
                        pass
                    if (
                        not drop_and_create_if_exists
                        and _is_already_exists_error(execution_error)
                    ):
                        reused_existing_target = True
                        last_error = None
                        log(
                            f"Target {object_type} already exists. Reusing existing target object and continuing."
                        )
                        break
                    execution_validation = {
                        "is_valid": False,
                        "errors": [str(execution_error)],
                        "warnings": [],
                    }
                    if object_type == "storedprocedure":
                        log("Execution failed")
                    else:
                        log(f"Target validation failed: {execution_error}")
                    try:
                        suggestions = rag_agent.analyze(
                            source_sql,
                            final_sql,
                            execution_validation,
                            source_config["database_type"],
                            target_config["database_type"],
                            object_type,
                        )
                    except Exception:
                        suggestions = []
                    if suggestions:
                        diagnostics.setdefault("rag_suggestions", []).extend(suggestions)
                    if attempt >= SQL_REPAIR_ATTEMPTS:
                        raise
                    if object_type == "storedprocedure":
                        log("Retrying with AI fix")
                    log("Applying automated SQL repair...")
                    previous_sql = final_sql
                    final_sql, repair_strategy = _repair_target_object_sql(
                        source_sql,
                        final_sql,
                        source_config,
                        target_config,
                        object_type,
                        object_name,
                        execution_error,
                        special_trigger_to_snowflake,
                        diagnostics=diagnostics,
                    )
                    if repair_strategy == "deterministic":
                        log("Deterministic SQL repair completed")
                    elif repair_strategy == "deterministic_trigger_fallback":
                        log("Deterministic trigger fallback completed")
                    else:
                        log("Agent-assisted SQL repair completed")
                    rule_result = apply_rules(
                        final_sql,
                        source_config["database_type"],
                        target_config["database_type"],
                    )
                    rule_errors = rule_result.get("rule_errors") or []
                    final_sql = rule_result.get("sql") or final_sql
                    if rule_result.get("applied_rules"):
                        diagnostics.setdefault("applied_rules", []).extend(rule_result.get("applied_rules") or [])
                    _record_rule_engine_failures(
                        source_sql,
                        final_sql,
                        source_config,
                        target_config,
                        object_type,
                        rule_errors,
                        diagnostics=diagnostics,
                    )
                    for rule_error in rule_errors:
                        log(
                            "Rule engine skipped faulty rule "
                            f"{rule_error.get('name', 'unnamed_rule')} "
                            f"pattern={rule_error.get('pattern', '')}: "
                            f"{rule_error.get('error', '')}"
                        )
                    if _standardize_sql_text(previous_sql) == _standardize_sql_text(final_sql):
                        raise Exception(
                            "Automated SQL repair produced no SQL changes; retry aborted before execution."
                        )
                    try:
                        final_sql, final_validation = _validate_sql_with_agent_retries(
                            source_sql,
                            final_sql,
                            source_config,
                            target_config,
                            object_type,
                            diagnostics=diagnostics,
                        )
                    except Exception:
                        if object_type == "storedprocedure":
                            log("Validation failed")
                        raise
                    validation_warnings = final_validation.get("warnings") or []
                    if validation_warnings:
                        log(
                            "Repaired SQL validation passed with warnings: "
                            + "; ".join(validation_warnings)
                        )
                    else:
                        log("Repaired SQL validation passed with no warnings")
                    retry_count = attempt + 1
            if last_error is not None:
                raise last_error
        else:
            log("Data-only mode enabled. Skipping schema transformation and DDL execution.")

        if migrate_data:
            if object_type not in {"table", "view"}:
                log(
                    "Migrate Data Also requested, but data copy is supported only "
                    "for tables and views. Skipping data migration."
                )
            else:
                log(f"Migrating {object_type} data to target...")
                if (
                    safe_replay_mode
                    and object_type == "table"
                    and data_migration_mode == "insert"
                    and not truncate_before_load
                    and target_table_exists(
                        target_cursor,
                        target_config["database_type"],
                        object_name,
                        target_config["connection_details"],
                    )
                ):
                    pre_count_summary = get_table_count_summary(
                        source_cursor,
                        target_cursor,
                        source_config["database_type"],
                        target_config["database_type"],
                        object_name,
                        source_config["connection_details"],
                        target_config["connection_details"],
                    )
                    source_row_count = pre_count_summary["source_row_count"]
                    target_row_count = pre_count_summary["target_row_count"]
                    missing_row_count = pre_count_summary["missing_row_count"]
                    if target_row_count >= source_row_count and missing_row_count == 0:
                        log(
                            "Target table already contains the current source row count. "
                            "Skipping data copy in safe replay mode to avoid duplicate inserts."
                        )
                        log(
                            f"Count validation completed. Source rows: {source_row_count}, "
                            f"Target rows: {target_row_count}, Missing rows: {missing_row_count}"
                        )
                        log("Migration SUCCESS")
                        return {
                            "status": "success",
                            "object_type": object_type,
                            "object_name": object_name,
                            "logs": logs,
                            "transformed_sql": final_sql,
                            "resolved_target_object_name": resolved_target_object_name,
                            "rows_migrated": 0,
                            "source_row_count": source_row_count,
                            "target_row_count": target_row_count,
                            "missing_row_count": missing_row_count,
                            "retry_count": retry_count,
                            "reused_existing_target": True,
                        }
                if truncate_before_load:
                    if object_type != "table":
                        raise Exception(
                            "Truncate before load is supported only for table migrations."
                        )
                    if target_table_exists(
                        target_cursor,
                        target_config["database_type"],
                        object_name,
                        target_config["connection_details"],
                    ):
                        truncate_target_table(
                            target_cursor,
                            target_config["database_type"],
                            object_name,
                            target_config["connection_details"],
                        )
                    else:
                        log("Target table not available. Truncate before load skipped.")
                rows_migrated = migrate_table_data(
                    source_cursor,
                    target_cursor,
                    source_config["database_type"],
                    target_config["database_type"],
                    object_name,
                    source_config["connection_details"],
                    target_config["connection_details"],
                    data_migration_mode,
                    data_batch_size,
                    execution_engine=data_execution_engine,
                    spark_row_threshold=spark_row_threshold,
                    spark_options=spark_options,
                    log_callback=log,
                )
                log(
                    f"Data migration completed. Rows migrated: {rows_migrated}. "
                    f"Mode: {data_migration_mode}. Batch size: {data_batch_size}. "
                    f"Engine: {data_execution_engine}"
                )
                if validate_row_counts:
                    count_summary = get_table_count_summary(
                        source_cursor,
                        target_cursor,
                        source_config["database_type"],
                        target_config["database_type"],
                        object_name,
                        source_config["connection_details"],
                        target_config["connection_details"],
                    )
                    source_row_count = count_summary["source_row_count"]
                    target_row_count = count_summary["target_row_count"]
                    missing_row_count = count_summary["missing_row_count"]
                    log(
                        "Count validation completed. "
                        f"Source rows: {source_row_count}, "
                        f"Target rows: {target_row_count}, "
                        f"Missing rows: {missing_row_count}"
                    )
                else:
                    log("Count validation skipped by request.")

        target_conn.commit()
        if not data_only and source_sql and final_sql and final_validation and final_validation.get("is_valid"):
            save_history(
                source_sql,
                final_sql,
                source_config["database_type"],
                target_config["database_type"],
                object_type,
                object_name,
                final_validation,
                status="SUCCESS",
                error=None,
                fix_attempts=diagnostics.get("agent_fix_attempts") or [],
                source_connection_details=source_config["connection_details"],
                target_connection_details=target_config["connection_details"],
            )
        if resolved_target_object_name:
            log(f"Final target object: {resolved_target_object_name}")
        log("Migration SUCCESS")
        return {
            "status": "success",
            "object_type": object_type,
            "object_name": object_name,
            "logs": logs,
            "transformed_sql": final_sql,
            "resolved_target_object_name": resolved_target_object_name,
            "diagnostics": diagnostics,
            "rows_migrated": rows_migrated,
            "source_row_count": source_row_count,
            "target_row_count": target_row_count,
            "missing_row_count": missing_row_count,
            "retry_count": retry_count,
            "reused_existing_target": reused_existing_target,
        }
    except Exception as error:
        try:
            target_conn.rollback()
        except Exception:
            pass
        error_text = str(error)
        if diagnostics.get("raw_errors"):
            latest_raw_error = diagnostics["raw_errors"][-1]
            log(
                "Raw failure "
                f"[{latest_raw_error.get('stage', 'unknown')}]: "
                f"{latest_raw_error.get('error', error_text)}"
            )
        error_type = _classify_error(error_text)
        remediation = _build_remediation_hint(
            target_config["database_type"], object_type, error_text
        )
        surfaced_error_text = error_text
        if diagnostics.get("raw_errors"):
            surfaced_error_text = str(diagnostics["raw_errors"][-1].get("error") or error_text)
        log(f"ERROR: {surfaced_error_text}")
        if source_sql:
            save_history(
                source_sql,
                final_sql or "",
                source_config["database_type"],
                target_config["database_type"],
                object_type,
                object_name,
                diagnostics.get("validation") or _build_failed_validation(surfaced_error_text),
                status="ERROR",
                error=surfaced_error_text,
                fix_attempts=diagnostics.get("agent_fix_attempts") or [],
                source_connection_details=source_config["connection_details"],
                target_connection_details=target_config["connection_details"],
            )
        return {
            "status": "error",
            "object_type": object_type,
            "object_name": object_name,
            "logs": logs,
            "error": surfaced_error_text,
            "resolved_target_object_name": resolved_target_object_name,
            "error_type": error_type,
            "remediation": remediation,
            "diagnostics": diagnostics,
            "retry_count": retry_count,
            "rows_migrated": rows_migrated,
            "source_row_count": source_row_count,
            "target_row_count": target_row_count,
            "missing_row_count": missing_row_count,
        }


def _run_agent_migrate(req: AgentMigrationRequest, log_callback=None, event_callback=None, job_context=None):
    logs = []
    source_conn = None
    target_conn = None
    run_id = str(uuid4())
    run_started_at = datetime.now()
    if event_callback:
        event_callback("meta", {"run_id": run_id})

    def log(msg):
        entry = f"{datetime.now()} - {msg}"
        logs.append(entry)
        if log_callback:
            log_callback(entry)

    try:
        get_llm_runtime_info()
        log("Schema transformation engine initialized.")

        log("Connecting to source DB...")
        source_conn = get_connection(
            req.source_config["database_type"],
            req.source_config["connection_details"],
        )
        source_cursor = source_conn.cursor()

        log("Connecting to target DB...")
        target_conn, target_cursor = _connect_target_with_namespace_bootstrap(
            req.target_config,
            log_callback=log_callback,
        )

        object_started_at = datetime.now()
        object_result = _migrate_object(
            source_cursor,
            target_conn,
            target_cursor,
            req.source_config,
            req.target_config,
            req.object_type,
            req.object_name,
            req.migrate_data,
            req.data_only,
            req.data_migration_mode,
            req.data_batch_size,
            req.data_execution_engine,
            req.spark_row_threshold,
            req.validate_row_counts,
            req.spark_options,
            req.truncate_before_load,
            req.drop_and_create_if_exists,
            safe_replay_mode=bool((job_context or {}).get("parent_job_id")),
            log_callback=log_callback,
        )
        object_completed_at = datetime.now()
        object_stats = _build_object_stats_result(
            run_id,
            object_result,
            req.source_config["database_type"],
            req.target_config["database_type"],
            object_started_at,
            object_completed_at,
        )
        run_completed_at = datetime.now()
        run_summary = _build_run_summary(
            run_id,
            object_result["status"],
            req.source_config["database_type"],
            req.target_config["database_type"],
            [req.object_type],
            [object_stats],
            run_started_at,
            run_completed_at,
        )

        response = {
            "status": object_result["status"],
            "logs": logs,
            "object_result": object_result,
            "run_summary": run_summary.model_dump(mode="json"),
        }
        if object_result.get("transformed_sql"):
            response["transformed_sql"] = object_result["transformed_sql"]
        if req.show_transformed_queries:
            response["transformed_queries"] = _collect_transformed_queries([object_result])
        append_migration_run(
            _with_job_request(
                _build_history_record(
                    run_summary,
                    logs,
                    results=[object_result],
                    summary=run_summary.stats.model_dump(mode="json"),
                ),
                _serialize_request(
                    req,
                    "single",
                    parent_job_id=(job_context or {}).get("parent_job_id"),
                    trigger_type=(job_context or {}).get("trigger_type", "manual"),
                    event_name=(job_context or {}).get("event_name"),
                ),
            )
        )
        append_migration_diagnostics(
            _build_diagnostics_record(
                run_id,
                "single",
                req.source_config["database_type"],
                req.target_config["database_type"],
                [req.object_type],
                [object_result],
                run_started_at,
                run_completed_at,
            )
        )
        return response
    except Exception as e:
        if target_conn:
            try:
                target_conn.rollback()
            except Exception:
                pass
        log("ERROR: Migration pipeline failed before object execution completed.")
        run_completed_at = datetime.now()
        run_summary = _build_run_summary(
            run_id,
            "error",
            req.source_config["database_type"],
            req.target_config["database_type"],
            [req.object_type],
            [],
            run_started_at,
            run_completed_at,
        )
        response = {
            "status": "error",
            "logs": logs,
            "run_summary": run_summary.model_dump(mode="json"),
        }
        append_migration_run(
            _with_job_request(
                _build_history_record(
                    run_summary,
                    logs,
                    results=[],
                    summary=run_summary.stats.model_dump(mode="json"),
                ),
                _serialize_request(
                    req,
                    "single",
                    parent_job_id=(job_context or {}).get("parent_job_id"),
                    trigger_type=(job_context or {}).get("trigger_type", "manual"),
                    event_name=(job_context or {}).get("event_name"),
                ),
            )
        )
        append_migration_diagnostics(
            _build_diagnostics_record(
                run_id,
                "single",
                req.source_config["database_type"],
                req.target_config["database_type"],
                [req.object_type],
                [],
                run_started_at,
                run_completed_at,
            )
        )
        return response
    finally:
        if source_conn:
            source_conn.close()
        if target_conn:
            target_conn.close()

def _run_agent_migrate_bulk(req: BulkAgentMigrationRequest, log_callback=None, event_callback=None, job_context=None):
    logs = []
    results = []
    object_stats_results = []
    source_conn = None
    target_conn = None
    run_id = str(uuid4())
    run_started_at = datetime.now()
    stop_event = _register_run_control(run_id)
    first_resume_checkpoint = None
    if event_callback:
        event_callback("meta", {"run_id": run_id})

    def log(msg):
        entry = f"{datetime.now()} - {msg}"
        logs.append(entry)
        if log_callback:
            log_callback(entry)

    try:
        selected_types = {item for item in req.object_types if item}
        selected_objects_by_type = {
            str(object_type): [str(item) for item in (object_names or []) if item]
            for object_type, object_names in (req.selected_objects or {}).items()
        }
        if not selected_types:
            raise Exception("At least one object type must be selected for bulk migration.")

        source_details = req.source_config["connection_details"]
        source_database = source_details.get("database")
        source_schema = source_details.get("schema") or source_database
        if not source_database or not source_schema:
            raise Exception("Bulk migration requires source database and schema.")

        get_llm_runtime_info()
        execution_order = _build_execution_order(selected_types)
        resume_from = req.resume_from or {}
        resume_active = bool(resume_from.get("object_type") and resume_from.get("object_name"))
        previous_successful_objects = set()
        previous_run_id = resume_from.get("run_id")
        if previous_run_id:
            previous_run = get_migration_run(previous_run_id)
            if previous_run:
                previous_successful_objects = {
                    (item.get("object_type"), item.get("object_name"))
                    for item in ((previous_run.get("run_summary") or {}).get("object_results") or [])
                    if item.get("status") == "success"
                }
        log("Schema transformation engine initialized.")
        log("Bulk migration execution order -> " + ", ".join(execution_order))

        safe_replay_mode = bool(previous_run_id or (job_context or {}).get("parent_job_id"))

        log("Connecting to source DB...")
        source_conn = get_connection(
            req.source_config["database_type"],
            source_details,
        )
        source_cursor = source_conn.cursor()

        log("Connecting to target DB...")
        target_conn, target_cursor = _connect_target_with_namespace_bootstrap(
            req.target_config,
            log_callback=log_callback,
        )
        failed_types = set()

        for object_type in execution_order:
            initial_object_names = list_objects(
                source_cursor,
                req.source_config["database_type"],
                source_database,
                source_schema,
                object_type,
            )
            if selected_objects_by_type.get(object_type):
                selected_name_set = set(selected_objects_by_type[object_type])
                initial_object_names = [
                    item for item in initial_object_names if item in selected_name_set
                ]
            object_names = list(initial_object_names)
            log(f"Queued {len(object_names)} {object_type} object(s) for migration.")
            deferred_object_names = []
            deferred_attempted = set()
            object_index = 0
            while object_index < len(object_names):
                object_name = object_names[object_index]
                object_index += 1
                if resume_active:
                    if (
                        object_type == resume_from.get("object_type")
                        and object_name == resume_from.get("object_name")
                    ):
                        resume_active = False
                        log(
                            f"Resume checkpoint reached at {object_type} {object_name}. Continuing migration from this object."
                        )
                    else:
                        continue
                if previous_successful_objects and (object_type, object_name) in previous_successful_objects:
                    reason = "Skipped because this object already succeeded in the previous run."
                    object_started_at = datetime.now()
                    result = _build_skipped_result(object_type, object_name, reason)
                    results.append(result)
                    for entry in result["logs"]:
                        logs.append(entry)
                        if log_callback:
                            log_callback(entry)
                    object_stats_results.append(
                        _build_object_stats_result(
                            run_id,
                            result,
                            req.source_config["database_type"],
                            req.target_config["database_type"],
                            object_started_at,
                            datetime.now(),
                        )
                    )
                    continue
                if stop_event.is_set():
                    checkpoint = {
                        "object_type": object_type,
                        "object_name": object_name,
                        "run_id": run_id,
                    }
                    if not first_resume_checkpoint:
                        first_resume_checkpoint = checkpoint
                    log(
                        f"STOPPED: Stop requested. Next resume checkpoint -> {object_type} {object_name}."
                    )
                    status = "stopped"
                    run_completed_at = datetime.now()
                    run_summary = _build_run_summary(
                        run_id,
                        status,
                        req.source_config["database_type"],
                        req.target_config["database_type"],
                        execution_order,
                        object_stats_results,
                        run_started_at,
                        run_completed_at,
                    )
                    response = {
                        "status": status,
                        "logs": logs,
                        "execution_order": execution_order,
                        "results": results,
                        "resume_checkpoint": first_resume_checkpoint,
                        "run_summary": run_summary.model_dump(mode="json"),
                        "summary": {
                            "total": len(results),
                            "success": sum(1 for item in results if item["status"] == "success"),
                            "error": sum(1 for item in results if item["status"] == "error"),
                            "skipped": sum(1 for item in results if item["status"] == "skipped"),
                        },
                    }
                    if req.show_transformed_queries:
                        response["transformed_queries"] = _collect_transformed_queries(results)
                    append_migration_run(
                        _with_job_request(
                            _build_history_record(
                                run_summary,
                                logs,
                                results=results,
                                summary=run_summary.stats.model_dump(mode="json"),
                            ),
                            _serialize_request(
                                req,
                                "bulk",
                                parent_job_id=(job_context or {}).get("parent_job_id"),
                                trigger_type=(job_context or {}).get("trigger_type", "manual"),
                                event_name=(job_context or {}).get("event_name"),
                            ),
                        )
                    )
                    append_migration_diagnostics(
                        _build_diagnostics_record(
                            run_id,
                            "bulk",
                            req.source_config["database_type"],
                            req.target_config["database_type"],
                            execution_order,
                            results,
                            run_started_at,
                            run_completed_at,
                        )
                    )
                    return response
                blockers = DEPENDENCY_BLOCKERS.get(object_type, set())
                if failed_types & blockers:
                    reason = (
                        f"Skipped because prerequisite object types failed earlier: "
                        f"{', '.join(sorted(failed_types & blockers))}."
                    )
                    object_started_at = datetime.now()
                    result = _build_skipped_result(object_type, object_name, reason)
                    results.append(result)
                    for entry in result["logs"]:
                        logs.append(entry)
                        if log_callback:
                            log_callback(entry)
                    object_stats_results.append(
                        _build_object_stats_result(
                            run_id,
                            result,
                            req.source_config["database_type"],
                            req.target_config["database_type"],
                            object_started_at,
                            datetime.now(),
                        )
                    )
                    continue
                log(f"Starting {object_type} {object_name}...")
                object_started_at = datetime.now()
                result = _migrate_object(
                    source_cursor,
                    target_conn,
                    target_cursor,
                    req.source_config,
                    req.target_config,
                    object_type,
                    object_name,
                    req.migrate_data,
                    req.data_only,
                    req.data_migration_mode,
                    req.data_batch_size,
                    req.data_execution_engine,
                    req.spark_row_threshold,
                    req.validate_row_counts,
                    req.spark_options,
                    req.truncate_before_load,
                    req.drop_and_create_if_exists,
                    safe_replay_mode=safe_replay_mode,
                    log_callback=log_callback,
                )
                if (
                    object_type == "table"
                    and _is_retryable_table_dependency_error(
                        result,
                        req.target_config["database_type"],
                    )
                    and object_name not in deferred_attempted
                ):
                    deferred_attempted.add(object_name)
                    deferred_object_names.append(object_name)
                    log(
                        f"Deferred table {object_name} due to missing referenced target object. "
                        "Will retry after remaining tables."
                    )
                    continue
                results.append(result)
                object_stats_results.append(
                    _build_object_stats_result(
                        run_id,
                        result,
                        req.source_config["database_type"],
                        req.target_config["database_type"],
                        object_started_at,
                        datetime.now(),
                    )
                )
                if result["status"] == "error":
                    failed_types.add(object_type)
                    if not first_resume_checkpoint:
                        first_resume_checkpoint = {
                            "object_type": object_type,
                            "object_name": object_name,
                            "run_id": run_id,
                        }
                if result["status"] == "error" and not req.continue_on_error:
                    raise Exception(
                        f"Bulk migration stopped after failure on {object_type} {object_name}."
                    )
            if deferred_object_names:
                log(
                    f"Retrying {len(deferred_object_names)} deferred table object(s) after first pass."
                )
                for object_name in deferred_object_names:
                    if stop_event.is_set():
                        checkpoint = {
                            "object_type": object_type,
                            "object_name": object_name,
                            "run_id": run_id,
                        }
                        if not first_resume_checkpoint:
                            first_resume_checkpoint = checkpoint
                        log(
                            f"STOPPED: Stop requested. Next resume checkpoint -> {object_type} {object_name}."
                        )
                        status = "stopped"
                        run_completed_at = datetime.now()
                        run_summary = _build_run_summary(
                            run_id,
                            status,
                            req.source_config["database_type"],
                            req.target_config["database_type"],
                            execution_order,
                            object_stats_results,
                            run_started_at,
                            run_completed_at,
                        )
                        response = {
                            "status": status,
                            "logs": logs,
                            "execution_order": execution_order,
                            "results": results,
                            "resume_checkpoint": first_resume_checkpoint,
                            "run_summary": run_summary.model_dump(mode="json"),
                            "summary": {
                                "total": len(results),
                                "success": sum(1 for item in results if item["status"] == "success"),
                                "error": sum(1 for item in results if item["status"] == "error"),
                                "skipped": sum(1 for item in results if item["status"] == "skipped"),
                            },
                        }
                        if req.show_transformed_queries:
                            response["transformed_queries"] = _collect_transformed_queries(results)
                        append_migration_run(
                            _with_job_request(
                                _build_history_record(
                                    run_summary,
                                    logs,
                                    results=results,
                                    summary=run_summary.stats.model_dump(mode="json"),
                                ),
                                _serialize_request(
                                    req,
                                    "bulk",
                                    parent_job_id=(job_context or {}).get("parent_job_id"),
                                    trigger_type=(job_context or {}).get("trigger_type", "manual"),
                                    event_name=(job_context or {}).get("event_name"),
                                ),
                            )
                        )
                        append_migration_diagnostics(
                            _build_diagnostics_record(
                                run_id,
                                "bulk",
                                req.source_config["database_type"],
                                req.target_config["database_type"],
                                execution_order,
                                results,
                                run_started_at,
                                run_completed_at,
                            )
                        )
                        return response
                    log(f"Retrying deferred table {object_name}...")
                    object_started_at = datetime.now()
                    result = _migrate_object(
                        source_cursor,
                        target_conn,
                        target_cursor,
                        req.source_config,
                        req.target_config,
                        object_type,
                        object_name,
                        req.migrate_data,
                        req.data_only,
                        req.data_migration_mode,
                        req.data_batch_size,
                        req.data_execution_engine,
                        req.spark_row_threshold,
                        req.validate_row_counts,
                        req.spark_options,
                        req.truncate_before_load,
                        req.drop_and_create_if_exists,
                        safe_replay_mode=safe_replay_mode,
                        log_callback=log_callback,
                    )
                    results.append(result)
                    object_stats_results.append(
                        _build_object_stats_result(
                            run_id,
                            result,
                            req.source_config["database_type"],
                            req.target_config["database_type"],
                            object_started_at,
                            datetime.now(),
                        )
                    )
                    if result["status"] == "error":
                        failed_types.add(object_type)
                        if not first_resume_checkpoint:
                            first_resume_checkpoint = {
                                "object_type": object_type,
                                "object_name": object_name,
                                "run_id": run_id,
                            }
                    if result["status"] == "error" and not req.continue_on_error:
                        raise Exception(
                            f"Bulk migration stopped after failure on {object_type} {object_name}."
                        )

        status = "success" if all(item["status"] == "success" for item in results) else "partial"
        if not results:
            status = "success"
            log("Bulk migration completed with no matching objects.")
        run_completed_at = datetime.now()
        run_summary = _build_run_summary(
            run_id,
            status,
            req.source_config["database_type"],
            req.target_config["database_type"],
            execution_order,
            object_stats_results,
            run_started_at,
            run_completed_at,
        )

        response = {
            "status": status,
            "logs": logs,
            "execution_order": execution_order,
            "results": results,
            "resume_checkpoint": first_resume_checkpoint,
            "run_summary": run_summary.model_dump(mode="json"),
            "summary": {
                "total": len(results),
                "success": sum(1 for item in results if item["status"] == "success"),
                "error": sum(1 for item in results if item["status"] == "error"),
                "skipped": sum(1 for item in results if item["status"] == "skipped"),
            },
        }
        if req.show_transformed_queries:
            response["transformed_queries"] = _collect_transformed_queries(results)
        append_migration_run(
            _with_job_request(
                _build_history_record(
                    run_summary,
                    logs,
                    results=results,
                    summary=run_summary.stats.model_dump(mode="json"),
                ),
                _serialize_request(
                    req,
                    "bulk",
                    parent_job_id=(job_context or {}).get("parent_job_id"),
                    trigger_type=(job_context or {}).get("trigger_type", "manual"),
                    event_name=(job_context or {}).get("event_name"),
                ),
            )
        )
        append_migration_diagnostics(
            _build_diagnostics_record(
                run_id,
                "bulk",
                req.source_config["database_type"],
                req.target_config["database_type"],
                execution_order,
                results,
                run_started_at,
                run_completed_at,
            )
        )
        return response
    except Exception as e:
        if target_conn:
            try:
                target_conn.rollback()
            except Exception:
                pass
        log("ERROR: Bulk migration pipeline failed before completion.")
        run_completed_at = datetime.now()
        run_summary = _build_run_summary(
            run_id,
            "error",
            req.source_config["database_type"],
            req.target_config["database_type"],
            _build_execution_order({item for item in req.object_types if item}),
            object_stats_results,
            run_started_at,
            run_completed_at,
        )
        response = {
            "status": "error",
            "logs": logs,
            "results": results,
            "resume_checkpoint": first_resume_checkpoint,
            "run_summary": run_summary.model_dump(mode="json"),
            "summary": {
                "total": len(results),
                "success": sum(1 for item in results if item["status"] == "success"),
                "error": sum(1 for item in results if item["status"] == "error"),
                "skipped": sum(1 for item in results if item["status"] == "skipped"),
            },
        }
        if req.show_transformed_queries:
            response["transformed_queries"] = _collect_transformed_queries(results)
        append_migration_run(
            _with_job_request(
                _build_history_record(
                    run_summary,
                    logs,
                    results=results,
                    summary=run_summary.stats.model_dump(mode="json"),
                ),
                _serialize_request(
                    req,
                    "bulk",
                    parent_job_id=(job_context or {}).get("parent_job_id"),
                    trigger_type=(job_context or {}).get("trigger_type", "manual"),
                    event_name=(job_context or {}).get("event_name"),
                ),
            )
        )
        append_migration_diagnostics(
            _build_diagnostics_record(
                run_id,
                "bulk",
                req.source_config["database_type"],
                req.target_config["database_type"],
                _build_execution_order({item for item in req.object_types if item}),
                results,
                run_started_at,
                run_completed_at,
            )
        )
        return response
    finally:
        _clear_run_control(run_id)
        if source_conn:
            source_conn.close()
        if target_conn:
            target_conn.close()


@router.post("/capabilities")
def adapter_capabilities(req: CapabilityRequest):
    adapter = get_adapter(req.database_type)
    return {
        "status": "success",
        "engine": adapter.engine_name,
        "capabilities": adapter.capabilities.__dict__,
    }


@router.get("/migration-diagnostics")
def migration_diagnostics(limit: int = 50):
    return {
        "status": "success",
        "items": list_migration_diagnostics(limit=limit),
    }


@router.get("/migration-diagnostics/{run_id}")
def migration_diagnostics_item(run_id: str):
    item = get_migration_diagnostics(run_id)
    if not item:
        return {
            "status": "error",
            "message": f"Diagnostics not found for run_id={run_id}",
        }
    return {
        "status": "success",
        "item": item,
    }


@router.post("/test-connection")
def test_connection(req: ConnectionTestRequest):
    conn = None
    try:
        conn = get_connection(req.database_type, req.connection_details)
        return {
            "status": "success",
            "message": "Connection successful",
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"test-connection failed: {str(e)}",
            "debug": traceback.format_exc(),
        }
    finally:
        if conn:
            conn.close()


@router.post("/metadata/databases")
def metadata_databases(req: MetadataRequest):
    conn = None
    try:
        conn = get_connection(req.database_type, req.connection_details)
        cursor = conn.cursor()
        return {
            "status": "success",
            "items": list_databases(cursor, req.database_type),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"metadata/databases failed: {str(e)}",
            "debug": traceback.format_exc(),
        }
    finally:
        if conn:
            conn.close()


@router.post("/metadata/schemas")
def metadata_schemas(req: MetadataRequest):
    conn = None
    try:
        details = dict(req.connection_details)
        if req.database_name:
            details["database"] = req.database_name
        conn = get_connection(req.database_type, details)
        cursor = conn.cursor()
        return {
            "status": "success",
            "items": list_schemas(cursor, req.database_type, req.database_name),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"metadata/schemas failed: {str(e)}",
            "debug": traceback.format_exc(),
        }
    finally:
        if conn:
            conn.close()


@router.post("/metadata/object-summary")
def metadata_object_summary(req: MetadataRequest):
    conn = None
    try:
        details = dict(req.connection_details)
        if req.database_name:
            details["database"] = req.database_name
        conn = get_connection(req.database_type, details)
        cursor = conn.cursor()
        schema_name = req.schema_name or req.database_name
        return {
            "status": "success",
            "items": get_object_summary(
                cursor,
                req.database_type,
                req.database_name,
                schema_name,
            ),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"metadata/object-summary failed: {str(e)}",
            "debug": traceback.format_exc(),
        }
    finally:
        if conn:
            conn.close()


@router.post("/metadata/objects")
def metadata_objects(req: MetadataRequest):
    conn = None
    try:
        details = dict(req.connection_details)
        if req.database_name:
            details["database"] = req.database_name
        conn = get_connection(req.database_type, details)
        cursor = conn.cursor()
        schema_name = req.schema_name or req.database_name
        return {
            "status": "success",
            "items": list_objects(
                cursor,
                req.database_type,
                req.database_name,
                schema_name,
                req.object_type,
            ),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"metadata/objects failed: {str(e)}",
            "debug": traceback.format_exc(),
        }
    finally:
        if conn:
            conn.close()


@router.get("/migration-history")
def migration_history(limit: int = 100):
    return {
        "status": "success",
        "items": list_migration_runs(limit=limit),
    }


@router.get("/migration-history/{run_id}")
def migration_history_item(run_id: str):
    item = get_migration_run(run_id)
    if not item:
        return {
            "status": "error",
            "message": f"Migration run not found: {run_id}",
        }
    return {
        "status": "success",
        "item": item,
    }


@router.get("/migration/{run_id}/queries")
def migration_run_queries(run_id: str):
    item = get_migration_run(run_id)
    if not item:
        return {
            "status": "error",
            "message": f"Migration run not found: {run_id}",
            "full_script": "",
            "objects": [],
        }
    payload = _build_queries_payload(item)
    return {
        "status": "success",
        **payload,
    }


@router.get("/jobs")
def list_jobs(limit: int = 100):
    schedule_map = list_job_schedules()
    job_items = [
        _build_job_summary(record, schedule_map.get((record.get("run_summary") or {}).get("run_id")))
        for record in list_migration_runs(limit=limit)
        if (record.get("run_summary") or {}).get("run_id")
    ]
    return {
        "status": "success",
        "items": [item.model_dump(mode="json") for item in job_items],
    }


@router.get("/jobs/{job_id}")
def get_job(job_id: str):
    record = get_migration_run(job_id)
    if not record:
        return {
            "status": "error",
            "message": f"Job not found: {job_id}",
        }
    job = _build_job_summary(record, get_job_schedule(job_id))
    return {
        "status": "success",
        "item": job.model_dump(mode="json"),
    }


@router.post("/jobs/{job_id}/schedule")
def save_job_schedule(job_id: str, req: JobScheduleRequest):
    record = get_migration_run(job_id)
    if not record:
        return {
            "status": "error",
            "message": f"Job not found: {job_id}",
        }
    if req.trigger_type == "scheduled_trigger" and not req.cron_expression:
        return {
            "status": "error",
            "message": "Cron expression is required for scheduled triggers.",
        }
    if req.trigger_type == "event_trigger" and not req.event_name:
        return {
            "status": "error",
            "message": "Event name is required for event triggers.",
        }
    existing = get_job_schedule(job_id) or {}
    created_at = existing.get("created_at") or datetime.utcnow().isoformat()
    schedule = JobScheduleConfig(
        job_id=job_id,
        trigger_type=req.trigger_type,
        enabled=req.enabled,
        description=req.description,
        timezone=req.timezone or "Asia/Calcutta",
        cron_expression=req.cron_expression if req.trigger_type == "scheduled_trigger" else None,
        start_at=_parse_datetime_or_none(req.start_at),
        event_name=req.event_name if req.trigger_type == "event_trigger" else None,
        created_at=_parse_datetime_or_none(created_at) or datetime.utcnow(),
        updated_at=datetime.utcnow(),
        last_triggered_at=_parse_datetime_or_none(existing.get("last_triggered_at")),
        trigger_count=int(existing.get("trigger_count") or 0),
        last_run_id=existing.get("last_run_id"),
        last_run_status=existing.get("last_run_status"),
        next_run_at=None,
    )
    if req.trigger_type == "scheduled_trigger" and req.enabled and req.cron_expression:
        from app.services.job_scheduler import _compute_next_run

        schedule.next_run_at = _parse_datetime_or_none(
            _compute_next_run(req.cron_expression, req.timezone or "Asia/Calcutta", datetime.utcnow())
        )
    saved = upsert_job_schedule(job_id, schedule.model_dump(mode="json"))
    return {
        "status": "success",
        "item": saved,
        "message": "Job schedule saved successfully.",
    }


@router.post("/jobs/{job_id}/event-trigger")
def trigger_job_event(job_id: str, event_name: str | None = None):
    record = get_migration_run(job_id)
    if not record:
        return {
            "status": "error",
            "message": f"Job not found: {job_id}",
        }
    schedule = get_job_schedule(job_id)
    if not schedule:
        return {
            "status": "error",
            "message": "Create an event trigger schedule before recording an event.",
        }
    if schedule.get("trigger_type") != "event_trigger":
        return {
            "status": "error",
            "message": "Only event trigger schedules can accept event executions.",
        }
    if not schedule.get("enabled", True):
        return {
            "status": "error",
            "message": "This event trigger schedule is disabled.",
        }
    triggered_at = datetime.utcnow().isoformat()
    updated = record_schedule_event(job_id, event_name=event_name, triggered_at=triggered_at)
    trigger_job_run(job_id, rerun_saved_job, trigger_type="event_trigger", event_name=event_name)
    return {
        "status": "success",
        "item": updated,
        "message": f"Event trigger recorded for job {job_id}.",
    }


@router.post("/jobs/{job_id}/rerun")
def rerun_job(job_id: str):
    record = get_migration_run(job_id)
    if not record:
        return {
            "status": "error",
            "message": f"Job not found: {job_id}",
        }
    trigger_job_run(job_id, rerun_saved_job, trigger_type="manual")
    return {
        "status": "success",
        "message": f"Job rerun started for {job_id}.",
    }


@router.post("/agent-migrate")
def agent_migrate(req: AgentMigrationRequest):
    return _run_agent_migrate(req)


@router.post("/migrate", response_model=SqlMigrationResponse)
def migrate_sql_endpoint(req: SqlMigrationRequest):
    return migrate_sql(
        req.input_sql,
        req.source,
        req.target,
        req.object_type,
        req.object_name,
    )


@router.get("/rag-agent/status", response_model=RagAgentStatusResponse)
def rag_agent_status():
    return get_rag_agent_runtime_info()


@router.post("/agent-migrate/stream")
def agent_migrate_stream(req: AgentMigrationRequest):
    return StreamingResponse(
        _stream_events(
            lambda log_callback, event_callback: _run_agent_migrate(
                req, log_callback, event_callback
            )
        ),
        media_type="application/x-ndjson",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate, no-transform",
            "Pragma": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.post("/agent-migrate/bulk")
def agent_migrate_bulk(req: BulkAgentMigrationRequest):
    return _run_agent_migrate_bulk(req)


@router.post("/agent-migrate/bulk/stream")
def agent_migrate_bulk_stream(req: BulkAgentMigrationRequest):
    return StreamingResponse(
        _stream_events(
            lambda log_callback, event_callback: _run_agent_migrate_bulk(
                req, log_callback, event_callback
            )
        ),
        media_type="application/x-ndjson",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate, no-transform",
            "Pragma": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.post("/migration-control/stop/{run_id}")
def stop_migration(run_id: str):
    stop_event = _get_run_control(run_id)
    if not stop_event:
        return {
            "status": "error",
            "message": f"Migration run not found or already finished: {run_id}",
        }
    stop_event.set()
    return {
        "status": "success",
        "message": f"Stop requested for run {run_id}. The current object will finish first.",
    }
