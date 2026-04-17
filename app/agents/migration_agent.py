import os
import re
from functools import lru_cache

from openai import OpenAI

from app.config import load_environment
from app.services.deterministic_transformer import transform_deterministically
from app.services.rag_service import build_rag_context
from app.utils.prompt_loader import load_prompt_template
from app.utils.rule_loader import get_object_rules, get_target_rules


SQL_BUNDLE_DELIMITER = "\n-- SQL_BUNDLE_DELIMITER --\n"

load_environment()

DEFAULT_MODEL = os.getenv("MIGRATION_AGENT_MODEL") or "openai/gpt-oss-120b"


def _clean_sql_output(text: str) -> str:
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.replace("```sql", "").replace("```", "").strip()
    return cleaned


def _format_rule_block(lines: list[str]) -> str:
    if not lines:
        return "- No additional dialect-specific rules."
    return "\n".join(f"- {line}" for line in lines)


def _format_rag_context(source_db: str, target_db: str, object_type: str) -> str:
    rag_context = build_rag_context(source_db, target_db).strip()
    procedure_guidance = []
    if str(object_type or "").strip().lower() == "storedprocedure":
        procedure_guidance.extend(
            [
                "Stored procedure rules:",
                "- Do not omit the CREATE PROCEDURE header.",
                "- For MySQL procedures, produce a BEGIN ... END body.",
                "- Keep DECLARE statements inside BEGIN ... END and place variable declarations before cursors and handlers.",
                "- If a NOT FOUND or SQLEXCEPTION handler is declared, it must have a valid action block or statement.",
                "- Do not emit placeholder routine bodies or partial cursor loops.",
            ]
        )
    sections = []
    if rag_context:
        sections.append("RAG knowledge base context:\n" + rag_context)
    if procedure_guidance:
        sections.append("\n".join(procedure_guidance))
    if not sections:
        return "- No RAG context available."
    return "\n\n".join(sections)


def _sanitize_identifier(value: str, fallback: str = "migration_object") -> str:
    normalized = re.sub(r"[^0-9A-Za-z_]+", "_", value or "").strip("_")
    if not normalized:
        return fallback
    if normalized[0].isdigit():
        normalized = f"obj_{normalized}"
    return normalized.lower()


def _extract_trigger_table_name(source_sql: str, fallback_name: str) -> str:
    text = str(source_sql or "")
    patterns = [
        r"(?is)\bon\s+`?([A-Za-z0-9_$.]+)`?\b",
        r'(?is)\bon\s+"?([A-Za-z0-9_$.]+)"?\b',
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1).split(".")[-1]
    return f"{fallback_name}_table"


def _extract_trigger_event(source_sql: str) -> str:
    text = str(source_sql or "").upper()
    for event in ("INSERT", "UPDATE", "DELETE"):
        if event in text:
            return event
    return "INSERT"


@lru_cache(maxsize=1)
def _get_client() -> OpenAI | None:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return None
    return OpenAI(api_key=api_key, base_url="https://api.groq.com/openai/v1")


def get_llm_runtime_info() -> dict[str, str | bool]:
    client = _get_client()
    return {
        "provider": "groq",
        "model": DEFAULT_MODEL,
        "configured": client is not None,
    }


def _call_llm(prompt: str) -> str | None:
    client = _get_client()
    if client is None:
        return None

    response = client.chat.completions.create(
        model=DEFAULT_MODEL,
        messages=[
            {"role": "system", "content": load_prompt_template("system_sql_engine.txt")},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
    )
    return _clean_sql_output(response.choices[0].message.content or "")


def _get_extra_rules(source_db: str, target_db: str, object_type: str) -> list[str]:
    normalized_source_db = str(source_db or "").strip().lower()
    normalized_target_db = str(target_db or "").strip().lower()
    if object_type == "trigger" and normalized_target_db == "snowflake":
        return [
            "Do not output CREATE TRIGGER because Snowflake does not support table triggers.",
            "Output a Snowflake bundle that uses CREATE STREAM and CREATE TASK.",
            f"Separate multiple statements with the literal delimiter {SQL_BUNDLE_DELIMITER.strip()}.",
        ]
    if object_type == "trigger" and normalized_source_db == "snowflake" and normalized_target_db != "snowflake":
        return [
            "Interpret the source Snowflake TASK/STREAM bundle as a trigger-equivalent workflow.",
            "Output exactly one valid CREATE TRIGGER statement for the target database.",
            "Do not return Snowflake CREATE TASK or CREATE STREAM statements.",
        ]
    return []


def _build_prompt(template_name: str, **values) -> str:
    template = load_prompt_template(template_name)
    return template.format(**values)


def generate_transformed_sql(
    source_sql: str,
    source_db: str,
    target_db: str,
    object_type: str,
    object_name: str | None = None,
) -> str:
    deterministic_sql = transform_deterministically(
        source_sql,
        source_db,
        target_db,
        object_type,
        object_name,
    )
    if deterministic_sql:
        return deterministic_sql

    prompt = _build_prompt(
        "transform.txt",
        source_db=source_db,
        target_db=target_db,
        object_type=object_type,
        object_name=object_name or "",
        source_sql=source_sql,
        target_rules=_format_rule_block(get_target_rules(source_db, target_db)),
        object_rules=_format_rule_block(get_object_rules(source_db, target_db, object_type)),
        extra_rules=_format_rule_block(_get_extra_rules(source_db, target_db, object_type)),
        rag_context=_format_rag_context(source_db, target_db, object_type),
    )
    transformed = _call_llm(prompt)
    return transformed or source_sql


def review_transformed_sql(
    source_sql: str,
    transformed_sql: str,
    source_db: str,
    target_db: str,
    object_type: str,
    object_name: str | None = None,
) -> str:
    deterministic_sql = transform_deterministically(
        source_sql,
        source_db,
        target_db,
        object_type,
        object_name,
    )
    if deterministic_sql:
        return deterministic_sql

    prompt = _build_prompt(
        "review.txt",
        source_db=source_db,
        target_db=target_db,
        object_type=object_type,
        object_name=object_name or "",
        source_sql=source_sql,
        transformed_sql=transformed_sql,
        target_rules=_format_rule_block(get_target_rules(source_db, target_db)),
        object_rules=_format_rule_block(get_object_rules(source_db, target_db, object_type)),
        extra_rules=_format_rule_block(_get_extra_rules(source_db, target_db, object_type)),
        rag_context=_format_rag_context(source_db, target_db, object_type),
    )
    reviewed = _call_llm(prompt)
    return reviewed or transformed_sql


def generate_repaired_sql(
    source_sql: str,
    failed_sql: str,
    error_message: str,
    source_db: str,
    target_db: str,
    object_type: str,
    object_name: str | None = None,
) -> str:
    deterministic_sql = transform_deterministically(
        source_sql,
        source_db,
        target_db,
        object_type,
        object_name,
    )
    if deterministic_sql:
        return deterministic_sql

    prompt = _build_prompt(
        "repair.txt",
        source_db=source_db,
        target_db=target_db,
        object_type=object_type,
        object_name=object_name or "",
        source_sql=source_sql,
        failed_sql=failed_sql,
        error_message=error_message,
        target_rules=_format_rule_block(get_target_rules(source_db, target_db)),
        object_rules=_format_rule_block(get_object_rules(source_db, target_db, object_type)),
        extra_rules=_format_rule_block(_get_extra_rules(source_db, target_db, object_type)),
        rag_context=_format_rag_context(source_db, target_db, object_type),
    )
    repaired = _call_llm(prompt)
    return repaired or failed_sql


def validate_mysql_trigger_to_snowflake_bundle(text: str) -> bool:
    if not text:
        return False
    if SQL_BUNDLE_DELIMITER in text:
        statements = [
            statement.strip()
            for statement in text.split(SQL_BUNDLE_DELIMITER)
            if statement.strip()
        ]
    else:
        statements = [text.strip()]
    if not statements or not all(statement.endswith(";") for statement in statements):
        return False
    normalized = " ".join(statement.upper() for statement in statements)
    return "STREAM" in normalized and "TASK" in normalized


def build_mysql_trigger_to_snowflake_fallback_sql(
    source_sql: str,
    object_name: str,
    target_connection_details: dict | None = None,
) -> str:
    base_name = _sanitize_identifier(object_name, fallback="trigger_migration")
    table_name = _sanitize_identifier(
        _extract_trigger_table_name(source_sql, base_name),
        fallback=f"{base_name}_table",
    )
    event_name = _extract_trigger_event(source_sql)
    stream_name = f"{base_name}_stream"
    task_name = f"{base_name}_task"
    warehouse = (target_connection_details or {}).get("warehouse")
    warehouse_clause = (
        f"WAREHOUSE = {warehouse} "
        if warehouse and re.fullmatch(r"[A-Za-z0-9_$.]+", str(warehouse))
        else "USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' "
    )
    stream_sql = (
        f"CREATE OR REPLACE STREAM {stream_name} "
        f"ON TABLE {table_name};"
    )
    task_sql = (
        "CREATE OR REPLACE TASK "
        f"{task_name} "
        f"{warehouse_clause}"
        f"WHEN SYSTEM$STREAM_HAS_DATA('{stream_name}') "
        f"AS SELECT '{event_name}' AS trigger_event, CURRENT_TIMESTAMP() AS processed_at FROM {stream_name};"
    )
    return SQL_BUNDLE_DELIMITER.join([stream_sql, task_sql])
