import hashlib
import json
import re
import threading
from datetime import datetime
from pathlib import Path


_HISTORY_LOCK = threading.Lock()
_HISTORY_DIR = Path(__file__).resolve().parents[2] / "app_data"
_HISTORY_FILE = _HISTORY_DIR / "sql_migration_history.json"


def _ensure_history_file():
    try:
        _HISTORY_DIR.mkdir(parents=True, exist_ok=True)
        if not _HISTORY_FILE.exists():
            _HISTORY_FILE.write_text("{}", encoding="utf-8")
    except OSError:
        return


def _normalize_text(value: str) -> str:
    return str(value or "").strip().lower()


def _normalize_sql(sql_text: str) -> str:
    text = str(sql_text or "")
    if not text.strip():
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def _extract_namespace(connection_details: dict | None) -> dict[str, str]:
    details = connection_details or {}
    return {
        "database": _normalize_text(details.get("database") or details.get("dbname")),
        "schema": _normalize_text(details.get("schema")),
    }


def _build_history_payload(
    input_sql: str,
    source: str,
    target: str,
    object_type: str = "",
    object_name: str = "",
    source_connection_details: dict | None = None,
    target_connection_details: dict | None = None,
) -> dict:
    source_namespace = _extract_namespace(source_connection_details)
    target_namespace = _extract_namespace(target_connection_details)
    return {
        "input_sql_normalized": _normalize_sql(input_sql),
        "source": _normalize_text(source),
        "target": _normalize_text(target),
        "source_database": source_namespace["database"],
        "source_schema": source_namespace["schema"],
        "target_database": target_namespace["database"],
        "target_schema": target_namespace["schema"],
        "object_type": _normalize_text(object_type),
        "object_name": _normalize_text(object_name),
    }


def _history_key(
    input_sql: str,
    source: str,
    target: str,
    object_type: str = "",
    object_name: str = "",
    source_connection_details: dict | None = None,
    target_connection_details: dict | None = None,
) -> str:
    payload = _build_history_payload(
        input_sql,
        source,
        target,
        object_type,
        object_name,
        source_connection_details=source_connection_details,
        target_connection_details=target_connection_details,
    )
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _read_history() -> dict:
    _ensure_history_file()
    try:
        return json.loads(_HISTORY_FILE.read_text(encoding="utf-8") or "{}")
    except (OSError, json.JSONDecodeError):
        return {}


def _is_success_status(record: dict) -> bool:
    return _normalize_text((record or {}).get("status")) == "success"


def _record_matches_payload(record: dict, payload: dict) -> bool:
    if not isinstance(record, dict) or not _is_success_status(record):
        return False
    required_fields = (
        payload["source"],
        payload["target"],
        payload["source_database"],
        payload["source_schema"],
        payload["target_database"],
        payload["target_schema"],
        payload["object_type"],
        payload["input_sql_normalized"],
    )
    if any(not value for value in required_fields):
        return False
    record_required_fields = (
        _normalize_text(record.get("source")),
        _normalize_text(record.get("target")),
        _normalize_text(record.get("source_database")),
        _normalize_text(record.get("source_schema")),
        _normalize_text(record.get("target_database")),
        _normalize_text(record.get("target_schema")),
        _normalize_text(record.get("object_type")),
        _normalize_sql(record.get("input_sql_normalized") or record.get("input_sql")),
    )
    if any(not value for value in record_required_fields):
        return False
    return (
        _normalize_text(record.get("source")) == payload["source"]
        and _normalize_text(record.get("target")) == payload["target"]
        and _normalize_text(record.get("source_database")) == payload["source_database"]
        and _normalize_text(record.get("source_schema")) == payload["source_schema"]
        and _normalize_text(record.get("target_database")) == payload["target_database"]
        and _normalize_text(record.get("target_schema")) == payload["target_schema"]
        and _normalize_text(record.get("object_type")) == payload["object_type"]
        and _normalize_sql(record.get("input_sql_normalized") or record.get("input_sql")) == payload["input_sql_normalized"]
    )


def get_history_match(
    input_sql: str,
    source: str,
    target: str,
    object_type: str = "",
    object_name: str = "",
    source_connection_details: dict | None = None,
    target_connection_details: dict | None = None,
) -> dict | None:
    payload = _build_history_payload(
        input_sql,
        source,
        target,
        object_type,
        object_name,
        source_connection_details=source_connection_details,
        target_connection_details=target_connection_details,
    )
    key = _history_key(
        input_sql,
        source,
        target,
        object_type,
        object_name,
        source_connection_details=source_connection_details,
        target_connection_details=target_connection_details,
    )
    with _HISTORY_LOCK:
        history = _read_history()
        record = history.get(key)
        if not _record_matches_payload(record, payload):
            record = next(
                (item for item in history.values() if _record_matches_payload(item, payload)),
                None,
            )
    if not record:
        return None
    matched = dict(record)
    matched["history_key"] = str(record.get("history_key") or key)
    return matched


def save_history(
    input_sql: str,
    output_sql: str,
    source: str,
    target: str,
    object_type: str = "",
    object_name: str = "",
    validation: dict | None = None,
    status: str = "SUCCESS",
    error: str | None = None,
    fix_attempts: list[dict] | None = None,
    source_connection_details: dict | None = None,
    target_connection_details: dict | None = None,
) -> dict:
    payload = _build_history_payload(
        input_sql,
        source,
        target,
        object_type,
        object_name,
        source_connection_details=source_connection_details,
        target_connection_details=target_connection_details,
    )
    key = _history_key(
        input_sql,
        source,
        target,
        object_type,
        object_name,
        source_connection_details=source_connection_details,
        target_connection_details=target_connection_details,
    )
    record = {
        "history_key": key,
        "status": str(status or "SUCCESS").upper(),
        "input_sql": str(input_sql or "").strip(),
        "input_sql_normalized": payload["input_sql_normalized"],
        "output_sql": str(output_sql or "").strip(),
        "source": source,
        "target": target,
        "source_database": payload["source_database"],
        "source_schema": payload["source_schema"],
        "target_database": payload["target_database"],
        "target_schema": payload["target_schema"],
        "object_type": object_type,
        "object_name": object_name,
        "validation": validation or {},
        "error": str(error or "").strip(),
        "fix_attempts": fix_attempts or [],
        "updated_at": datetime.utcnow().isoformat(),
    }
    with _HISTORY_LOCK:
        history = _read_history()
        history[key] = record
        try:
            _HISTORY_FILE.write_text(
                json.dumps(history, ensure_ascii=True, indent=2),
                encoding="utf-8",
            )
        except OSError:
            return record
    return record
