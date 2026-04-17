import json
import threading
from pathlib import Path

from app.services.mysql_job_store import mirror_history_record


_HISTORY_LOCK = threading.Lock()
_HISTORY_DIR = Path(__file__).resolve().parents[2] / "app_data"
_HISTORY_FILE = _HISTORY_DIR / "migration_runs.jsonl"
_SUCCESS_HISTORY_FILE = _HISTORY_DIR / "successful_migration_runs.jsonl"
_SUCCESS_RUNS_DIR = _HISTORY_DIR / "successful_runs"


def _ensure_history_dir():
    _HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    _SUCCESS_RUNS_DIR.mkdir(parents=True, exist_ok=True)


def _is_successful_history_record(record):
    summary = (record or {}).get("run_summary") or {}
    return str(summary.get("status") or "").strip().lower() == "success"


def _archive_successful_history_record(record):
    if not _is_successful_history_record(record):
        return
    summary = (record or {}).get("run_summary") or {}
    run_id = str(summary.get("run_id") or "").strip()
    if not run_id:
        return
    with _SUCCESS_HISTORY_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True) + "\n")
    snapshot_path = _SUCCESS_RUNS_DIR / f"{run_id}_history.json"
    snapshot_path.write_text(
        json.dumps(record, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def append_migration_run(record):
    _ensure_history_dir()
    with _HISTORY_LOCK:
        with _HISTORY_FILE.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")
        _archive_successful_history_record(record)
    mirror_history_record(record)


def list_migration_runs(limit=100):
    _ensure_history_dir()
    if not _HISTORY_FILE.exists():
        return []
    with _HISTORY_LOCK:
        with _HISTORY_FILE.open("r", encoding="utf-8") as handle:
            records = [
                json.loads(line)
                for line in handle
                if line.strip()
            ]
    if limit is not None and limit >= 0:
        records = records[-limit:]
    return list(reversed(records))


def get_migration_run(run_id):
    if not run_id:
        return None
    for record in list_migration_runs(limit=None):
        summary = record.get("run_summary") or {}
        if summary.get("run_id") == run_id:
            return record
    return None
