import json
import threading
from pathlib import Path

from app.services.mysql_job_store import mirror_diagnostics_record


_DIAGNOSTICS_LOCK = threading.Lock()
_DIAGNOSTICS_DIR = Path(__file__).resolve().parents[2] / "app_data"
_DIAGNOSTICS_FILE = _DIAGNOSTICS_DIR / "migration_diagnostics.jsonl"
_SUCCESS_DIAGNOSTICS_FILE = _DIAGNOSTICS_DIR / "successful_migration_diagnostics.jsonl"
_SUCCESS_RUNS_DIR = _DIAGNOSTICS_DIR / "successful_runs"


def _ensure_diagnostics_dir():
    _DIAGNOSTICS_DIR.mkdir(parents=True, exist_ok=True)
    _SUCCESS_RUNS_DIR.mkdir(parents=True, exist_ok=True)


def _is_successful_diagnostics_record(record):
    objects = (record or {}).get("objects") or []
    if not objects:
        return False
    return all(
        str((item or {}).get("status") or "").strip().lower() == "success"
        for item in objects
    )


def _archive_successful_diagnostics_record(record):
    if not _is_successful_diagnostics_record(record):
        return
    run_id = str((record or {}).get("run_id") or "").strip()
    if not run_id:
        return
    with _SUCCESS_DIAGNOSTICS_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True) + "\n")
    snapshot_path = _SUCCESS_RUNS_DIR / f"{run_id}_diagnostics.json"
    snapshot_path.write_text(
        json.dumps(record, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def append_migration_diagnostics(record):
    _ensure_diagnostics_dir()
    with _DIAGNOSTICS_LOCK:
        with _DIAGNOSTICS_FILE.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")
        _archive_successful_diagnostics_record(record)
    mirror_diagnostics_record(record)


def list_migration_diagnostics(limit=100):
    _ensure_diagnostics_dir()
    if not _DIAGNOSTICS_FILE.exists():
        return []
    with _DIAGNOSTICS_LOCK:
        with _DIAGNOSTICS_FILE.open("r", encoding="utf-8") as handle:
            records = [json.loads(line) for line in handle if line.strip()]
    if limit is not None and limit >= 0:
        records = records[-limit:]
    return list(reversed(records))


def get_migration_diagnostics(run_id):
    if not run_id:
        return None
    for record in list_migration_diagnostics(limit=None):
        if str(record.get("run_id")) == str(run_id):
            return record
    return None
