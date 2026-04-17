import json
import threading
from pathlib import Path


_SCHEDULE_LOCK = threading.Lock()
_DATA_DIR = Path(__file__).resolve().parents[2] / "app_data"
_SCHEDULE_FILE = _DATA_DIR / "job_schedules.json"


def _ensure_data_dir():
    _DATA_DIR.mkdir(parents=True, exist_ok=True)


def _read_schedule_map():
    _ensure_data_dir()
    if not _SCHEDULE_FILE.exists():
        return {}
    with _SCHEDULE_LOCK:
        try:
            with _SCHEDULE_FILE.open("r", encoding="utf-8") as handle:
                return json.load(handle) or {}
        except json.JSONDecodeError:
            return {}


def _write_schedule_map(schedule_map):
    _ensure_data_dir()
    with _SCHEDULE_LOCK:
        with _SCHEDULE_FILE.open("w", encoding="utf-8") as handle:
            json.dump(schedule_map, handle, ensure_ascii=True, indent=2)


def list_job_schedules():
    return _read_schedule_map()


def get_job_schedule(job_id):
    if not job_id:
        return None
    return _read_schedule_map().get(job_id)


def upsert_job_schedule(job_id, schedule_record):
    schedule_map = _read_schedule_map()
    schedule_map[job_id] = schedule_record
    _write_schedule_map(schedule_map)
    return schedule_record


def record_schedule_event(job_id, event_name=None, triggered_at=None):
    schedule_map = _read_schedule_map()
    schedule = schedule_map.get(job_id)
    if not schedule:
        return None
    schedule["last_triggered_at"] = triggered_at
    if event_name:
        schedule["event_name"] = event_name
    schedule["trigger_count"] = int(schedule.get("trigger_count") or 0) + 1
    schedule_map[job_id] = schedule
    _write_schedule_map(schedule_map)
    return schedule


def update_job_schedule(job_id, **fields):
    schedule_map = _read_schedule_map()
    schedule = schedule_map.get(job_id)
    if not schedule:
        return None
    schedule.update(fields)
    schedule_map[job_id] = schedule
    _write_schedule_map(schedule_map)
    return schedule
