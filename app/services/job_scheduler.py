from __future__ import annotations

from datetime import datetime, timedelta
import threading
from zoneinfo import ZoneInfo

from app.services.job_schedule import list_job_schedules, update_job_schedule


_SCHEDULER_STOP = threading.Event()
_SCHEDULER_THREAD = None
_ACTIVE_JOBS = set()
_ACTIVE_LOCK = threading.Lock()


def _expand_token(token, minimum, maximum):
    token = str(token).strip()
    if token in {"*", "?"}:
        return set(range(minimum, maximum + 1))
    values = set()
    for part in token.split(","):
        part = part.strip()
        if not part:
            continue
        if "/" in part:
            base, step_text = part.split("/", 1)
            step = max(1, int(step_text))
            if base in {"*", "?"}:
                start, end = minimum, maximum
            elif "-" in base:
                start_text, end_text = base.split("-", 1)
                start, end = int(start_text), int(end_text)
            else:
                start, end = int(base), maximum
            values.update(range(max(minimum, start), min(maximum, end) + 1, step))
        elif "-" in part:
            start_text, end_text = part.split("-", 1)
            values.update(range(max(minimum, int(start_text)), min(maximum, int(end_text)) + 1))
        else:
            values.add(int(part))
    return {value for value in values if minimum <= value <= maximum}


def _cron_matches(expression, dt_local):
    parts = [part.strip() for part in str(expression or "").split() if part.strip()]
    if len(parts) == 6:
        _, minute_expr, hour_expr, day_expr, month_expr, weekday_expr = parts
    elif len(parts) == 5:
        minute_expr, hour_expr, day_expr, month_expr, weekday_expr = parts
    else:
        return False
    minute_values = _expand_token(minute_expr, 0, 59)
    hour_values = _expand_token(hour_expr, 0, 23)
    day_values = _expand_token(day_expr, 1, 31)
    month_values = _expand_token(month_expr, 1, 12)
    weekday_values = _expand_token(str(weekday_expr).replace("7", "0"), 0, 6)
    current_weekday = (dt_local.weekday() + 1) % 7
    day_wildcard = str(day_expr) in {"*", "?"}
    weekday_wildcard = str(weekday_expr) in {"*", "?"}
    day_match = dt_local.day in day_values
    weekday_match = current_weekday in weekday_values
    if day_wildcard and weekday_wildcard:
        calendar_match = True
    elif day_wildcard:
        calendar_match = weekday_match
    elif weekday_wildcard:
        calendar_match = day_match
    else:
        calendar_match = day_match or weekday_match
    return (
        dt_local.minute in minute_values
        and dt_local.hour in hour_values
        and dt_local.month in month_values
        and calendar_match
    )


def _compute_next_run(expression, timezone_name, from_utc=None):
    try:
        tz = ZoneInfo(timezone_name or "UTC")
    except Exception:
        tz = ZoneInfo("UTC")
    base_utc = from_utc or datetime.utcnow().replace(second=0, microsecond=0)
    local_probe = base_utc.astimezone(tz).replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(60 * 24 * 31):
        if _cron_matches(expression, local_probe):
            return local_probe.astimezone(ZoneInfo("UTC")).isoformat()
        local_probe += timedelta(minutes=1)
    return None


def _run_job_async(job_id, runner, trigger_type, event_name=None):
    with _ACTIVE_LOCK:
        if job_id in _ACTIVE_JOBS:
            return
        _ACTIVE_JOBS.add(job_id)

    def execute():
        try:
            result = runner(job_id, trigger_type=trigger_type, event_name=event_name)
            run_summary = (result or {}).get("run_summary") or {}
            schedule = list_job_schedules().get(job_id) or {}
            schedule_fields = {
                "last_triggered_at": datetime.utcnow().isoformat(),
                "last_run_id": run_summary.get("run_id"),
                "last_run_status": (result or {}).get("status"),
                "updated_at": datetime.utcnow().isoformat(),
                "trigger_count": int(schedule.get("trigger_count") or 0) + (1 if trigger_type == "scheduled_trigger" else 0),
            }
            if schedule.get("trigger_type") == "scheduled_trigger":
                schedule_fields["next_run_at"] = _compute_next_run(
                    schedule.get("cron_expression"),
                    schedule.get("timezone") or "UTC",
                    datetime.utcnow(),
                )
            update_job_schedule(job_id, **schedule_fields)
        finally:
            with _ACTIVE_LOCK:
                _ACTIVE_JOBS.discard(job_id)

    threading.Thread(target=execute, daemon=True).start()


def _scheduler_loop(runner, interval_seconds):
    while not _SCHEDULER_STOP.wait(interval_seconds):
        now_utc = datetime.utcnow().replace(second=0, microsecond=0)
        schedules = list_job_schedules()
        for job_id, schedule in schedules.items():
            if not schedule or not schedule.get("enabled"):
                continue
            if schedule.get("trigger_type") != "scheduled_trigger":
                continue
            cron_expression = schedule.get("cron_expression")
            if not cron_expression:
                continue
            try:
                timezone_name = schedule.get("timezone") or "UTC"
                tz = ZoneInfo(timezone_name)
            except Exception:
                timezone_name = "UTC"
                tz = ZoneInfo("UTC")
            local_now = now_utc.astimezone(tz)
            slot_key = local_now.strftime("%Y-%m-%dT%H:%M")
            if schedule.get("last_scheduled_slot") == slot_key:
                continue
            start_at = schedule.get("start_at")
            if start_at:
                try:
                    start_local = datetime.fromisoformat(str(start_at))
                    if start_local.tzinfo is None:
                        start_local = start_local.replace(tzinfo=tz)
                    if local_now < start_local.astimezone(tz):
                        update_job_schedule(
                            job_id,
                            next_run_at=_compute_next_run(cron_expression, timezone_name, now_utc),
                        )
                        continue
                except ValueError:
                    pass
            if _cron_matches(cron_expression, local_now):
                update_job_schedule(
                    job_id,
                    last_scheduled_slot=slot_key,
                    next_run_at=_compute_next_run(cron_expression, timezone_name, now_utc),
                    updated_at=datetime.utcnow().isoformat(),
                )
                _run_job_async(job_id, runner, trigger_type="scheduled_trigger")
            elif not schedule.get("next_run_at"):
                update_job_schedule(
                    job_id,
                    next_run_at=_compute_next_run(cron_expression, timezone_name, now_utc),
                    updated_at=datetime.utcnow().isoformat(),
                )


def start_scheduler(runner, interval_seconds=30):
    global _SCHEDULER_THREAD
    if _SCHEDULER_THREAD and _SCHEDULER_THREAD.is_alive():
        return
    _SCHEDULER_STOP.clear()
    _SCHEDULER_THREAD = threading.Thread(
        target=_scheduler_loop,
        args=(runner, interval_seconds),
        daemon=True,
        name="dbm-job-scheduler",
    )
    _SCHEDULER_THREAD.start()


def stop_scheduler():
    _SCHEDULER_STOP.set()


def trigger_job_run(job_id, runner, trigger_type, event_name=None):
    _run_job_async(job_id, runner, trigger_type=trigger_type, event_name=event_name)
