import json
import os
import threading
from datetime import datetime
from pathlib import Path

try:
    import pymysql
except Exception:  # pragma: no cover
    pymysql = None


_JOB_STORE_LOCK = threading.Lock()
_SCHEMA_READY = False
_JOB_STORE_DIR = Path(__file__).resolve().parents[2] / "app_data"
_JOB_STORE_ERROR_FILE = _JOB_STORE_DIR / "mysql_job_store_errors.log"


def _log_job_store_error(stage, error):
    _JOB_STORE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().isoformat()
    message = f"{timestamp} [{stage}] {type(error).__name__}: {error}\n"
    with _JOB_STORE_ERROR_FILE.open("a", encoding="utf-8") as handle:
        handle.write(message)


def _column_exists(cursor, table_name, column_name):
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table_name, column_name),
    )
    return cursor.fetchone() is not None


def _ensure_column(cursor, table_name, column_name, definition):
    if _column_exists(cursor, table_name, column_name):
        return
    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def _is_enabled():
    if os.getenv("MIGRATION_JOB_STORE_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}:
        return True
    return bool(
        os.getenv("MIGRATION_JOB_STORE_HOST")
        and os.getenv("MIGRATION_JOB_STORE_USER")
        and os.getenv("MIGRATION_JOB_STORE_PASSWORD")
        and os.getenv("MIGRATION_JOB_STORE_DATABASE")
    )


def _connect():
    if not _is_enabled() or pymysql is None:
        return None
    return pymysql.connect(
        host=os.getenv("MIGRATION_JOB_STORE_HOST", "localhost"),
        port=int(os.getenv("MIGRATION_JOB_STORE_PORT", "3306")),
        user=os.getenv("MIGRATION_JOB_STORE_USER", ""),
        password=os.getenv("MIGRATION_JOB_STORE_PASSWORD", ""),
        database=os.getenv("MIGRATION_JOB_STORE_DATABASE", ""),
        charset="utf8mb4",
        autocommit=False,
    )


def _parse_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def initialize_mysql_job_store():
    global _SCHEMA_READY
    if _SCHEMA_READY or not _is_enabled():
        return
    with _JOB_STORE_LOCK:
        if _SCHEMA_READY:
            return
        connection = _connect()
        if connection is None:
            return
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS migration_runs (
                      run_id VARCHAR(64) PRIMARY KEY,
                      status VARCHAR(20) NOT NULL,
                      source_db VARCHAR(50) NOT NULL,
                      target_db VARCHAR(50) NOT NULL,
                      request_payload JSON NULL,
                      run_summary_json JSON NULL,
                      history_record_json JSON NULL,
                      diagnostics_json JSON NULL,
                      current_object_type VARCHAR(50) NULL,
                      current_object_name VARCHAR(255) NULL,
                      resume_checkpoint JSON NULL,
                      error_message TEXT NULL,
                      created_by VARCHAR(100) NULL,
                      started_at DATETIME NULL,
                      completed_at DATETIME NULL,
                      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS migration_run_logs (
                      id BIGINT PRIMARY KEY AUTO_INCREMENT,
                      run_id VARCHAR(64) NOT NULL,
                      sequence_no BIGINT NOT NULL,
                      level VARCHAR(20) NOT NULL,
                      message LONGTEXT NOT NULL,
                      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      KEY idx_logs_run_seq (run_id, sequence_no),
                      CONSTRAINT fk_migration_run_logs_run
                        FOREIGN KEY (run_id) REFERENCES migration_runs(run_id)
                        ON DELETE CASCADE
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS migration_run_objects (
                      id BIGINT PRIMARY KEY AUTO_INCREMENT,
                      run_id VARCHAR(64) NOT NULL,
                      object_type VARCHAR(50) NOT NULL,
                      object_name VARCHAR(255) NOT NULL,
                      status VARCHAR(20) NOT NULL,
                      rows_migrated BIGINT NOT NULL DEFAULT 0,
                      error_type VARCHAR(50) NULL,
                      error_message LONGTEXT NULL,
                      remediation LONGTEXT NULL,
                      started_at DATETIME NULL,
                      completed_at DATETIME NULL,
                      object_result_json JSON NULL,
                      KEY idx_objects_run (run_id),
                      CONSTRAINT fk_migration_run_objects_run
                        FOREIGN KEY (run_id) REFERENCES migration_runs(run_id)
                        ON DELETE CASCADE
                    )
                    """
                )
                cursor.execute("ALTER TABLE migration_runs MODIFY COLUMN request_payload JSON NULL")
                _ensure_column(cursor, "migration_runs", "run_summary_json", "JSON NULL")
                _ensure_column(cursor, "migration_runs", "history_record_json", "JSON NULL")
                _ensure_column(cursor, "migration_runs", "diagnostics_json", "JSON NULL")
                _ensure_column(cursor, "migration_runs", "updated_at", "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")
                _ensure_column(cursor, "migration_run_objects", "remediation", "LONGTEXT NULL")
                _ensure_column(cursor, "migration_run_objects", "object_result_json", "JSON NULL")
                cursor.execute("ALTER TABLE migration_run_logs MODIFY COLUMN message LONGTEXT NOT NULL")
                cursor.execute("ALTER TABLE migration_run_objects MODIFY COLUMN error_message LONGTEXT NULL")
            connection.commit()
            _SCHEMA_READY = True
        except Exception as error:
            try:
                connection.rollback()
            except Exception:
                pass
            _log_job_store_error("initialize_mysql_job_store", error)
        finally:
            connection.close()


def _infer_log_level(message):
    text = str(message or "").lower()
    if "error" in text:
        return "error"
    if "skipped" in text:
        return "warn"
    if "success" in text:
        return "success"
    return "info"


def mirror_history_record(record):
    if not _is_enabled():
        return
    initialize_mysql_job_store()
    connection = _connect()
    if connection is None:
        return
    try:
        run_summary = record.get("run_summary") or {}
        run_id = str(run_summary.get("run_id") or "").strip()
        if not run_id:
            return
        logs = list(record.get("logs") or [])
        object_results = list(run_summary.get("object_results") or [])
        resume_checkpoint = record.get("summary", {}).get("resume_checkpoint")
        error_message = next(
            (item.get("error_message") for item in object_results if item.get("status") == "error" and item.get("error_message")),
            None,
        )
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO migration_runs (
                  run_id, status, source_db, target_db, run_summary_json, history_record_json,
                  current_object_type, current_object_name, resume_checkpoint, error_message,
                  started_at, completed_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  status = VALUES(status),
                  source_db = VALUES(source_db),
                  target_db = VALUES(target_db),
                  run_summary_json = VALUES(run_summary_json),
                  history_record_json = VALUES(history_record_json),
                  current_object_type = VALUES(current_object_type),
                  current_object_name = VALUES(current_object_name),
                  resume_checkpoint = VALUES(resume_checkpoint),
                  error_message = VALUES(error_message),
                  started_at = VALUES(started_at),
                  completed_at = VALUES(completed_at)
                """,
                (
                    run_id,
                    run_summary.get("status", "unknown"),
                    run_summary.get("source_db", ""),
                    run_summary.get("target_db", ""),
                    json.dumps(run_summary, ensure_ascii=True),
                    json.dumps(record, ensure_ascii=True),
                    (resume_checkpoint or {}).get("object_type") if isinstance(resume_checkpoint, dict) else None,
                    (resume_checkpoint or {}).get("object_name") if isinstance(resume_checkpoint, dict) else None,
                    json.dumps(resume_checkpoint, ensure_ascii=True) if resume_checkpoint else None,
                    error_message,
                    _parse_datetime(run_summary.get("started_at")),
                    _parse_datetime(run_summary.get("completed_at")),
                ),
            )
            cursor.execute("DELETE FROM migration_run_logs WHERE run_id = %s", (run_id,))
            for index, message in enumerate(logs, start=1):
                cursor.execute(
                    """
                    INSERT INTO migration_run_logs (run_id, sequence_no, level, message)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (run_id, index, _infer_log_level(message), str(message)),
                )
            cursor.execute("DELETE FROM migration_run_objects WHERE run_id = %s", (run_id,))
            for item in object_results:
                cursor.execute(
                    """
                    INSERT INTO migration_run_objects (
                      run_id, object_type, object_name, status, rows_migrated,
                      error_type, error_message, remediation, started_at, completed_at, object_result_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        item.get("object_type", ""),
                        item.get("object_name", ""),
                        item.get("status", ""),
                        int(item.get("rows_migrated") or 0),
                        item.get("error_type"),
                        item.get("error_message"),
                        item.get("remediation"),
                        _parse_datetime(item.get("started_at")),
                        _parse_datetime(item.get("completed_at")),
                        json.dumps(item, ensure_ascii=True),
                    ),
                )
        connection.commit()
    except Exception as error:
        try:
            connection.rollback()
        except Exception:
            pass
        _log_job_store_error("mirror_history_record", error)
    finally:
        connection.close()


def mirror_diagnostics_record(record):
    if not _is_enabled():
        return
    initialize_mysql_job_store()
    connection = _connect()
    if connection is None:
        return
    try:
        run_id = str(record.get("run_id") or "").strip()
        if not run_id:
            return
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO migration_runs (
                  run_id, status, source_db, target_db, diagnostics_json, started_at, completed_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  source_db = VALUES(source_db),
                  target_db = VALUES(target_db),
                  diagnostics_json = VALUES(diagnostics_json),
                  started_at = COALESCE(VALUES(started_at), started_at),
                  completed_at = COALESCE(VALUES(completed_at), completed_at)
                """,
                (
                    run_id,
                    "diagnostics",
                    record.get("source_db", ""),
                    record.get("target_db", ""),
                    json.dumps(record, ensure_ascii=True),
                    _parse_datetime(record.get("started_at")),
                    _parse_datetime(record.get("completed_at")),
                ),
            )
        connection.commit()
    except Exception as error:
        try:
            connection.rollback()
        except Exception:
            pass
        _log_job_store_error("mirror_diagnostics_record", error)
    finally:
        connection.close()


__all__ = [
    "initialize_mysql_job_store",
    "mirror_diagnostics_record",
    "mirror_history_record",
]
