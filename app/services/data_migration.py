import csv
from datetime import date, datetime
from decimal import Decimal
import os
from pathlib import Path
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil

from app.adapters.registry import get_adapter
from app.utils.rule_loader import normalize_identifier_with_rules

DEFAULT_DATA_BATCH_SIZE = 10000
SNOWFLAKE_NULL_SENTINEL = "__DBM_NULL__"
SNOWFLAKE_PUT_PARALLEL = 8


def _is_all_null_row(row):
    for value in row:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return False
    return True


def _filter_rows(rows):
    return [row for row in rows if not _is_all_null_row(row)]


def _normalize_batch_size(batch_size):
    if not isinstance(batch_size, int) or batch_size <= 0:
        return DEFAULT_DATA_BATCH_SIZE
    return batch_size


def _iter_source_batches(
    source_cursor,
    source_adapter,
    table_name,
    source_connection_details,
    batch_size,
):
    quoted_source_table = source_adapter.qualify_table_name(
        table_name, source_connection_details
    )
    source_cursor.execute(f"SELECT * FROM {quoted_source_table}")
    columns = [column[0] for column in source_cursor.description or []]
    if not columns:
        return columns

    while True:
        rows = source_cursor.fetchmany(batch_size)
        if not rows:
            break
        filtered_rows = _filter_rows(rows)
        if filtered_rows:
            yield columns, filtered_rows


def _quoted_columns(target_adapter, columns):
    return ", ".join(target_adapter.quote_identifier(column) for column in columns)


def _normalize_identifier(identifier):
    return normalize_identifier_with_rules(identifier)


def _preserve_target_column_identifier(target_db_type, column_name):
    if target_db_type == "Snowflake":
        text = str(column_name or "")
        if any(character.islower() for character in text):
            escaped = text.replace('"', '""')
            return f'"{escaped}"'
    return column_name


def _get_target_columns(target_cursor, target_adapter, table_name, target_connection_details):
    quoted_target_table = target_adapter.qualify_table_name(
        table_name, target_connection_details
    )
    target_cursor.execute(f"SELECT * FROM {quoted_target_table} WHERE 1 = 0")
    return [column[0] for column in target_cursor.description or []]


def _get_target_column_types(
    target_cursor,
    target_adapter,
    table_name,
    target_connection_details,
):
    quoted_target_table = target_adapter.qualify_table_name(
        table_name, target_connection_details
    )
    target_cursor.execute(f"SELECT * FROM {quoted_target_table} WHERE 1 = 0")
    column_types = {}
    for column in target_cursor.description or []:
        column_name = str(column[0]).lower()
        type_descriptor = column[1] if len(column) > 1 else None
        column_types[column_name] = type_descriptor
    return column_types


def _infer_snowflake_column_type(values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int) and not isinstance(value, bool):
            return "NUMBER"
        if isinstance(value, float):
            return "FLOAT"
        if isinstance(value, Decimal):
            return "NUMBER"
        if isinstance(value, datetime):
            return "TIMESTAMP_NTZ"
        if isinstance(value, date):
            return "DATE"
        if isinstance(value, (bytes, bytearray)):
            return "BINARY"
        return "VARCHAR"
    return "VARCHAR"


def _auto_add_missing_snowflake_columns(
    target_cursor,
    target_adapter,
    table_name,
    missing_columns,
    source_columns,
    sample_rows,
    target_connection_details,
    log_callback=None,
):
    qualified_target_table = target_adapter.qualify_table_name(
        table_name, target_connection_details
    )
    for column in missing_columns:
        index = source_columns.index(column)
        inferred_type = _infer_snowflake_column_type(
            [row[index] for row in sample_rows]
        )
        target_cursor.execute(
            f"ALTER TABLE {qualified_target_table} "
            f"ADD COLUMN IF NOT EXISTS {target_adapter.quote_identifier(column)} {inferred_type}"
        )
        if log_callback:
            log_callback(
                f"Auto-added missing Snowflake target column {column} as {inferred_type} "
                f"for {table_name}."
            )


def _build_insert_sql(target_adapter, table_name, columns, target_connection_details):
    quoted_target_table = target_adapter.qualify_table_name(
        table_name, target_connection_details
    )
    placeholders = ", ".join(target_adapter.placeholder() for _ in columns)
    return (
        f"INSERT INTO {quoted_target_table} ({_quoted_columns(target_adapter, columns)}) "
        f"VALUES ({placeholders})"
    )


def _stringify_snowflake_csv_value(value):
    if value is None:
        return SNOWFLAKE_NULL_SENTINEL
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return str(value)


def _stringify_mysql_csv_value(value):
    if value is None:
        return r"\N"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return str(value)


def _write_snowflake_batch_file(rows):
    temp_dir = Path(tempfile.gettempdir()) / "dbm_snowflake_stage"
    temp_dir.mkdir(parents=True, exist_ok=True)
    file_path = temp_dir / f"{uuid.uuid4().hex}.csv"
    with file_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["c"] * len(rows[0]))
        for row in rows:
            writer.writerow([_stringify_snowflake_csv_value(value) for value in row])
    return file_path


class _SnowflakeStagedFileWriter:
    def __init__(self, column_count):
        temp_dir = Path(tempfile.gettempdir()) / "dbm_snowflake_stage"
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.file_path = temp_dir / f"{uuid.uuid4().hex}.csv"
        self._handle = self.file_path.open("w", encoding="utf-8", newline="")
        self._writer = csv.writer(self._handle, quoting=csv.QUOTE_MINIMAL)
        self._writer.writerow(["c"] * column_count)

    def write_rows(self, rows):
        for row in rows:
            self._writer.writerow(
                [_stringify_snowflake_csv_value(value) for value in row]
            )

    def close(self):
        if not self._handle.closed:
            self._handle.close()


def _write_csv_file(columns, rows, file_path, include_header=True):
    with file_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_MINIMAL)
        if include_header:
            writer.writerow(["c"] * len(columns))
        for row in rows:
            writer.writerow([_stringify_snowflake_csv_value(value) for value in row])


class _MySQLCsvStagedFileWriter:
    def __init__(self, column_count):
        temp_dir = Path(tempfile.gettempdir()) / "dbm_mysql_stage"
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.file_path = temp_dir / f"{uuid.uuid4().hex}.csv"
        self._handle = self.file_path.open("w", encoding="utf-8", newline="")
        self._writer = csv.writer(
            self._handle,
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )

    def write_rows(self, rows):
        for row in rows:
            self._writer.writerow(
                [_stringify_mysql_csv_value(value) for value in row]
            )

    def close(self):
        if not self._handle.closed:
            self._handle.close()


def _cleanup_temp_file(file_path):
    try:
        os.remove(file_path)
    except OSError:
        pass


def _create_snowflake_temp_stage(target_cursor):
    stage_name = f"DBM_STAGE_{uuid.uuid4().hex.upper()}"
    target_cursor.execute(
        "CREATE TEMP STAGE "
        f"{stage_name} "
        "FILE_FORMAT = ("
        "TYPE = CSV "
        "FIELD_OPTIONALLY_ENCLOSED_BY = '\"' "
        "SKIP_HEADER = 1 "
        f"NULL_IF = ('{SNOWFLAKE_NULL_SENTINEL}') "
        "EMPTY_FIELD_AS_NULL = FALSE"
        ")"
    )
    return stage_name


def _build_snowflake_copy_sql(
    target_adapter,
    table_name,
    columns,
    target_connection_details,
    stage_name,
    staged_filename=None,
):
    quoted_target_table = target_adapter.qualify_table_name(
        table_name, target_connection_details
    )
    quoted_columns = _quoted_columns(target_adapter, columns)
    select_list = ", ".join(f"${index}" for index in range(1, len(columns) + 1))
    if staged_filename:
        from_clause = f"FROM (SELECT {select_list} FROM @{stage_name}/{staged_filename})"
    else:
        from_clause = f"FROM (SELECT {select_list} FROM @{stage_name})"
    return (
        f"COPY INTO {quoted_target_table} ({quoted_columns}) "
        f"{from_clause} "
        "FILE_FORMAT = ("
        "TYPE = CSV "
        "FIELD_OPTIONALLY_ENCLOSED_BY = '\"' "
        "SKIP_HEADER = 1 "
        f"NULL_IF = ('{SNOWFLAKE_NULL_SENTINEL}') "
        "EMPTY_FIELD_AS_NULL = FALSE"
        ") "
        "PURGE = TRUE "
        "ON_ERROR = 'ABORT_STATEMENT'"
    )


def _bulk_load_to_snowflake(
    target_cursor,
    target_adapter,
    table_name,
    columns,
    rows,
    target_connection_details,
    stage_name,
    log_callback=None,
):
    file_path = _write_snowflake_batch_file(rows)
    try:
        normalized_path = file_path.resolve().as_posix()
        target_cursor.execute(
            f"PUT 'file://{normalized_path}' @{stage_name} AUTO_COMPRESS = TRUE OVERWRITE = TRUE"
        )
        staged_filename = f"{file_path.name}.gz"
        target_cursor.execute(
            _build_snowflake_copy_sql(
                target_adapter,
                table_name,
                columns,
                target_connection_details,
                stage_name,
                staged_filename,
            )
        )
        if log_callback:
            log_callback(f"Snowflake staged load completed for batch of {len(rows)} row(s).")
        return len(rows)
    finally:
        _cleanup_temp_file(file_path)


def _finalize_snowflake_staged_load(
    target_cursor,
    target_adapter,
    table_name,
    columns,
    target_connection_details,
    stage_name,
    file_path,
    row_count,
    log_callback=None,
):
    normalized_path = file_path.resolve().as_posix()
    if log_callback:
        log_callback(
            f"Starting Snowflake staged load for {table_name} using {file_path.name}."
        )
    target_cursor.execute(
        f"PUT 'file://{normalized_path}' @{stage_name} "
        f"AUTO_COMPRESS = TRUE OVERWRITE = TRUE PARALLEL = {SNOWFLAKE_PUT_PARALLEL}"
    )
    staged_filename = f"{file_path.name}.gz"
    target_cursor.execute(
        _build_snowflake_copy_sql(
            target_adapter,
            table_name,
            columns,
            target_connection_details,
            stage_name,
            staged_filename,
        )
    )
    if log_callback:
        log_callback(
            f"Snowflake staged load completed for {row_count} row(s) using a single table file."
        )


def load_local_csv_files_to_snowflake(
    target_cursor,
    target_adapter,
    table_name,
    target_connection_details,
    local_path,
    log_callback=None,
):
    resolved_path = Path(local_path).resolve()
    if not resolved_path.exists():
        raise Exception(f"Spark output path not found: {resolved_path}")

    stage_name = _create_snowflake_temp_stage(target_cursor)
    if resolved_path.is_dir():
        csv_files = sorted(resolved_path.rglob("*.csv"))
        if not csv_files:
            raise Exception(f"No CSV files found in Spark output path: {resolved_path}")
        put_source = f"{resolved_path.as_posix()}/*.csv"
        staged_filename = None
    else:
        put_source = resolved_path.as_posix()
        staged_filename = resolved_path.name

    if log_callback:
        log_callback(
            f"Starting Snowflake COPY INTO from CSV source {resolved_path} using stage {stage_name}."
        )
    target_cursor.execute(
        f"PUT 'file://{put_source}' @{stage_name} "
        f"AUTO_COMPRESS = TRUE OVERWRITE = TRUE PARALLEL = {SNOWFLAKE_PUT_PARALLEL}"
    )
    if staged_filename:
        columns = _get_target_columns(
            target_cursor,
            target_adapter,
            table_name,
            target_connection_details,
        )
        target_cursor.execute(
            _build_snowflake_copy_sql(
                target_adapter,
                table_name,
                columns,
                target_connection_details,
                stage_name,
                staged_filename,
            )
        )
    else:
        columns = _get_target_columns(
            target_cursor,
            target_adapter,
            table_name,
            target_connection_details,
        )
        target_cursor.execute(
            _build_snowflake_copy_sql(
                target_adapter,
                table_name,
                columns,
                target_connection_details,
                stage_name,
                None,
            )
        )
    if log_callback:
        log_callback(
            f"Snowflake staged load completed from CSV output: {resolved_path}"
        )


def _build_mysql_load_data_sql(
    target_adapter,
    table_name,
    columns,
    target_connection_details,
    local_path_placeholder="%s",
):
    quoted_target_table = target_adapter.qualify_table_name(
        table_name, target_connection_details
    )
    quoted_columns = ", ".join(target_adapter.quote_identifier(column) for column in columns)
    return (
        f"LOAD DATA LOCAL INFILE {local_path_placeholder} "
        f"INTO TABLE {quoted_target_table} "
        "CHARACTER SET utf8mb4 "
        "FIELDS TERMINATED BY ',' "
        "OPTIONALLY ENCLOSED BY '\"' "
        "ESCAPED BY '\\\\' "
        "LINES TERMINATED BY '\\n' "
        f"({quoted_columns})"
    )


def load_local_csv_to_mysql(
    target_cursor,
    target_adapter,
    table_name,
    columns,
    target_connection_details,
    local_path,
    log_callback=None,
):
    resolved_path = Path(local_path).resolve()
    if not resolved_path.exists():
        raise Exception(f"MySQL bulk load file not found: {resolved_path}")
    sql_text = _build_mysql_load_data_sql(
        target_adapter,
        table_name,
        columns,
        target_connection_details,
    )
    if log_callback:
        log_callback(
            f"Starting MySQL LOAD DATA LOCAL INFILE for {table_name} from {resolved_path}."
        )
    target_cursor.execute(sql_text, (str(resolved_path),))
    if log_callback:
        log_callback(f"MySQL bulk load completed from CSV output: {resolved_path}")


def _build_mysql_upsert_sql(
    target_adapter, table_name, columns, primary_keys, target_connection_details
):
    quoted_target_table = target_adapter.qualify_table_name(
        table_name, target_connection_details
    )
    placeholders = ", ".join(target_adapter.placeholder() for _ in columns)
    quoted_cols = _quoted_columns(target_adapter, columns)
    update_columns = [column for column in columns if column not in primary_keys]
    if not update_columns:
        update_clause = ", ".join(
            f"{target_adapter.quote_identifier(pk)} = VALUES({target_adapter.quote_identifier(pk)})"
            for pk in primary_keys
        )
    else:
        update_clause = ", ".join(
            f"{target_adapter.quote_identifier(column)} = VALUES({target_adapter.quote_identifier(column)})"
            for column in update_columns
        )
    return (
        f"INSERT INTO {quoted_target_table} ({quoted_cols}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )


def _build_postgres_upsert_sql(
    target_adapter, table_name, columns, primary_keys, target_connection_details
):
    quoted_target_table = target_adapter.qualify_table_name(
        table_name, target_connection_details
    )
    placeholders = ", ".join(target_adapter.placeholder() for _ in columns)
    quoted_cols = _quoted_columns(target_adapter, columns)
    conflict_cols = ", ".join(target_adapter.quote_identifier(pk) for pk in primary_keys)
    update_columns = [column for column in columns if column not in primary_keys]
    if not update_columns:
        return (
            f"INSERT INTO {quoted_target_table} ({quoted_cols}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_cols}) DO NOTHING"
        )
    update_clause = ", ".join(
        f"{target_adapter.quote_identifier(column)} = EXCLUDED.{target_adapter.quote_identifier(column)}"
        for column in update_columns
    )
    return (
        f"INSERT INTO {quoted_target_table} ({quoted_cols}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}"
    )


def _build_skip_existing_insert_sql(
    target_db_type, target_adapter, table_name, columns, primary_keys, target_connection_details
):
    if target_db_type == "MySQL":
        return _build_insert_sql(target_adapter, table_name, columns, target_connection_details).replace(
            "INSERT INTO", "INSERT IGNORE INTO", 1
        )
    if target_db_type == "PostgreSQL":
        base_sql = _build_insert_sql(target_adapter, table_name, columns, target_connection_details)
        conflict_cols = ", ".join(target_adapter.quote_identifier(pk) for pk in primary_keys)
        return f"{base_sql} ON CONFLICT ({conflict_cols}) DO NOTHING"
    if target_db_type == "Snowflake":
        return _build_merge_sql(
            target_db_type,
            target_adapter,
            table_name,
            columns,
            primary_keys,
            target_connection_details,
            update_existing=False,
        )
    if target_db_type == "SQL Server":
        return _build_merge_sql(
            target_db_type,
            target_adapter,
            table_name,
            columns,
            primary_keys,
            target_connection_details,
            update_existing=False,
        )
    raise Exception(f"Skip existing mode is not implemented for {target_db_type}.")


def _build_merge_sql(
    target_db_type,
    target_adapter,
    table_name,
    columns,
    primary_keys,
    target_connection_details,
    update_existing,
):
    quoted_target_table = target_adapter.qualify_table_name(
        table_name, target_connection_details
    )
    values_alias_columns = ", ".join(target_adapter.quote_identifier(column) for column in columns)
    placeholder_groups = []
    for _ in columns:
        placeholder_groups.append(target_adapter.placeholder())
    source_values = ", ".join(placeholder_groups)
    source_alias = "src"
    target_alias = "tgt"
    join_condition = " AND ".join(
        f"{target_alias}.{target_adapter.quote_identifier(pk)} = {source_alias}.{target_adapter.quote_identifier(pk)}"
        for pk in primary_keys
    )
    update_columns = [column for column in columns if column not in primary_keys]
    update_clause = ""
    if update_existing and update_columns:
        assignments = ", ".join(
            f"{target_alias}.{target_adapter.quote_identifier(column)} = {source_alias}.{target_adapter.quote_identifier(column)}"
            for column in update_columns
        )
        update_clause = f" WHEN MATCHED THEN UPDATE SET {assignments}"
    insert_columns = _quoted_columns(target_adapter, columns)
    insert_values = ", ".join(
        f"{source_alias}.{target_adapter.quote_identifier(column)}" for column in columns
    )
    if target_db_type == "Snowflake":
        return (
            f"MERGE INTO {quoted_target_table} {target_alias} "
            f"USING (SELECT {source_values}) AS {source_alias} ({values_alias_columns}) "
            f"ON {join_condition}"
            f"{update_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
        )
    return (
        f"MERGE INTO {quoted_target_table} AS {target_alias} "
        f"USING (VALUES ({source_values})) AS {source_alias} ({values_alias_columns}) "
        f"ON {join_condition}"
        f"{update_clause} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values});"
    )


def _execute_row_by_row(target_cursor, sql_text, rows):
    affected_rows = 0
    for row in rows:
        target_cursor.execute(sql_text, row)
        affected_rows += 1
    return affected_rows


def _enable_fast_batch_mode(target_cursor, target_db_type):
    if target_db_type != "SQL Server":
        return
    if hasattr(target_cursor, "fast_executemany"):
        try:
            target_cursor.fast_executemany = True
        except Exception:
            pass


def _build_batch_executor(
    target_cursor,
    target_adapter,
    table_name,
    columns,
    target_db_type,
    target_connection_details,
    migration_mode,
):
    if migration_mode == "insert":
        sql_text = _build_insert_sql(
            target_adapter, table_name, columns, target_connection_details
        )

        def execute_batch(rows):
            target_cursor.executemany(sql_text, rows)
            return len(rows)

        return execute_batch

    if target_db_type == "Snowflake":
        raise Exception(
            "Snowflake data migration supports insert mode only. Use staged COPY INTO for bulk loads."
        )

    primary_keys = target_adapter.get_primary_keys(
        target_cursor,
        table_name,
        target_connection_details,
        (target_connection_details or {}).get("schema"),
    )
    if not primary_keys:
        raise Exception(
            f"Primary key not found for target table {table_name}. "
            f"{migration_mode} mode requires a primary key."
        )

    if migration_mode == "upsert":
        if target_db_type == "MySQL":
            sql_text = _build_mysql_upsert_sql(
                target_adapter, table_name, columns, primary_keys, target_connection_details
            )

            def execute_batch(rows):
                target_cursor.executemany(sql_text, rows)
                return len(rows)

            return execute_batch
        if target_db_type == "PostgreSQL":
            sql_text = _build_postgres_upsert_sql(
                target_adapter, table_name, columns, primary_keys, target_connection_details
            )

            def execute_batch(rows):
                target_cursor.executemany(sql_text, rows)
                return len(rows)

            return execute_batch
        if target_db_type in {"SQL Server", "Snowflake"}:
            sql_text = _build_merge_sql(
                target_db_type,
                target_adapter,
                table_name,
                columns,
                primary_keys,
                target_connection_details,
                update_existing=True,
            )

            def execute_batch(rows):
                return _execute_row_by_row(target_cursor, sql_text, rows)

            return execute_batch
        raise Exception(f"Upsert mode is not implemented for {target_db_type}.")

    if migration_mode == "skip_existing":
        sql_text = _build_skip_existing_insert_sql(
            target_db_type,
            target_adapter,
            table_name,
            columns,
            primary_keys,
            target_connection_details,
        )
        if target_db_type in {"SQL Server", "Snowflake"}:

            def execute_batch(rows):
                return _execute_row_by_row(target_cursor, sql_text, rows)

            return execute_batch

        def execute_batch(rows):
            target_cursor.executemany(sql_text, rows)
            return len(rows)

        return execute_batch

    raise Exception(f"Unsupported data migration mode: {migration_mode}")


def _count_rows(cursor, adapter, table_name, connection_details):
    qualified_table = adapter.qualify_table_name(table_name, connection_details)
    cursor.execute(f"SELECT COUNT(*) FROM {qualified_table}")
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _source_column_type(cursor, source_db_type, table_name, column_name, connection_details):
    base_table_name = table_name.split(".")[-1]
    details = connection_details or {}
    normalized = str(source_db_type or "").lower()
    if normalized == "mysql":
        cursor.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            """,
            (details.get("database"), base_table_name, column_name),
        )
    elif normalized == "postgresql":
        cursor.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            """,
            (details.get("schema") or "public", base_table_name, column_name),
        )
    elif normalized == "sql server":
        cursor.execute(
            """
            SELECT DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = ?
              AND COLUMN_NAME = ?
            """,
            (
                details.get("schema") or "dbo",
                base_table_name,
                column_name,
            ),
        )
    else:
        return None
    row = cursor.fetchone()
    return str(row[0]).lower() if row and row[0] else None


def _list_source_columns(cursor, source_db_type, table_name, connection_details):
    details = connection_details or {}
    base_table_name = table_name.split(".")[-1]
    normalized = str(source_db_type or "").lower()
    if normalized == "mysql":
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (details.get("database"), base_table_name),
        )
    elif normalized == "postgresql":
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (details.get("schema") or "public", base_table_name),
        )
    elif normalized == "sql server":
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """,
            (
                details.get("schema") or "dbo",
                base_table_name,
            ),
        )
    else:
        return []
    return [row[0] for row in cursor.fetchall()]


def _find_numeric_fallback_column(
    source_cursor,
    source_db_type,
    table_name,
    source_connection_details,
    excluded_columns=None,
):
    excluded = {str(item).lower() for item in (excluded_columns or [])}
    for candidate in _list_source_columns(
        source_cursor,
        source_db_type,
        table_name,
        source_connection_details,
    ):
        candidate_lower = str(candidate).lower()
        if candidate_lower in excluded:
            continue
        candidate_type = _source_column_type(
            source_cursor,
            source_db_type,
            table_name,
            candidate,
            source_connection_details,
        )
        if _is_spark_partition_type(candidate_type):
            return candidate
    return None


def _is_spark_partition_type(type_name):
    text = str(type_name or "").lower()
    return text in {
        "bigint",
        "int",
        "integer",
        "mediumint",
        "smallint",
        "tinyint",
        "decimal",
        "numeric",
        "number",
        "date",
        "datetime",
        "timestamp",
        "timestamp without time zone",
        "timestamp with time zone",
    }


def _detect_spark_partitioning(
    source_cursor,
    source_adapter,
    source_db_type,
    table_name,
    source_connection_details,
    spark_options=None,
):
    options = dict(spark_options or {})
    preferred_columns = [
        options.get("partition_column"),
        options.get("chunk_column"),
    ]
    partition_column = None
    for preferred in preferred_columns:
        if preferred:
            partition_column = str(preferred)
            break
    if not partition_column:
        primary_keys = source_adapter.get_primary_keys(
            source_cursor,
            table_name,
            source_connection_details,
            (source_connection_details or {}).get("schema"),
        )
        for candidate in primary_keys:
            candidate_type = _source_column_type(
                source_cursor,
                source_db_type,
                table_name,
                candidate,
                source_connection_details,
            )
            if _is_spark_partition_type(candidate_type):
                partition_column = candidate
                break
    if not partition_column and options.get("allow_auto_chunk_column_fallback", True):
        partition_column = _find_numeric_fallback_column(
            source_cursor,
            source_db_type,
            table_name,
            source_connection_details,
            excluded_columns=preferred_columns,
        )
    if not partition_column:
        return options

    options["partition_column"] = partition_column
    if options.get("lower_bound") is not None and options.get("upper_bound") is not None:
        return options

    quoted_table = source_adapter.qualify_table_name(table_name, source_connection_details)
    quoted_column = source_adapter.quote_identifier(partition_column)
    source_cursor.execute(
        f"SELECT MIN({quoted_column}), MAX({quoted_column}) FROM {quoted_table}"
    )
    row = source_cursor.fetchone()
    if not row or row[0] is None or row[1] is None:
        return options

    lower_bound = row[0]
    upper_bound = row[1]
    if isinstance(lower_bound, (datetime, date)):
        lower_bound = lower_bound.isoformat(sep=" ") if isinstance(lower_bound, datetime) else lower_bound.isoformat()
    if isinstance(upper_bound, (datetime, date)):
        upper_bound = upper_bound.isoformat(sep=" ") if isinstance(upper_bound, datetime) else upper_bound.isoformat()
    options.setdefault("lower_bound", lower_bound)
    options.setdefault("upper_bound", upper_bound)
    options.setdefault("num_partitions", 8)
    return options


def _normalize_partition_value(value):
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def _build_partition_ranges(lower_bound, upper_bound, num_partitions):
    if lower_bound is None or upper_bound is None:
        return []
    try:
        lower_int = int(lower_bound)
        upper_int = int(upper_bound)
    except Exception:
        return []
    if upper_int < lower_int:
        return []
    span = upper_int - lower_int + 1
    partitions = max(1, int(num_partitions or 1))
    step = max(1, ceil(span / partitions))
    ranges = []
    start = lower_int
    while start <= upper_int:
        end = min(start + step - 1, upper_int)
        ranges.append((start, end))
        start = end + 1
    return ranges


def _fetch_partition_rows(
    source_db_type,
    source_connection_details,
    table_name,
    source_adapter,
    partition_column,
    lower_bound,
    upper_bound,
    source_columns,
):
    connection = None
    cursor = None
    try:
        connection = source_adapter.connect(source_connection_details)
        cursor = connection.cursor()
        quoted_table = source_adapter.qualify_table_name(table_name, source_connection_details)
        quoted_column = source_adapter.quote_identifier(partition_column)
        if lower_bound == upper_bound:
            cursor.execute(
                f"SELECT * FROM {quoted_table} WHERE {quoted_column} = %s",
                (lower_bound,),
            )
        else:
            cursor.execute(
                f"SELECT * FROM {quoted_table} WHERE {quoted_column} >= %s AND {quoted_column} <= %s",
                (lower_bound, upper_bound),
            )
        rows = cursor.fetchall()
        filtered_rows = _filter_rows(rows)
        if not filtered_rows:
            return []
        return [tuple(row) for row in filtered_rows]
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


def _extract_partition_file(
    source_db_type,
    source_connection_details,
    table_name,
    source_adapter,
    partition_column,
    lower_bound,
    upper_bound,
    selected_indexes,
    selected_columns,
    log_callback=None,
    partition_label=None,
):
    if log_callback:
        label = f" ({partition_label})" if partition_label else ""
        log_callback(
            f"Reading partition{label} for {table_name}: {partition_column} between {lower_bound} and {upper_bound}."
        )
    rows = _fetch_partition_rows(
        source_db_type,
        source_connection_details,
        table_name,
        source_adapter,
        partition_column,
        lower_bound,
        upper_bound,
        selected_columns,
    )
    if not rows:
        if log_callback:
            label = f" ({partition_label})" if partition_label else ""
            log_callback(f"Partition{label} for {table_name} returned 0 row(s).")
        return None, 0
    temp_dir = Path(tempfile.gettempdir()) / "dbm_parallel_chunks"
    temp_dir.mkdir(parents=True, exist_ok=True)
    file_path = temp_dir / f"{uuid.uuid4().hex}.csv"
    output_rows = [tuple(row[index] for index in selected_indexes) for row in rows]
    _write_csv_file(selected_columns, output_rows, file_path)
    if log_callback:
        label = f" ({partition_label})" if partition_label else ""
        log_callback(
            f"Finished partition{label} for {table_name}: wrote {len(rows)} row(s) to {file_path.name}."
        )
    return file_path, len(rows)


def _parallel_python_snowflake_load(
    source_db_type,
    source_cursor,
    source_adapter,
    target_adapter,
    table_name,
    source_columns,
    selected_indexes,
    selected_columns,
    source_connection_details,
    target_connection_details,
    spark_options=None,
    log_callback=None,
):
    partition_options = _detect_spark_partitioning(
        source_cursor,
        source_adapter,
        source_db_type,
        table_name,
        source_connection_details,
        spark_options=spark_options,
    )
    partition_column = partition_options.get("partition_column")
    lower_bound = partition_options.get("lower_bound")
    upper_bound = partition_options.get("upper_bound")
    num_partitions = partition_options.get("num_partitions") or 8
    if not partition_column or lower_bound is None or upper_bound is None:
        return None

    ranges = _build_partition_ranges(lower_bound, upper_bound, num_partitions)
    if not ranges:
        return None

    temp_dir = Path(tempfile.gettempdir()) / "dbm_parallel_chunks"
    temp_dir.mkdir(parents=True, exist_ok=True)
    for existing in temp_dir.glob("*.csv"):
        _cleanup_temp_file(existing)

    if log_callback:
        log_callback(
            f"Python parallel chunking enabled for {table_name} using {partition_column} across {len(ranges)} range(s)."
        )

    total_rows = 0
    files = []
    with ThreadPoolExecutor(max_workers=len(ranges)) as executor:
        futures = {
            executor.submit(
                _extract_partition_file,
                source_db_type,
                source_connection_details,
                table_name,
                source_adapter,
                partition_column,
                start,
                end,
                selected_indexes,
                selected_columns,
                log_callback,
                f"{start}-{end}",
            ): (start, end)
            for start, end in ranges
        }
        completed_ranges = 0
        for future in as_completed(futures):
            file_path, row_count = future.result()
            completed_ranges += 1
            if file_path is not None:
                files.append(file_path)
                total_rows += row_count
            if log_callback:
                log_callback(
                    f"Parallel chunking progress for {table_name}: {completed_ranges}/{len(ranges)} partition(s) completed, {total_rows} row(s) staged."
                )

    if not files:
        return None

    try:
        if log_callback:
            log_callback(
                f"Parallel chunking for {table_name} finished staging {total_rows} row(s). Starting Snowflake bulk copy from {len(files)} CSV file(s)."
            )
        load_local_csv_files_to_snowflake(
            target_cursor,
            target_adapter,
            table_name,
            target_connection_details,
            temp_dir,
            log_callback=log_callback,
        )
    finally:
        for file_path in files:
            _cleanup_temp_file(file_path)
    return total_rows


def _parallel_fetch_source_rows(
    source_db_type,
    source_adapter,
    table_name,
    source_connection_details,
    partition_column,
    lower_bound,
    upper_bound,
    num_partitions,
    log_callback=None,
):
    ranges = _build_partition_ranges(lower_bound, upper_bound, num_partitions)
    if not ranges:
        return []
    if log_callback:
        log_callback(
            f"Parallel chunking enabled for {table_name} using {partition_column} across {len(ranges)} range(s)."
        )
    all_rows = []
    with ThreadPoolExecutor(max_workers=len(ranges)) as executor:
        futures = [
            executor.submit(
                _fetch_partition_rows,
                source_db_type,
                source_connection_details,
                table_name,
                source_adapter,
                partition_column,
                start,
                end,
                None,
            )
            for start, end in ranges
        ]
        for future in as_completed(futures):
            all_rows.extend(future.result())
    return all_rows


def get_table_count_summary(
    source_cursor,
    target_cursor,
    source_db_type,
    target_db_type,
    table_name,
    source_connection_details=None,
    target_connection_details=None,
):
    source_adapter = get_adapter(source_db_type)
    target_adapter = get_adapter(target_db_type)
    source_row_count = _count_rows(
        source_cursor,
        source_adapter,
        table_name,
        source_connection_details,
    )
    target_row_count = _count_rows(
        target_cursor,
        target_adapter,
        table_name,
        target_connection_details,
    )
    return {
        "source_row_count": source_row_count,
        "target_row_count": target_row_count,
        "missing_row_count": max(source_row_count - target_row_count, 0),
    }


def target_table_exists(
    target_cursor,
    target_db_type,
    table_name,
    target_connection_details=None,
):
    target_adapter = get_adapter(target_db_type)
    qualified_table = target_adapter.qualify_table_name(
        table_name,
        target_connection_details,
    )
    try:
        target_cursor.execute(f"SELECT * FROM {qualified_table} WHERE 1 = 0")
        return True
    except Exception:
        return False


def truncate_target_table(
    target_cursor,
    target_db_type,
    table_name,
    target_connection_details=None,
):
    target_adapter = get_adapter(target_db_type)
    qualified_table = target_adapter.qualify_table_name(
        table_name,
        target_connection_details,
    )
    target_cursor.execute(f"TRUNCATE TABLE {qualified_table}")


def drop_target_table(
    target_cursor,
    target_db_type,
    table_name,
    target_connection_details=None,
):
    target_adapter = get_adapter(target_db_type)
    qualified_table = target_adapter.qualify_table_name(
        table_name,
        target_connection_details,
    )
    if target_db_type == "SQL Server":
        target_cursor.execute(
            f"IF OBJECT_ID(N'{qualified_table}', N'U') IS NOT NULL DROP TABLE {qualified_table}"
        )
        return
    target_cursor.execute(f"DROP TABLE IF EXISTS {qualified_table}")


def migrate_table_data(
    source_cursor,
    target_cursor,
    source_db_type,
    target_db_type,
    table_name,
    source_connection_details=None,
    target_connection_details=None,
    migration_mode="insert",
    batch_size=DEFAULT_DATA_BATCH_SIZE,
    execution_engine="python",
    spark_row_threshold=1000,
    spark_options=None,
    log_callback=None,
):
    source_adapter = get_adapter(source_db_type)
    target_adapter = get_adapter(target_db_type)
    migration_mode = (migration_mode or "insert").lower()
    execution_engine = (execution_engine or "python").lower()
    batch_size = _normalize_batch_size(batch_size)

    if execution_engine not in {"python", "spark", "auto"}:
        raise Exception(f"Unsupported data execution engine: {execution_engine}")

    if execution_engine in {"spark", "auto"}:
        from app.services.spark_bulk_loader import (
            resolve_execution_engine,
            spark_bulk_load_table,
        )

        source_row_count_for_engine = None
        effective_spark_options = dict(spark_options or {})
        if execution_engine == "auto":
            source_row_count_for_engine = _count_rows(
                source_cursor,
                source_adapter,
                table_name,
                source_connection_details,
            )
            if log_callback:
                log_callback(
                    "Auto engine selection evaluated source row count "
                    f"{source_row_count_for_engine} with spark threshold {spark_row_threshold}."
                )

        effective_spark_options = _detect_spark_partitioning(
            source_cursor,
            source_adapter,
            source_db_type,
            table_name,
            source_connection_details,
            spark_options=effective_spark_options,
        )
        if log_callback and effective_spark_options.get("partition_column"):
            log_callback(
                "Spark partitioning configured with "
                f"column {effective_spark_options.get('partition_column')}, "
                f"lower_bound={effective_spark_options.get('lower_bound')}, "
                f"upper_bound={effective_spark_options.get('upper_bound')}, "
                f"num_partitions={effective_spark_options.get('num_partitions')}."
            )

        resolved_engine = resolve_execution_engine(
            execution_engine,
            source_db_type,
            target_db_type,
            migration_mode,
            source_row_count=source_row_count_for_engine,
            spark_row_threshold=spark_row_threshold,
            spark_options=effective_spark_options,
        )
        if log_callback:
            log_callback(f"Resolved data execution engine: {resolved_engine}.")
        if resolved_engine == "spark":
            if log_callback:
                log_callback(
                    f"Using Spark bulk load engine for {source_db_type} -> {target_db_type}."
                )
            return spark_bulk_load_table(
                source_db_type,
                target_db_type,
                table_name,
                source_connection_details,
                target_connection_details,
                target_cursor,
                batch_size=batch_size,
                spark_options=effective_spark_options,
                log_callback=log_callback,
            )

    _enable_fast_batch_mode(target_cursor, target_db_type)

    total_rows = 0
    batch_number = 0
    batch_executor = None
    source_columns = None
    selected_indexes = None
    selected_columns = None
    snowflake_stage_name = None
    snowflake_file_writer = None
    snowflake_staged_row_count = 0
    mysql_file_writer = None
    mysql_staged_row_count = 0

    try:
        for columns, rows in _iter_source_batches(
            source_cursor,
            source_adapter,
            table_name,
            source_connection_details,
            batch_size,
        ):
            if source_columns is None:
                source_columns = columns
                target_columns = _get_target_columns(
                    target_cursor,
                    target_adapter,
                    table_name,
                    target_connection_details,
                )
                if target_db_type == "Snowflake":
                    target_lookup = {
                        _normalize_identifier(column): column for column in target_columns
                    }
                    missing_columns = [
                        column
                        for column in source_columns
                        if _normalize_identifier(column) not in target_lookup
                    ]
                    if missing_columns:
                        _auto_add_missing_snowflake_columns(
                            target_cursor,
                            target_adapter,
                            table_name,
                            missing_columns,
                            source_columns,
                            rows,
                            target_connection_details,
                            log_callback=log_callback,
                        )
                        target_columns = _get_target_columns(
                            target_cursor,
                            target_adapter,
                            table_name,
                            target_connection_details,
                        )
                if target_columns:
                    target_lookup = {
                        _normalize_identifier(column): column for column in target_columns
                    }
                    selected_indexes = []
                    selected_columns = []
                    for index, column in enumerate(source_columns):
                        normalized = _normalize_identifier(column)
                        if normalized in target_lookup:
                            selected_indexes.append(index)
                            selected_columns.append(
                                _preserve_target_column_identifier(
                                    target_db_type,
                                    target_lookup[normalized],
                                )
                            )
                else:
                    selected_indexes = list(range(len(source_columns)))
                    selected_columns = list(source_columns)

                if not selected_columns:
                    raise Exception(
                        f"No compatible target columns found for {table_name}. "
                        "The transformed target table does not match the source columns."
                    )

                target_column_types = _get_target_column_types(
                    target_cursor,
                    target_adapter,
                    table_name,
                    target_connection_details,
                )
                selected_column_types = {
                    column: target_column_types.get(str(column).lower())
                    for column in selected_columns
                }

                if (
                    target_db_type == "Snowflake"
                    and migration_mode == "insert"
                    and execution_engine in {"python", "auto"}
                ):
                    parallel_rows = _parallel_python_snowflake_load(
                        source_db_type,
                        source_cursor,
                        source_adapter,
                        target_adapter,
                        table_name,
                        source_columns,
                        selected_indexes,
                        selected_columns,
                        source_connection_details,
                        target_connection_details,
                        spark_options=spark_options,
                        log_callback=log_callback,
                    )
                    if parallel_rows is not None:
                        return parallel_rows

                if target_db_type == "MySQL" and migration_mode == "insert":
                    mysql_file_writer = _MySQLCsvStagedFileWriter(len(selected_columns))

                    def execute_batch(batch_rows):
                        mysql_file_writer.write_rows(batch_rows)
                        return len(batch_rows)

                    batch_executor = execute_batch

            if batch_executor is None:
                if target_db_type == "Snowflake" and migration_mode == "insert":
                    snowflake_stage_name = _create_snowflake_temp_stage(target_cursor)
                    snowflake_file_writer = _SnowflakeStagedFileWriter(len(selected_columns))
                    if log_callback:
                        log_callback(
                            f"Snowflake bulk load for {table_name} is using CSV staging."
                        )

                    def execute_batch(batch_rows):
                        snowflake_file_writer.write_rows(batch_rows)
                        return len(batch_rows)

                    batch_executor = execute_batch
                else:
                    batch_executor = _build_batch_executor(
                        target_cursor,
                        target_adapter,
                        table_name,
                        selected_columns,
                        target_db_type,
                        target_connection_details,
                        migration_mode,
                    )
            selected_rows = [
                tuple(row[index] for index in selected_indexes)
                for row in rows
            ]
            batch_number += 1
            if log_callback:
                log_callback(
                    f"Processing batch {batch_number} for {table_name}: {len(selected_rows)} row(s) in memory."
                )
            processed_rows = batch_executor(selected_rows)
            total_rows += processed_rows
            if snowflake_file_writer is not None:
                snowflake_staged_row_count += processed_rows
            if mysql_file_writer is not None:
                mysql_staged_row_count += processed_rows
            if log_callback:
                log_callback(
                    f"Batch {batch_number} for {table_name} completed: {processed_rows} row(s) written, {total_rows} total row(s) processed."
                )

        if snowflake_file_writer is not None and snowflake_staged_row_count:
            snowflake_file_writer.close()
            if log_callback:
                log_callback(
                    f"Finalizing Snowflake staged file for {table_name}: {snowflake_staged_row_count} row(s) staged."
                )
            _finalize_snowflake_staged_load(
                target_cursor,
                target_adapter,
                table_name,
                selected_columns,
                target_connection_details,
                snowflake_stage_name,
                snowflake_file_writer.file_path,
                snowflake_staged_row_count,
                log_callback=log_callback,
            )
        if mysql_file_writer is not None and mysql_staged_row_count:
            mysql_file_writer.close()
            if log_callback:
                log_callback(
                    f"Finalizing MySQL staged CSV for {table_name}: {mysql_staged_row_count} row(s) staged."
                )
            load_local_csv_to_mysql(
                target_cursor,
                target_adapter,
                table_name,
                selected_columns,
                target_connection_details,
                mysql_file_writer.file_path,
                log_callback=log_callback,
            )

        return total_rows
    finally:
        if snowflake_file_writer is not None:
            snowflake_file_writer.close()
            _cleanup_temp_file(snowflake_file_writer.file_path)
        if mysql_file_writer is not None:
            mysql_file_writer.close()
            _cleanup_temp_file(mysql_file_writer.file_path)
