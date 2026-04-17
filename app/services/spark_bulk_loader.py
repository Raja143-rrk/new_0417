import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from app.adapters.registry import get_adapter
from app.services.data_migration import load_local_csv_files_to_snowflake


SUPPORTED_SPARK_TARGET = "snowflake"
JDBC_SOURCE_CONFIG = {
    "mysql": {
        "driver": "com.mysql.cj.jdbc.Driver",
        "env_var": "DBM_SPARK_MYSQL_JDBC_JAR",
        "option_key": "mysql_jdbc_jar",
    },
    "postgresql": {
        "driver": "org.postgresql.Driver",
        "env_var": "DBM_SPARK_POSTGRES_JDBC_JAR",
        "option_key": "postgres_jdbc_jar",
    },
    "sql server": {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "env_var": "DBM_SPARK_SQLSERVER_JDBC_JAR",
        "option_key": "sqlserver_jdbc_jar",
    },
}


def _project_root():
    return Path(__file__).resolve().parents[2]


def _worker_module_path():
    return _project_root() / "app" / "services" / "spark_bulk_job.py"


def _supports_inline_pyspark():
    try:
        import pyspark  # noqa: F401
    except Exception:
        return False
    return True


def _spark_runtime_available():
    return shutil.which("spark-submit") is not None or _supports_inline_pyspark()


def _normalize_db_type(db_type):
    return str(db_type or "").strip().lower()


def _get_source_jdbc_config(source_db_type):
    return JDBC_SOURCE_CONFIG.get(_normalize_db_type(source_db_type))


def _get_source_jdbc_jar(source_db_type, spark_options=None):
    options = dict(spark_options or {})
    config = _get_source_jdbc_config(source_db_type)
    if not config:
        return None
    value = options.get(config["option_key"]) or os.getenv(config["env_var"])
    if not value:
        return None
    return Path(str(value)).expanduser().resolve()


def _get_python_executable(spark_options=None):
    configured = (spark_options or {}).get("python_executable")
    if configured:
        return str(Path(str(configured)).expanduser())
    return sys.executable


def supports_spark_bulk_load(source_db_type, target_db_type, migration_mode):
    return (
        _normalize_db_type(source_db_type) in JDBC_SOURCE_CONFIG
        and _normalize_db_type(target_db_type) == SUPPORTED_SPARK_TARGET
        and str(migration_mode or "").lower() == "insert"
    )


def resolve_execution_engine(
    requested_engine,
    source_db_type,
    target_db_type,
    migration_mode,
    source_row_count=None,
    spark_row_threshold=1000,
    spark_options=None,
):
    engine = str(requested_engine or "python").lower()
    if engine == "python":
        return "python"

    spark_supported = supports_spark_bulk_load(
        source_db_type,
        target_db_type,
        migration_mode,
    )
    spark_ready = (
        spark_supported
        and _get_source_jdbc_jar(source_db_type, spark_options) is not None
        and _spark_runtime_available()
    )

    if engine == "auto":
        if not spark_ready or source_row_count is None:
            return "python"
        return "spark" if int(source_row_count) > int(spark_row_threshold or 0) else "python"

    if engine != "spark":
        raise Exception(f"Unsupported execution engine: {requested_engine}")
    if not spark_supported:
        raise Exception(
            "Spark bulk loading is supported only for JDBC sources (MySQL, PostgreSQL, SQL Server) to Snowflake in insert mode."
        )
    if _get_source_jdbc_jar(source_db_type, spark_options) is None:
        raise Exception(
            "Spark bulk loading requires a source JDBC jar. Configure the matching spark_options jar path or environment variable."
        )
    if not _spark_runtime_available():
        raise Exception(
            "Spark bulk loading requires pyspark or spark-submit to be installed in the runtime."
        )
    return "spark"


def _source_jdbc_url(source_db_type, source_details):
    db_type = _normalize_db_type(source_db_type)
    host = source_details.get("host") or "localhost"
    port = source_details.get("port")
    database = source_details.get("database") or ""
    if db_type == "mysql":
        port = port or 3306
        return f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&rewriteBatchedStatements=true&useCursorFetch=true"
    if db_type == "postgresql":
        port = port or 5432
        return f"jdbc:postgresql://{host}:{port}/{database}"
    if db_type == "sql server":
        port = port or 1433
        return f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=false;trustServerCertificate=true"
    raise Exception(f"Unsupported Spark JDBC source: {source_db_type}")


def _build_worker_command(
    source_db_type,
    table_name,
    source_details,
    output_dir,
    batch_size,
    spark_options=None,
):
    options = dict(spark_options or {})
    jdbc_config = _get_source_jdbc_config(source_db_type)
    jdbc_jar = _get_source_jdbc_jar(source_db_type, options)
    command = [
        _get_python_executable(options),
        str(_worker_module_path()),
        "--jdbc-url",
        _source_jdbc_url(source_db_type, source_details),
        "--jdbc-driver",
        jdbc_config["driver"],
        "--jdbc-jar",
        str(jdbc_jar),
        "--source-user",
        str(source_details.get("username") or ""),
        "--source-password",
        str(source_details.get("password") or ""),
        "--table-name",
        str(table_name),
        "--output-dir",
        str(output_dir),
        "--jdbc-fetch-size",
        str(batch_size),
        "--spark-master",
        str(options.get("master") or "local[*]"),
        "--output-partitions",
        str(options.get("output_partitions") or 1),
    ]
    if options.get("partition_column"):
        command.extend(["--partition-column", str(options["partition_column"])])
    if options.get("lower_bound") is not None:
        command.extend(["--lower-bound", str(options["lower_bound"])])
    if options.get("upper_bound") is not None:
        command.extend(["--upper-bound", str(options["upper_bound"])])
    if options.get("num_partitions") is not None:
        command.extend(["--num-partitions", str(options["num_partitions"])])
    if options.get("count_rows"):
        command.append("--count-rows")
    return command


def spark_bulk_load_table(
    source_db_type,
    target_db_type,
    table_name,
    source_connection_details,
    target_connection_details,
    target_cursor,
    batch_size,
    spark_options=None,
    log_callback=None,
):
    if not supports_spark_bulk_load(source_db_type, target_db_type, "insert"):
        raise Exception(
            "Spark bulk loading is supported only for JDBC sources (MySQL, PostgreSQL, SQL Server) to Snowflake in insert mode."
        )

    source_details = dict(source_connection_details or {})
    target_details = dict(target_connection_details or {})
    target_adapter = get_adapter(target_db_type)

    with tempfile.TemporaryDirectory(prefix="dbm_spark_bulk_") as temp_dir:
        output_dir = Path(temp_dir) / "csv_output"
        metadata_file = Path(temp_dir) / "spark_job_result.json"
        command = _build_worker_command(
            source_db_type,
            table_name,
            source_details,
            output_dir,
            batch_size,
            spark_options=spark_options,
        )
        env = dict(os.environ)
        env["DBM_SPARK_RESULT_FILE"] = str(metadata_file)

        if log_callback:
            log_callback(f"Starting Spark bulk extract for {table_name} into {output_dir}.")

        completed = subprocess.run(
            command,
            cwd=str(_project_root()),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        stdout = (completed.stdout or "").strip()
        stderr = (completed.stderr or "").strip()
        if stdout and log_callback:
            log_callback(f"Spark stdout: {stdout[-1500:]}")
        if stderr and log_callback:
            log_callback(f"Spark stderr: {stderr[-1500:]}")
        if completed.returncode != 0:
            raise Exception(
                "Spark bulk load job failed. "
                f"Exit code: {completed.returncode}. stderr: {stderr[-1500:] or 'n/a'}"
            )

        load_local_csv_files_to_snowflake(
            target_cursor,
            target_adapter,
            table_name,
            target_details,
            output_dir,
            log_callback=log_callback,
        )

        row_count = 0
        if metadata_file.exists():
            try:
                metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
                row_count = int(metadata.get("row_count") or 0)
            except Exception:
                row_count = 0
        return row_count
