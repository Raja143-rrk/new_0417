DB_TYPE_ALIASES = {
    "MySQL": "MySQL",
    "mysql": "MySQL",
    "SQL Server": "SQL Server",
    "sqlserver": "SQL Server",
    "sql_server": "SQL Server",
    "Azure SQL": "SQL Server",
    "azuresql": "SQL Server",
    "azure_sql": "SQL Server",
    "PostgreSQL": "PostgreSQL",
    "postgresql": "PostgreSQL",
    "postgres": "PostgreSQL",
    "Amazon Redshift": "PostgreSQL",
    "redshift": "PostgreSQL",
    "Google Cloud SQL": "PostgreSQL",
    "Snowflake": "Snowflake",
    "snowflake": "Snowflake",
}


def normalize_db_type(db_type):
    normalized = DB_TYPE_ALIASES.get(db_type)
    if not normalized:
        raise Exception(f"Unsupported DB: {db_type}")
    return normalized


def _build_adapters():
    from app.adapters.mysql_adapter import MySQLAdapter
    from app.adapters.postgres_adapter import PostgreSQLAdapter
    from app.adapters.snowflake_adapter import SnowflakeAdapter
    from app.adapters.sqlserver_adapter import SQLServerAdapter

    return {
        "MySQL": MySQLAdapter(),
        "SQL Server": SQLServerAdapter(),
        "PostgreSQL": PostgreSQLAdapter(),
        "Snowflake": SnowflakeAdapter(),
    }


ADAPTERS = _build_adapters()


def get_adapter(db_type):
    return ADAPTERS[normalize_db_type(db_type)]


def list_registered_adapters():
    return sorted(ADAPTERS.keys())
