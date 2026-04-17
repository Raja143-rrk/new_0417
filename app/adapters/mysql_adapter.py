from pymysql.constants import CLIENT
import pymysql

from app.adapters.base import DatabaseAdapter
from app.adapters.common import empty_object_summary, fetch_required, first_value


class MySQLAdapter(DatabaseAdapter):
    engine_name = "MySQL"

    def connect(self, details):
        port = details.get("port")
        return pymysql.connect(
            host=details["host"],
            port=int(port) if port else 3306,
            user=details["username"],
            password=details["password"],
            database=details.get("database"),
            local_infile=True,
            client_flag=CLIENT.LOCAL_FILES,
        )

    def list_databases(self, cursor, details=None):
        cursor.execute("SHOW DATABASES")
        return [row[0] for row in cursor.fetchall()]

    def list_schemas(self, cursor, database_name, details=None):
        return [database_name] if database_name else []

    def get_object_summary(self, cursor, database_name, schema_name):
        summary = empty_object_summary()
        cursor.execute(
            """
            SELECT
                SUM(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 1 ELSE 0 END),
                SUM(CASE WHEN TABLE_TYPE = 'VIEW' THEN 1 ELSE 0 END)
            FROM information_schema.tables
            WHERE table_schema = %s
            """,
            (database_name,),
        )
        row = cursor.fetchone() or (0, 0)
        summary["table"] = row[0] or 0
        summary["view"] = row[1] or 0

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.routines
            WHERE routine_schema = %s
              AND routine_type = 'PROCEDURE'
            """,
            (database_name,),
        )
        summary["storedprocedure"] = first_value(cursor)

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.routines
            WHERE routine_schema = %s
              AND routine_type = 'FUNCTION'
            """,
            (database_name,),
        )
        summary["function"] = first_value(cursor)

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.triggers
            WHERE trigger_schema = %s
            """,
            (database_name,),
        )
        summary["trigger"] = first_value(cursor)

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.events
            WHERE event_schema = %s
            """,
            (database_name,),
        )
        summary["event"] = first_value(cursor)
        return summary

    def list_objects(self, cursor, database_name, schema_name, object_type):
        queries = {
            "table": (
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (database_name,),
            ),
            "view": (
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'VIEW'
                ORDER BY table_name
                """,
                (database_name,),
            ),
            "storedprocedure": (
                """
                SELECT routine_name
                FROM information_schema.routines
                WHERE routine_schema = %s
                  AND routine_type = 'PROCEDURE'
                ORDER BY routine_name
                """,
                (database_name,),
            ),
            "function": (
                """
                SELECT routine_name
                FROM information_schema.routines
                WHERE routine_schema = %s
                  AND routine_type = 'FUNCTION'
                ORDER BY routine_name
                """,
                (database_name,),
            ),
            "trigger": (
                """
                SELECT trigger_name
                FROM information_schema.triggers
                WHERE trigger_schema = %s
                ORDER BY trigger_name
                """,
                (database_name,),
            ),
            "event": (
                """
                SELECT event_name
                FROM information_schema.events
                WHERE event_schema = %s
                ORDER BY event_name
                """,
                (database_name,),
            ),
        }
        if object_type == "cursor":
            return []
        query = queries.get(object_type)
        if not query:
            return []
        cursor.execute(query[0], query[1])
        return [row[0] for row in cursor.fetchall()]

    def extract_ddl(self, cursor, object_name, object_type, connection_details=None):
        if object_type == "table":
            cursor.execute(f"SHOW CREATE TABLE `{object_name}`")
            return fetch_required(cursor, 1, f"Could not locate MySQL table: {object_name}")
        if object_type == "view":
            cursor.execute(f"SHOW CREATE VIEW `{object_name}`")
            return fetch_required(cursor, 1, f"Could not locate MySQL view: {object_name}")
        if object_type == "storedprocedure":
            cursor.execute(f"SHOW CREATE PROCEDURE `{object_name}`")
            return fetch_required(
                cursor, 2, f"Could not locate MySQL stored procedure: {object_name}"
            )
        if object_type == "function":
            cursor.execute(f"SHOW CREATE FUNCTION `{object_name}`")
            return fetch_required(cursor, 2, f"Could not locate MySQL function: {object_name}")
        if object_type == "trigger":
            cursor.execute(f"SHOW CREATE TRIGGER `{object_name}`")
            return fetch_required(cursor, 2, f"Could not locate MySQL trigger: {object_name}")
        if object_type == "event":
            cursor.execute(f"SHOW CREATE EVENT `{object_name}`")
            return fetch_required(cursor, 3, f"Could not locate MySQL event: {object_name}")
        raise Exception(f"Unsupported extraction for MySQL {object_type}.")

    def quote_identifier(self, identifier):
        return f"`{identifier}`"

    def placeholder(self):
        return "%s"

    def get_primary_keys(
        self, cursor, table_name, connection_details=None, schema_name=None
    ):
        details = connection_details or {}
        database_name = details.get("database")
        if not database_name:
            return []
        base_table_name = table_name.split(".")[-1]
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = %s
              AND table_name = %s
              AND constraint_name = 'PRIMARY'
            ORDER BY ordinal_position
            """,
            (database_name, base_table_name),
        )
        return [row[0] for row in cursor.fetchall()]

    def preflight_validate_sql(
        self, cursor, sql_text, object_type, object_name, connection_details=None
    ):
        if object_type not in {"table", "view"}:
            return {
                "status": "skipped",
                "message": (
                    f"MySQL preflight validation is skipped for {object_type} "
                    "because PREPARE does not safely support this statement type."
                ),
            }
        try:
            cursor.execute("PREPARE migration_stmt FROM %s", (sql_text,))
            cursor.execute("DEALLOCATE PREPARE migration_stmt")
            return {"status": "success", "message": "MySQL preflight validation passed."}
        except Exception as error:
            try:
                cursor.execute("DEALLOCATE PREPARE migration_stmt")
            except Exception:
                pass
            return {"status": "error", "message": str(error)}

    def ensure_database_and_schema(
        self, connection, cursor, connection_details=None
    ):
        details = connection_details or {}
        database = details.get("database")
        if database:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.quote_identifier(database)}")
            cursor.execute(f"USE {self.quote_identifier(database)}")
        return {
            "status": "success",
            "message": "MySQL database bootstrap completed.",
            "reconnect_required": False,
        }
