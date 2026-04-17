import psycopg2

from app.adapters.base import DatabaseAdapter
from app.adapters.common import empty_object_summary, first_value


class PostgreSQLAdapter(DatabaseAdapter):
    engine_name = "PostgreSQL"

    def connect(self, details):
        port = details.get("port")
        return psycopg2.connect(
            host=details["host"],
            port=int(port) if port else 5432,
            user=details["username"],
            password=details["password"],
            dbname=details.get("database") or "postgres",
        )

    def list_databases(self, cursor, details=None):
        cursor.execute(
            """
            SELECT datname
            FROM pg_database
            WHERE datistemplate = false
            ORDER BY datname
            """
        )
        return [row[0] for row in cursor.fetchall()]

    def list_schemas(self, cursor, database_name, details=None):
        cursor.execute(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
            ORDER BY schema_name
            """
        )
        return [row[0] for row in cursor.fetchall()]

    def get_object_summary(self, cursor, database_name, schema_name):
        summary = empty_object_summary()
        cursor.execute(
            """
            SELECT
                SUM(CASE WHEN table_type = 'BASE TABLE' THEN 1 ELSE 0 END),
                SUM(CASE WHEN table_type = 'VIEW' THEN 1 ELSE 0 END)
            FROM information_schema.tables
            WHERE table_schema = %s
            """,
            (schema_name,),
        )
        row = cursor.fetchone() or (0, 0)
        summary["table"] = row[0] or 0
        summary["view"] = row[1] or 0

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.routines
            WHERE specific_schema = %s
              AND routine_type = 'PROCEDURE'
            """,
            (schema_name,),
        )
        summary["storedprocedure"] = first_value(cursor)

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.routines
            WHERE specific_schema = %s
              AND routine_type = 'FUNCTION'
            """,
            (schema_name,),
        )
        summary["function"] = first_value(cursor)

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.triggers
            WHERE trigger_schema = %s
            """,
            (schema_name,),
        )
        summary["trigger"] = first_value(cursor)
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
                (schema_name,),
            ),
            "view": (
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'VIEW'
                ORDER BY table_name
                """,
                (schema_name,),
            ),
            "storedprocedure": (
                """
                SELECT routine_name
                FROM information_schema.routines
                WHERE specific_schema = %s
                  AND routine_type = 'PROCEDURE'
                ORDER BY routine_name
                """,
                (schema_name,),
            ),
            "function": (
                """
                SELECT routine_name
                FROM information_schema.routines
                WHERE specific_schema = %s
                  AND routine_type = 'FUNCTION'
                ORDER BY routine_name
                """,
                (schema_name,),
            ),
            "trigger": (
                """
                SELECT trigger_name
                FROM information_schema.triggers
                WHERE trigger_schema = %s
                ORDER BY trigger_name
                """,
                (schema_name,),
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
        if object_type == "view":
            cursor.execute(
                """
                SELECT 'CREATE OR REPLACE VIEW ' || %s || ' AS ' || pg_get_viewdef(%s::regclass, true)
                """,
                (object_name, object_name),
            )
            row = cursor.fetchone()
            if row:
                return row[0]
            raise Exception(f"Could not locate PostgreSQL view: {object_name}")

        if object_type in {"function", "storedprocedure"}:
            cursor.execute(
                """
                SELECT pg_get_functiondef(p.oid)
                FROM pg_proc p
                WHERE p.proname = %s
                ORDER BY p.oid
                LIMIT 1
                """,
                (object_name,),
            )
            row = cursor.fetchone()
            if row:
                return row[0]
            raise Exception(f"Could not locate PostgreSQL {object_type}: {object_name}")

        if object_type == "trigger":
            cursor.execute(
                """
                SELECT pg_get_triggerdef(t.oid, true)
                FROM pg_trigger t
                WHERE t.tgname = %s
                  AND NOT t.tgisinternal
                LIMIT 1
                """,
                (object_name,),
            )
            row = cursor.fetchone()
            if row:
                return row[0]
            raise Exception(f"Could not locate PostgreSQL trigger: {object_name}")

        if object_type == "table":
            cursor.execute(
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
                """,
                (object_name,),
            )
            columns = cursor.fetchall()
            if not columns:
                raise Exception(f"Could not locate PostgreSQL table: {object_name}")

            lines = []
            for column_name, data_type, is_nullable, column_default in columns:
                line = f'"{column_name}" {data_type}'
                if column_default is not None:
                    line += f" DEFAULT {column_default}"
                if is_nullable == "NO":
                    line += " NOT NULL"
                lines.append(line)

            return "CREATE TABLE " + object_name + " (\n  " + ",\n  ".join(lines) + "\n)"

        raise Exception(f"Unsupported extraction for PostgreSQL {object_type}.")

    def quote_identifier(self, identifier):
        return f'"{identifier}"'

    def placeholder(self):
        return "%s"

    def get_primary_keys(
        self, cursor, table_name, connection_details=None, schema_name=None
    ):
        details = connection_details or {}
        schema = schema_name or details.get("schema") or "public"
        base_table_name = table_name.split(".")[-1]
        cursor.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
            """,
            (schema, base_table_name),
        )
        return [row[0] for row in cursor.fetchall()]

    def qualify_table_name(self, table_name, connection_details=None):
        details = connection_details or {}
        if "." in table_name:
            return super().qualify_table_name(table_name, connection_details)
        schema = details.get("schema")
        if schema:
            return ".".join([self.quote_identifier(schema), self.quote_identifier(table_name)])
        return self.quote_identifier(table_name)

    def ensure_database_and_schema(
        self, connection, cursor, connection_details=None
    ):
        details = connection_details or {}
        database = details.get("database")
        schema = details.get("schema")
        reconnect_required = False
        if database:
            previous_autocommit = getattr(connection, "autocommit", False)
            connection.autocommit = True
            try:
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (database,),
                )
                if not cursor.fetchone():
                    cursor.execute(
                        f"CREATE DATABASE {self.quote_identifier(database)}"
                    )
                cursor.execute("SELECT current_database()")
                current_database = cursor.fetchone()[0]
                reconnect_required = current_database != database
            finally:
                connection.autocommit = previous_autocommit
        if schema and not reconnect_required:
            cursor.execute(
                f"CREATE SCHEMA IF NOT EXISTS {self.quote_identifier(schema)}"
            )
        return {
            "status": "success",
            "message": "PostgreSQL database/schema bootstrap completed.",
            "reconnect_required": reconnect_required,
        }
