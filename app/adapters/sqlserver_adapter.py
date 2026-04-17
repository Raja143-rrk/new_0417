import pyodbc

from app.adapters.base import DatabaseAdapter
from app.adapters.common import empty_object_summary, first_value


class SQLServerAdapter(DatabaseAdapter):
    engine_name = "SQL Server"

    def connect(self, details):
        port = details.get("port")
        server = details["host"]
        if port:
            server = f"{server},{port}"

        database = details.get("database") or "master"
        conn_str = f"""
        DRIVER={{ODBC Driver 17 for SQL Server}};
        SERVER={server};
        UID={details['username']};
        PWD={details['password']};
        DATABASE={database};
        """
        return pyodbc.connect(conn_str)

    def list_databases(self, cursor, details=None):
        cursor.execute(
            """
            SELECT name
            FROM sys.databases
            WHERE database_id > 4
            ORDER BY name
            """
        )
        return [row[0] for row in cursor.fetchall()]

    def list_schemas(self, cursor, database_name, details=None):
        cursor.execute(
            """
            SELECT name
            FROM sys.schemas
            WHERE schema_id < 16384
            ORDER BY name
            """
        )
        return [row[0] for row in cursor.fetchall()]

    def get_object_summary(self, cursor, database_name, schema_name):
        summary = empty_object_summary()
        cursor.execute(
            """
            SELECT
                SUM(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 1 ELSE 0 END),
                SUM(CASE WHEN TABLE_TYPE = 'VIEW' THEN 1 ELSE 0 END)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ?
            """,
            (schema_name,),
        )
        row = cursor.fetchone() or (0, 0)
        summary["table"] = row[0] or 0
        summary["view"] = row[1] or 0

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_SCHEMA = ?
              AND ROUTINE_TYPE = 'PROCEDURE'
            """,
            (schema_name,),
        )
        summary["storedprocedure"] = first_value(cursor)

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_SCHEMA = ?
              AND ROUTINE_TYPE = 'FUNCTION'
            """,
            (schema_name,),
        )
        summary["function"] = first_value(cursor)

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM sys.triggers t
            INNER JOIN sys.objects o ON t.parent_id = o.object_id
            INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE t.parent_class = 1
              AND s.name = ?
            """,
            (schema_name,),
        )
        summary["trigger"] = first_value(cursor)

        cursor.execute("SELECT COUNT(*) FROM sys.sequences")
        summary["sequence"] = first_value(cursor)
        cursor.execute("SELECT COUNT(*) FROM sys.synonyms")
        summary["synonym"] = first_value(cursor)
        return summary

    def list_objects(self, cursor, database_name, schema_name, object_type):
        queries = {
            "table": (
                """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ?
                  AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
                """,
                (schema_name,),
            ),
            "view": (
                """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ?
                  AND TABLE_TYPE = 'VIEW'
                ORDER BY TABLE_NAME
                """,
                (schema_name,),
            ),
            "storedprocedure": (
                """
                SELECT ROUTINE_NAME
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_SCHEMA = ?
                  AND ROUTINE_TYPE = 'PROCEDURE'
                ORDER BY ROUTINE_NAME
                """,
                (schema_name,),
            ),
            "function": (
                """
                SELECT ROUTINE_NAME
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_SCHEMA = ?
                  AND ROUTINE_TYPE = 'FUNCTION'
                ORDER BY ROUTINE_NAME
                """,
                (schema_name,),
            ),
            "trigger": (
                """
                SELECT t.name
                FROM sys.triggers t
                INNER JOIN sys.objects o ON t.parent_id = o.object_id
                INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE t.parent_class = 1
                  AND s.name = ?
                ORDER BY t.name
                """,
                (schema_name,),
            ),
            "sequence": ("SELECT name FROM sys.sequences ORDER BY name", ()),
            "synonym": ("SELECT name FROM sys.synonyms ORDER BY name", ()),
        }
        if object_type == "cursor":
            return []
        query = queries.get(object_type)
        if not query:
            return []
        cursor.execute(query[0], query[1])
        return [row[0] for row in cursor.fetchall()]

    def extract_ddl(self, cursor, object_name, object_type, connection_details=None):
        try:
            if object_type in {"view", "storedprocedure", "trigger", "function"}:
                details = connection_details or {}
                raw_name = str(object_name or "").strip()
                if "." in raw_name:
                    qualified_name = raw_name
                else:
                    schema = str(details.get("schema") or "dbo").strip()
                    qualified_name = f"{schema}.{raw_name}"
                cursor.execute("EXEC sp_helptext ?", (qualified_name,))
                return "".join([row[0] for row in cursor.fetchall()])
            if object_type == "table":
                details = connection_details or {}
                base_table_name = object_name.split(".")[-1].strip("[]`\"")
                explicit_schema = None
                if "." in str(object_name):
                    explicit_schema = str(object_name).split(".")[-2].strip("[]`\"")
                schema_candidates = [
                    explicit_schema,
                    details.get("schema"),
                    "dbo",
                ]
                columns = []
                chosen_schema = None
                for schema in [schema for schema in schema_candidates if schema]:
                    cursor.execute(
                        """
                        SELECT
                            c.name AS column_name,
                            ty.name AS data_type,
                            c.max_length,
                            c.precision,
                            c.scale,
                            c.is_nullable,
                            c.is_identity,
                            c.is_computed,
                            dc.definition AS column_default,
                            ic.seed_value,
                            ic.increment_value
                        FROM sys.columns c
                        INNER JOIN sys.tables t
                            ON c.object_id = t.object_id
                        INNER JOIN sys.schemas s
                            ON t.schema_id = s.schema_id
                        INNER JOIN sys.types ty
                            ON c.user_type_id = ty.user_type_id
                        LEFT JOIN sys.default_constraints dc
                            ON c.default_object_id = dc.object_id
                        LEFT JOIN sys.identity_columns ic
                            ON c.object_id = ic.object_id
                           AND c.column_id = ic.column_id
                        WHERE s.name = ?
                          AND t.name = ?
                        ORDER BY c.column_id
                        """,
                        (schema, base_table_name),
                    )
                    columns = cursor.fetchall()
                    if columns:
                        chosen_schema = schema
                        break
                if not columns:
                    cursor.execute(
                        """
                        SELECT
                            s.name AS schema_name,
                            c.name AS column_name,
                            ty.name AS data_type,
                            c.max_length,
                            c.precision,
                            c.scale,
                            c.is_nullable,
                            c.is_identity,
                            c.is_computed,
                            dc.definition AS column_default,
                            ic.seed_value,
                            ic.increment_value
                        FROM sys.columns c
                        INNER JOIN sys.tables t
                            ON c.object_id = t.object_id
                        INNER JOIN sys.schemas s
                            ON t.schema_id = s.schema_id
                        INNER JOIN sys.types ty
                            ON c.user_type_id = ty.user_type_id
                        LEFT JOIN sys.default_constraints dc
                            ON c.default_object_id = dc.object_id
                        LEFT JOIN sys.identity_columns ic
                            ON c.object_id = ic.object_id
                           AND c.column_id = ic.column_id
                        WHERE t.name = ?
                        ORDER BY s.name, t.name, c.column_id
                        """,
                        (base_table_name,),
                    )
                    all_rows = cursor.fetchall()
                    if not all_rows:
                        raise Exception(f"Could not locate SQL Server table: {object_name}")
                    if explicit_schema:
                        filtered_rows = [row[1:] for row in all_rows if str(row[0]).lower() == str(explicit_schema).lower()]
                        if filtered_rows:
                            columns = filtered_rows
                            chosen_schema = explicit_schema
                        else:
                            columns = [row[1:] for row in all_rows]
                            chosen_schema = all_rows[0][0]
                    else:
                        columns = [row[1:] for row in all_rows if str(row[0]).lower() == str(details.get("schema") or "dbo").lower()]
                        if columns:
                            chosen_schema = details.get("schema") or "dbo"
                        else:
                            columns = [row[1:] for row in all_rows]
                            chosen_schema = all_rows[0][0]
                if not columns:
                    raise Exception(f"Could not locate SQL Server table: {object_name}")

                cursor.execute(
                    """
                    SELECT c.name AS column_name
                    FROM sys.indexes i
                    INNER JOIN sys.index_columns ic
                        ON i.object_id = ic.object_id
                       AND i.index_id = ic.index_id
                    INNER JOIN sys.columns c
                        ON ic.object_id = c.object_id
                       AND ic.column_id = c.column_id
                    INNER JOIN sys.tables t
                        ON i.object_id = t.object_id
                    INNER JOIN sys.schemas s
                        ON t.schema_id = s.schema_id
                    WHERE i.is_primary_key = 1
                      AND s.name = ?
                      AND t.name = ?
                    ORDER BY ic.key_ordinal
                    """,
                    (chosen_schema or (details.get("schema") or "dbo"), base_table_name),
                )
                primary_keys = [row[0] for row in cursor.fetchall()]

            def format_type(row):
                data_type = str(row[1] or "").upper()
                max_length = row[2]
                precision = row[3]
                scale = row[4]
                if data_type in {"VARCHAR", "CHAR", "NVARCHAR", "NCHAR", "VARBINARY", "BINARY"}:
                    if max_length == -1:
                        return f"{data_type}(MAX)"
                    length = max_length
                    if data_type in {"NVARCHAR", "NCHAR"} and length not in (None, -1):
                        length = int(length // 2)
                    return f"{data_type}({int(length)})"
                if data_type in {"DECIMAL", "NUMERIC"}:
                    if precision is not None and scale is not None:
                        return f"{data_type}({int(precision)},{int(scale)})"
                if data_type in {"DATETIME2", "TIME", "DATETIMEOFFSET"}:
                    if precision is not None:
                        return f"{data_type}({int(precision)})"
                return data_type

            def normalize_default(default_text):
                if default_text is None:
                    return None
                text = str(default_text).strip()
                if text.startswith("(") and text.endswith(")"):
                    text = text[1:-1].strip()
                return text

            lines = []
            for row in columns:
                column_name = row[0]
                data_type = format_type(row)
                is_nullable = row[5]
                is_identity = row[6]
                is_computed = row[7]
                column_default = normalize_default(row[8])
                if is_computed:
                    continue
                line = f"[{column_name}] {data_type}"
                if is_identity:
                    seed_value = int(row[9] or 1)
                    increment_value = int(row[10] or 1)
                    line += f" IDENTITY({seed_value},{increment_value})"
                if column_default:
                    line += f" DEFAULT {column_default}"
                if str(is_nullable).upper() == "NO":
                    line += " NOT NULL"
                lines.append(line)

            if primary_keys:
                pk_columns = ", ".join(f"[{name}]" for name in primary_keys)
                lines.append(f"CONSTRAINT [PK_{base_table_name}] PRIMARY KEY ({pk_columns})")

                return "CREATE TABLE [{schema}].[{table}] (\n  {body}\n)".format(
                    schema=chosen_schema or (details.get("schema") or "dbo"),
                    table=base_table_name,
                    body=",\n  ".join(lines),
                )
            raise Exception(f"Unsupported extraction for SQL Server {object_type}.")
        except Exception as error:
            raise Exception(
                f"SQL Server {object_type} metadata extraction failed for {object_name}: {error}"
            ) from error

    def quote_identifier(self, identifier):
        return f"[{identifier}]"

    def placeholder(self):
        return "?"

    def get_primary_keys(
        self, cursor, table_name, connection_details=None, schema_name=None
    ):
        schema = schema_name or (connection_details or {}).get("schema") or "dbo"
        base_table_name = table_name.split(".")[-1]
        cursor.execute(
            """
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
              ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
             AND tc.TABLE_SCHEMA = c.TABLE_SCHEMA
             AND tc.TABLE_NAME = c.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND tc.TABLE_SCHEMA = ?
              AND tc.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
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

    def preflight_validate_sql(
        self, cursor, sql_text, object_type, object_name, connection_details=None
    ):
        try:
            cursor.execute("SET NOEXEC ON")
            cursor.execute(sql_text)
            return {
                "status": "success",
                "message": "SQL Server preflight validation passed.",
            }
        except Exception as error:
            return {"status": "error", "message": str(error)}
        finally:
            try:
                cursor.execute("SET NOEXEC OFF")
            except Exception:
                pass

    def ensure_database_and_schema(
        self, connection, cursor, connection_details=None
    ):
        details = connection_details or {}
        database = details.get("database")
        schema = details.get("schema")
        if database:
            escaped_database = database.replace("'", "''")
            cursor.execute(
                f"IF DB_ID('{escaped_database}') IS NULL CREATE DATABASE {self.quote_identifier(database)}"
            )
            cursor.execute(f"USE {self.quote_identifier(database)}")
        if schema and schema.lower() != "dbo":
            escaped_schema = schema.replace("'", "''")
            cursor.execute(
                f"IF SCHEMA_ID('{escaped_schema}') IS NULL EXEC('CREATE SCHEMA {self.quote_identifier(schema)}')"
            )
        return {
            "status": "success",
            "message": "SQL Server database/schema bootstrap completed.",
            "reconnect_required": False,
        }
