import snowflake.connector
import re
from snowflake.connector.errors import Error as SnowflakeError

from app.adapters.base import DatabaseAdapter
from app.adapters.common import empty_object_summary, normalize_snowflake_account, snowflake_names


def build_snowflake_auth_error(error, details, account):
    authenticator = details.get("authenticator") or "snowflake"
    username = details.get("username", "")
    hints = [
        f"Snowflake authentication failed for user '{username}' on account '{account}'.",
        "Verify the account identifier format. Use the account locator form like 'org-account' or 'xy12345.eu-north-1.aws', not an arbitrary URL alias.",
        "Verify the Snowflake login name exactly, including case if your org uses a different login name than the display name.",
        f"Current authenticator: '{authenticator}'. If your org uses SSO/MFA, set authenticator to 'externalbrowser' or your Okta URL.",
        "If credentials are correct, check whether a network policy or IP allowlist is blocking this client.",
    ]
    return f"{error}. " + " ".join(hints)


class SnowflakeAdapter(DatabaseAdapter):
    engine_name = "Snowflake"

    def connect(self, details):
        account = normalize_snowflake_account(details.get("account") or details.get("host"))
        connect_kwargs = {
            "account": account,
            "user": details["username"],
            "password": details["password"],
            "warehouse": details.get("warehouse"),
            "database": details.get("database"),
            "schema": details.get("schema"),
            "role": details.get("role"),
        }
        authenticator = details.get("authenticator")
        if authenticator:
            connect_kwargs["authenticator"] = authenticator
        try:
            return snowflake.connector.connect(**connect_kwargs)
        except TypeError as error:
            raise Exception(
                "Snowflake connector failed during authentication setup. "
                "The connector entered an SSO/Okta flow without a valid IdP response. "
                "Use standard username/password only, or provide a valid Snowflake SSO authenticator."
            ) from error
        except SnowflakeError as error:
            raise Exception(build_snowflake_auth_error(error, details, account)) from error

    def list_databases(self, cursor, details=None):
        cursor.execute("SHOW DATABASES")
        return snowflake_names(cursor.fetchall())

    def _fetch_named_rows(self, cursor):
        columns = [description[0].lower() for description in cursor.description or []]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def _format_routine_signature(self, row):
        name = str(row.get("name") or "").strip()
        arguments = str(row.get("arguments") or "").strip()
        if not name:
            return ""
        if not arguments:
            return name
        signature = arguments.split(" RETURN ", 1)[0].strip()
        if signature.upper().startswith(name.upper()):
            return signature
        return f"{name}{signature}"

    def _get_stream_names(self, cursor, database_name, schema_name):
        cursor.execute(f"SHOW STREAMS IN SCHEMA {database_name}.{schema_name}")
        return snowflake_names(cursor.fetchall())

    def _get_task_names(self, cursor, database_name, schema_name):
        cursor.execute(f"SHOW TASKS IN SCHEMA {database_name}.{schema_name}")
        return snowflake_names(cursor.fetchall())

    def _extract_referenced_streams(self, task_ddl, stream_names):
        normalized_task_ddl = str(task_ddl or "").upper()
        return [
            stream_name
            for stream_name in stream_names
            if stream_name and stream_name.upper() in normalized_task_ddl
        ]

    def _qualify_routine_name(self, object_name, connection_details=None):
        details = connection_details or {}
        match = re.match(r"^(?P<name>[^()]+?)(?P<signature>\(.*\))$", object_name.strip())
        if not match:
            return self.qualify_table_name(object_name, connection_details)

        routine_name = match.group("name").strip()
        signature = match.group("signature").strip()
        database = details.get("database")
        schema = details.get("schema")
        qualified_parts = []
        if database:
            qualified_parts.append(self._format_identifier(database))
        if schema:
            qualified_parts.append(self._format_identifier(schema))
        qualified_parts.append(self._format_identifier(routine_name))
        return ".".join(qualified_parts) + signature

    def list_schemas(self, cursor, database_name, details=None):
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database_name}")
        return snowflake_names(cursor.fetchall())

    def get_object_summary(self, cursor, database_name, schema_name):
        summary = empty_object_summary()
        cursor.execute(f"SHOW TERSE TABLES IN SCHEMA {database_name}.{schema_name}")
        summary["table"] = len(cursor.fetchall())
        cursor.execute(f"SHOW TERSE VIEWS IN SCHEMA {database_name}.{schema_name}")
        summary["view"] = len(cursor.fetchall())
        cursor.execute(f"SHOW USER PROCEDURES IN SCHEMA {database_name}.{schema_name}")
        summary["storedprocedure"] = len(cursor.fetchall())
        cursor.execute(f"SHOW USER FUNCTIONS IN SCHEMA {database_name}.{schema_name}")
        summary["function"] = len(cursor.fetchall())
        summary["trigger"] = len(self._get_task_names(cursor, database_name, schema_name))
        return summary

    def list_objects(self, cursor, database_name, schema_name, object_type):
        if object_type == "table":
            cursor.execute(f"SHOW TERSE TABLES IN SCHEMA {database_name}.{schema_name}")
        elif object_type == "view":
            cursor.execute(f"SHOW TERSE VIEWS IN SCHEMA {database_name}.{schema_name}")
        elif object_type == "storedprocedure":
            cursor.execute(f"SHOW USER PROCEDURES IN SCHEMA {database_name}.{schema_name}")
            return [
                signature
                for signature in (
                    self._format_routine_signature(row)
                    for row in self._fetch_named_rows(cursor)
                )
                if signature
            ]
        elif object_type == "function":
            cursor.execute(f"SHOW USER FUNCTIONS IN SCHEMA {database_name}.{schema_name}")
            return [
                signature
                for signature in (
                    self._format_routine_signature(row)
                    for row in self._fetch_named_rows(cursor)
                )
                if signature
            ]
        elif object_type == "trigger":
            return self._get_task_names(cursor, database_name, schema_name)
        else:
            return []
        return snowflake_names(cursor.fetchall())

    def extract_ddl(self, cursor, object_name, object_type, connection_details=None):
        object_type_map = {
            "table": "TABLE",
            "view": "VIEW",
            "storedprocedure": "PROCEDURE",
            "function": "FUNCTION",
        }
        if object_type == "trigger":
            details = connection_details or {}
            database_name = details.get("database")
            schema_name = details.get("schema")
            if not database_name or not schema_name:
                raise Exception(
                    "Snowflake trigger extraction requires database and schema."
                )
            task_name = object_name.split(".")[-1]
            qualified_task_name = self.qualify_table_name(task_name, connection_details)
            cursor.execute("SELECT GET_DDL('TASK', %s)", (qualified_task_name,))
            task_row = cursor.fetchone()
            if not task_row:
                raise Exception(
                    f"Could not locate Snowflake task for trigger migration: {qualified_task_name}"
                )
            task_ddl = task_row[0]
            stream_ddls = []
            for stream_name in self._extract_referenced_streams(
                task_ddl,
                self._get_stream_names(cursor, database_name, schema_name),
            ):
                qualified_stream_name = self.qualify_table_name(
                    stream_name,
                    connection_details,
                )
                cursor.execute("SELECT GET_DDL('STREAM', %s)", (qualified_stream_name,))
                stream_row = cursor.fetchone()
                if stream_row:
                    stream_ddls.append(stream_row[0])
            bundle = stream_ddls + [task_ddl]
            return "\n-- SQL_BUNDLE_DELIMITER --\n".join(bundle)
        snowflake_type = object_type_map.get(object_type)
        if not snowflake_type:
            raise Exception(f"Unsupported extraction for Snowflake {object_type}.")
        if object_type in {"storedprocedure", "function"}:
            qualified_name = self._qualify_routine_name(
                object_name, connection_details
            )
        else:
            qualified_name = self.qualify_table_name(object_name, connection_details)
        cursor.execute(f"SELECT GET_DDL('{snowflake_type}', %s)", (qualified_name,))
        row = cursor.fetchone()
        if row:
            return row[0]
        raise Exception(f"Could not locate Snowflake {object_type}: {qualified_name}")

    def _format_identifier(self, identifier):
        if identifier.startswith('"') and identifier.endswith('"'):
            return identifier
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", identifier):
            return identifier.upper()
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'

    def quote_identifier(self, identifier):
        return self._format_identifier(identifier)

    def placeholder(self):
        return "%s"

    def get_primary_keys(
        self, cursor, table_name, connection_details=None, schema_name=None
    ):
        qualified_table = self.qualify_table_name(table_name, connection_details)
        cursor.execute(f"SHOW PRIMARY KEYS IN TABLE {qualified_table}")
        rows = self._fetch_named_rows(cursor)
        primary_keys = []
        for row in rows:
            column_name = row.get("column_name")
            if column_name:
                primary_keys.append(str(column_name))
        return primary_keys

    def qualify_table_name(self, table_name, connection_details=None):
        details = connection_details or {}
        if "." in table_name:
            parts = table_name.split(".")
            return ".".join(self._format_identifier(part) for part in parts)
        database = details.get("database")
        schema = details.get("schema")
        if database and schema:
            return ".".join(
                [
                    self._format_identifier(database),
                    self._format_identifier(schema),
                    self._format_identifier(table_name),
                ]
            )
        if schema:
            return ".".join(
                [self._format_identifier(schema), self._format_identifier(table_name)]
            )
        return self._format_identifier(table_name)

    def ensure_database_and_schema(
        self, connection, cursor, connection_details=None
    ):
        details = connection_details or {}
        database = details.get("database")
        schema = details.get("schema")
        if database:
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS {self.quote_identifier(database)}"
            )
            cursor.execute(f"USE DATABASE {self.quote_identifier(database)}")
        if schema:
            cursor.execute(
                f"CREATE SCHEMA IF NOT EXISTS {self.quote_identifier(schema)}"
            )
            cursor.execute(f"USE SCHEMA {self.quote_identifier(schema)}")
        return {
            "status": "success",
            "message": "Snowflake database/schema bootstrap completed.",
            "reconnect_required": False,
        }
