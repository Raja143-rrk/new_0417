from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class AdapterCapabilities:
    can_connect: bool = True
    can_list_databases: bool = True
    can_list_schemas: bool = True
    can_list_objects: bool = True
    can_extract_ddl: bool = True
    can_migrate_data: bool = True


class DatabaseAdapter(ABC):
    engine_name = ""
    capabilities = AdapterCapabilities()

    @abstractmethod
    def connect(self, details):
        raise NotImplementedError

    @abstractmethod
    def list_databases(self, cursor, details=None):
        raise NotImplementedError

    @abstractmethod
    def list_schemas(self, cursor, database_name, details=None):
        raise NotImplementedError

    @abstractmethod
    def get_object_summary(self, cursor, database_name, schema_name):
        raise NotImplementedError

    @abstractmethod
    def list_objects(self, cursor, database_name, schema_name, object_type):
        raise NotImplementedError

    @abstractmethod
    def extract_ddl(self, cursor, object_name, object_type, connection_details=None):
        raise NotImplementedError

    @abstractmethod
    def quote_identifier(self, identifier):
        raise NotImplementedError

    @abstractmethod
    def placeholder(self):
        raise NotImplementedError

    def get_primary_keys(
        self, cursor, table_name, connection_details=None, schema_name=None
    ):
        raise NotImplementedError(
            f"Primary key lookup is not implemented for {self.engine_name}."
        )

    def preflight_validate_sql(
        self, cursor, sql_text, object_type, object_name, connection_details=None
    ):
        return {
            "status": "skipped",
            "message": (
                f"Preflight validation is not implemented for {self.engine_name} "
                f"{object_type} objects."
            ),
        }

    def ensure_database_and_schema(
        self, connection, cursor, connection_details=None
    ):
        return {
            "status": "skipped",
            "message": (
                f"Database/schema bootstrap is not implemented for {self.engine_name}."
            ),
            "reconnect_required": False,
        }

    def qualify_table_name(self, table_name, connection_details=None):
        if "." in table_name:
            parts = table_name.split(".")
            return ".".join(self.quote_identifier(part) for part in parts)
        return self.quote_identifier(table_name)
