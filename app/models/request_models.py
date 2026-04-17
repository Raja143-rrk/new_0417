from typing import Any, Literal

from pydantic import BaseModel


class AgentMigrationRequest(BaseModel):
    source_config: dict
    target_config: dict
    object_type: str
    object_name: str
    migrate_data: bool = False
    data_only: bool = False
    data_migration_mode: str = "insert"
    data_batch_size: int = 10000
    data_execution_engine: Literal["auto", "python", "spark"] = "auto"
    spark_row_threshold: int = 1000
    validate_row_counts: bool = True
    spark_options: dict[str, Any] | None = None
    truncate_before_load: bool = False
    drop_and_create_if_exists: bool = False
    show_transformed_queries: bool = False


class SqlMigrationRequest(BaseModel):
    input_sql: str
    source: str
    target: str
    object_type: str = "table"
    object_name: str = ""


class BulkAgentMigrationRequest(BaseModel):
    source_config: dict
    target_config: dict
    object_types: list[str]
    selected_objects: dict[str, list[str]] | None = None
    migrate_data: bool = False
    data_only: bool = False
    data_migration_mode: str = "insert"
    data_batch_size: int = 10000
    data_execution_engine: Literal["auto", "python", "spark"] = "auto"
    spark_row_threshold: int = 1000
    validate_row_counts: bool = True
    spark_options: dict[str, Any] | None = None
    truncate_before_load: bool = False
    drop_and_create_if_exists: bool = False
    show_transformed_queries: bool = False
    continue_on_error: bool = True
    resume_from: dict | None = None


class ConnectionTestRequest(BaseModel):
    database_type: str
    connection_details: dict


class MetadataRequest(BaseModel):
    database_type: str
    connection_details: dict
    database_name: str | None = None
    schema_name: str | None = None
    object_type: str | None = None


class CapabilityRequest(BaseModel):
    database_type: str


ScheduleTriggerType = Literal["event_trigger", "scheduled_trigger"]


class JobScheduleRequest(BaseModel):
    trigger_type: ScheduleTriggerType
    enabled: bool = True
    description: str | None = None
    timezone: str = "Asia/Calcutta"
    cron_expression: str | None = None
    start_at: str | None = None
    event_name: str | None = None


UserRole = Literal["admin", "operator", "viewer"]


class LoginRequest(BaseModel):
    username: str
    password: str


class CreateUserRequest(BaseModel):
    username: str
    password: str
    email: str
    role: UserRole
    actor_role: UserRole
    invited_by: str | None = None


class UpdateUserRequest(BaseModel):
    email: str
    role: UserRole
    actor_role: UserRole


class AIChatRequest(BaseModel):
    message: str
    sql: str | None = None
    object_type: str | None = None
    source: str | None = None
    target: str | None = None


class ConnectionProfileRequest(BaseModel):
    id: str | None = None
    name: str
    engine: str
    fields: dict[str, str]
    actor_role: UserRole
    actor_username: str | None = None
