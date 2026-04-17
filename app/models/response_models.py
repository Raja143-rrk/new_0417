from datetime import datetime
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field


MigrationStatus = Literal["success", "error", "skipped", "partial", "stopped"]
ScheduleTriggerType = Literal["event_trigger", "scheduled_trigger"]


class ObjectCounters(BaseModel):
    total: int = 0
    success: int = 0
    error: int = 0
    skipped: int = 0


class MigrationObjectStats(BaseModel):
    run_id: str
    object_name: str
    object_type: str
    status: MigrationStatus
    source_db: str
    target_db: str
    rows_migrated: int = 0
    source_row_count: int | None = None
    target_row_count: int | None = None
    missing_row_count: int | None = None
    retry_count: int = 0
    error_type: str | None = None
    error_message: str | None = None
    remediation: str | None = None
    transformed_sql: str | None = None
    started_at: datetime
    completed_at: datetime


class MigrationRunStats(BaseModel):
    total_objects: int = 0
    success_objects: int = 0
    error_objects: int = 0
    skipped_objects: int = 0
    total_rows_migrated: int = 0
    total_source_rows: int = 0
    total_target_rows: int = 0
    total_missing_rows: int = 0
    total_retries: int = 0
    by_type: dict[str, ObjectCounters] = Field(default_factory=dict)


class MigrationRunSummary(BaseModel):
    run_id: str = Field(default_factory=lambda: str(uuid4()))
    status: MigrationStatus
    source_db: str
    target_db: str
    execution_order: list[str] = Field(default_factory=list)
    stats: MigrationRunStats
    object_results: list[MigrationObjectStats] = Field(default_factory=list)
    started_at: datetime
    completed_at: datetime


class JobScheduleConfig(BaseModel):
    job_id: str
    trigger_type: ScheduleTriggerType
    enabled: bool = True
    description: str | None = None
    timezone: str = "Asia/Calcutta"
    cron_expression: str | None = None
    start_at: datetime | None = None
    event_name: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_triggered_at: datetime | None = None
    trigger_count: int = 0
    last_run_id: str | None = None
    last_run_status: MigrationStatus | None = None
    next_run_at: datetime | None = None


class MigrationJobSummary(BaseModel):
    job_id: str
    run_id: str
    job_name: str
    status: MigrationStatus
    source_db: str
    target_db: str
    task_count: int = 0
    successful_tasks: int = 0
    failed_tasks: int = 0
    skipped_tasks: int = 0
    execution_order: list[str] = Field(default_factory=list)
    started_at: datetime
    completed_at: datetime
    schedule: JobScheduleConfig | None = None
    tasks: list[MigrationObjectStats] = Field(default_factory=list)


class SqlMigrationResponse(BaseModel):
    status: str
    output_sql: str
    validation: dict
    suggestions: list[dict] = Field(default_factory=list)
    source: str
    applied_rules: list[dict] = Field(default_factory=list)
    corrected_sql: str | None = None
    original_error: str | None = None
    retry_count: int = 0
    fix_attempts: list[dict] = Field(default_factory=list)
    history_key: str | None = None


class RagAgentStatusResponse(BaseModel):
    configured: bool
    provider: str
    model: str
