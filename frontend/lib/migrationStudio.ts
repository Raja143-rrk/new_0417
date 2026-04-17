export interface ConnectionProfile {
  id: string;
  name: string;
  engine: string;
  fields: Record<string, string>;
}

export interface ApiResponse<T> {
  status?: string;
  message?: string;
  items?: T;
}

export interface SessionInfo {
  isAuthenticated: boolean;
  username: string;
  roleLabel: string;
  role: string;
  avatar: string;
}

export interface SideSelectionState {
  database: string;
  schema: string;
  objectType: string;
  objectName: string;
}

export interface SideMetadataState {
  databases: string[];
  schemas: string[];
  objectSummary: Record<string, number>;
  objects: string[];
  loadingDatabases: boolean;
  loadingSchemas: boolean;
  loadingObjects: boolean;
}

export interface LogEntry {
  id: number;
  tone: 'info' | 'success' | 'error' | 'warn' | 'dim' | 'blue';
  text: string;
  timestamp: string;
}

export interface ResumeCheckpoint {
  object_type: string;
  object_name: string;
  run_id?: string;
}

export interface RunSummary {
  run_id?: string;
  status?: string;
  source_db?: string;
  target_db?: string;
  started_at?: string;
  completed_at?: string;
  stats?: {
    total_objects?: number;
    success_objects?: number;
    error_objects?: number;
    skipped_objects?: number;
    total_rows_migrated?: number;
    by_type?: Record<string, { total?: number; success?: number; error?: number; skipped?: number }>;
  };
}

export interface StreamFinalResult {
  status?: string;
  message?: string;
  output_sql?: string;
  validation?: {
    is_valid?: boolean;
    errors?: string[];
    warnings?: string[];
  };
  suggestions?: Array<{
    name?: string;
    reason?: string;
    description?: string;
    pattern?: string;
    replacement?: string;
    approved?: boolean;
  }>;
  source?: string;
  run_summary?: RunSummary;
  resume_checkpoint?: ResumeCheckpoint | null;
  summary?: {
    total?: number;
    success?: number;
    error?: number;
    skipped?: number;
  };
  object_result?: {
    object_name?: string;
    object_type?: string;
    status?: string;
    rows_migrated?: number;
    transformed_sql?: string;
  };
  results?: Array<{
    object_name?: string;
    object_type?: string;
    status?: string;
    transformed_sql?: string;
  }>;
  transformed_queries?: Array<{
    object_name?: string;
    object_type?: string;
    transformed_sql?: string;
  }>;
}

export interface RagAgentStatus {
  configured: boolean;
  provider: string;
  model: string;
}

export const SOURCE_DEFAULT_VALUE = '__SOURCE_DEFAULT__';
export const MULTI_SELECT_VALUE = '__MULTI_SELECT__';

export const OBJECT_TYPE_LABELS: Record<string, string> = {
  table: 'Tables',
  view: 'Views',
  storedprocedure: 'Stored Procedures',
  function: 'Functions',
  trigger: 'Triggers',
  event: 'Events',
  sequence: 'Sequences',
  synonym: 'Synonyms',
  cursor: 'Cursors',
};

export function createEmptyMetadata(): SideMetadataState {
  return {
    databases: [],
    schemas: [],
    objectSummary: {},
    objects: [],
    loadingDatabases: false,
    loadingSchemas: false,
    loadingObjects: false,
  };
}

export function createEmptySelection(): SideSelectionState {
  return {
    database: '',
    schema: '',
    objectType: '',
    objectName: '',
  };
}

export function formatObjectTypeLabel(type: string, count?: number): string {
  const base = OBJECT_TYPE_LABELS[type] || type;
  return typeof count === 'number' ? `${base} (${count})` : base;
}

export function resolveTargetValue(rawValue: string, sourceValue: string): string {
  if (rawValue === SOURCE_DEFAULT_VALUE) {
    return sourceValue;
  }
  return rawValue;
}

export function formatDateTime(value?: string): string {
  if (!value) return '--';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

export function formatDuration(startedAt?: string, completedAt?: string): string {
  if (!startedAt || !completedAt) return '--';
  const start = new Date(startedAt).getTime();
  const end = new Date(completedAt).getTime();
  if (Number.isNaN(start) || Number.isNaN(end) || end < start) return '--';
  const totalSeconds = Math.floor((end - start) / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}m ${seconds}s`;
}

export function inferLogTone(text: string): LogEntry['tone'] {
  const normalized = text.toLowerCase();
  if (normalized.includes('error') || normalized.includes('failed')) return 'error';
  if (normalized.includes('success') || normalized.includes('completed')) return 'success';
  if (normalized.includes('warn') || normalized.includes('retry') || normalized.includes('stopped')) return 'warn';
  if (normalized.includes('[system]')) return 'dim';
  if (normalized.includes('[migration]')) return 'blue';
  return 'info';
}

export function formatLogTimestamp(date: Date): string {
  return date.toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

export function normalizeLogPayload(text: string): { timestamp: string; text: string } {
  const raw = String(text || '');
  const match = raw.match(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+-\s+(.*)$/);
  if (!match) {
    return {
      timestamp: formatLogTimestamp(new Date()),
      text: raw,
    };
  }

  const serverDate = new Date(match[1].replace(' ', 'T'));
  return {
    timestamp: Number.isNaN(serverDate.getTime()) ? match[1] : formatLogTimestamp(serverDate),
    text: match[2],
  };
}

export function buildActionState(params: {
  srcConnected: boolean;
  tgtConnected: boolean;
  srcDatabase: string;
  srcSchema: string;
  tgtDatabase: string;
  tgtSchema: string;
  bulkMode: boolean;
  multiSelectMode: boolean;
  selectedObjectType: string;
  selectedObjectName: string;
  selectedBulkTypeCount: number;
  selectedObjectCount: number;
  migrationActive: boolean;
  stopRequested: boolean;
}) {
  if (!params.srcConnected || !params.tgtConnected) {
    return {
      title: 'Configure Connections',
      subtitle: 'Connect both source and target databases to enable migration.',
    };
  }
  if (!params.srcDatabase || !params.srcSchema || !params.tgtDatabase || !params.tgtSchema) {
    return {
      title: 'Complete Metadata Selection',
      subtitle: 'Choose source and target database and schema values before executing a migration.',
    };
  }
  if (params.bulkMode && !params.selectedBulkTypeCount) {
    return {
      title: 'Choose Bulk Object Types',
      subtitle: 'Select at least one object-type checkbox for Migrate All.',
    };
  }
  if (!params.bulkMode && (!params.selectedObjectType || !params.selectedObjectName || (params.multiSelectMode && !params.selectedObjectCount))) {
    return {
      title: 'Select Single Object',
      subtitle: params.multiSelectMode
        ? 'Choose one or more object names from the checkbox list.'
        : 'Choose one source object type and one object name for single migration.',
    };
  }
  if (params.migrationActive) {
    return {
      title: 'Migration In Progress',
      subtitle: params.stopRequested
        ? 'Stop request sent. The backend will stop after the current safe checkpoint.'
        : 'Streaming backend logs and status in real time.',
    };
  }
  return {
    title: params.bulkMode
      ? 'Ready for Bulk Migration'
      : params.multiSelectMode
        ? 'Ready for Multi-Object Migration'
        : 'Ready to Migrate',
    subtitle: params.bulkMode
      ? 'Both connections are active. Run the migrate-all workflow using backend execution order.'
      : params.multiSelectMode
        ? `Both connections are active. ${params.selectedObjectCount} selected object(s) from the current type will be migrated.`
        : 'Both connections are active. Run the single-object migration.',
  };
}
