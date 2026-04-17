'use client';

import { useSearchParams } from 'next/navigation';
import { useEffect, useMemo, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import {
  ApiResponse,
  ConnectionProfile,
  LogEntry,
  MULTI_SELECT_VALUE,
  RagAgentStatus,
  ResumeCheckpoint,
  RunSummary,
  SOURCE_DEFAULT_VALUE,
  SessionInfo,
  SideMetadataState,
  SideSelectionState,
  StreamFinalResult,
  buildActionState,
  createEmptyMetadata,
  createEmptySelection,
  formatDateTime,
  formatDuration,
  formatObjectTypeLabel,
  inferLogTone,
  normalizeLogPayload,
  resolveTargetValue,
} from '@/lib/migrationStudio';
import { listConnectionProfiles } from '@/lib/connectionProfiles';
import { ThemePreference, getStoredThemePreference, setThemePreference } from '@/lib/theme';
import PageHeader from '@/components/PageHeader';
import './migration-studio.css';

const API_BASE = '/api';

interface StoredMigrationConfig {
  database_type?: string;
  connection_details?: Record<string, string>;
}

interface StoredJobRequestPayload {
  source_config?: StoredMigrationConfig;
  target_config?: StoredMigrationConfig;
  object_type?: string;
  object_name?: string;
  object_types?: string[];
  selected_objects?: Record<string, string[]>;
  migrate_data?: boolean;
  data_only?: boolean;
  data_migration_mode?: string;
  data_batch_size?: number;
  truncate_before_load?: boolean;
  drop_and_create_if_exists?: boolean;
}

interface StoredHistoryRecord {
  run_summary?: RunSummary;
  job_request?: {
    mode?: 'single' | 'bulk' | string;
    payload?: StoredJobRequestPayload;
  };
}

interface HistoryItemResponse {
  status?: string;
  message?: string;
  item?: StoredHistoryRecord;
}

interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
}

async function postJson<T>(path: string, payload: Record<string, unknown>): Promise<ApiResponse<T>> {
  const response = await fetch(`${API_BASE}${path}`, {
    method: 'POST',
    cache: 'no-store',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = (await response.json().catch(() => null)) as ApiResponse<T> | null;
  if (!response.ok) {
    throw new Error(data?.message || `HTTP ${response.status}`);
  }
  return data || {};
}

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(url, { cache: 'no-store' });
  const data = (await response.json().catch(() => null)) as T | null;
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return data as T;
}

async function postJsonStream(
  path: string,
  payload: Record<string, unknown>,
  onEvent: (event: Record<string, unknown>) => void
): Promise<void> {
  const response = await fetch(`${API_BASE}${path}`, {
    method: 'POST',
    cache: 'no-store',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  if (!response.ok || !response.body) {
    const data = (await response.json().catch(() => null)) as ApiResponse<unknown> | null;
    throw new Error(data?.message || `HTTP ${response.status}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    buffer += decoder.decode(value || new Uint8Array(), { stream: !done });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      onEvent(JSON.parse(trimmed) as Record<string, unknown>);
    }

    if (done) break;
  }

  if (buffer.trim()) {
    onEvent(JSON.parse(buffer.trim()) as Record<string, unknown>);
  }
}


export default function MigrationStudioPage() {
  const searchParams = useSearchParams();
  const requestedRunId = searchParams.get('runId')?.trim() || '';
  const [mounted, setMounted] = useState(false);
  const [sessionInfo, setSessionInfo] = useState<SessionInfo | null>(null);
  const [connections, setConnections] = useState<ConnectionProfile[]>([]);
  const [srcEngine, setSrcEngine] = useState('');
  const [tgtEngine, setTgtEngine] = useState('');
  const [srcConnectionId, setSrcConnectionId] = useState('');
  const [tgtConnectionId, setTgtConnectionId] = useState('');
  const [srcConnected, setSrcConnected] = useState(false);
  const [tgtConnected, setTgtConnected] = useState(false);
  const [srcTesting, setSrcTesting] = useState(false);
  const [tgtTesting, setTgtTesting] = useState(false);
  const [srcMessage, setSrcMessage] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);
  const [tgtMessage, setTgtMessage] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);
  const [srcSelection, setSrcSelection] = useState<SideSelectionState>(createEmptySelection());
  const [tgtSelection, setTgtSelection] = useState<SideSelectionState>(createEmptySelection());
  const [srcMetadata, setSrcMetadata] = useState<SideMetadataState>(createEmptyMetadata());
  const [tgtMetadata, setTgtMetadata] = useState<SideMetadataState>(createEmptyMetadata());
  const [srcMigrateAll, setSrcMigrateAll] = useState(false);
  const [srcBulkTypes, setSrcBulkTypes] = useState<string[]>([]);
  const [srcSelectedObjects, setSrcSelectedObjects] = useState<string[]>([]);
  const [srcMigrateData, setSrcMigrateData] = useState(false);
  const [srcDataOnly, setSrcDataOnly] = useState(false);
  const [srcDataMode, setSrcDataMode] = useState('insert');
  const [srcDataBatchSize, setSrcDataBatchSize] = useState('1000');
  const [tgtTruncateBeforeLoad, setTgtTruncateBeforeLoad] = useState(false);
  const [tgtDropAndCreateIfExists, setTgtDropAndCreateIfExists] = useState(false);
  const [migrationActive, setMigrationActive] = useState(false);
  const [migrationRunId, setMigrationRunId] = useState('');
  const [stopRequested, setStopRequested] = useState(false);
  const [resumeCheckpoint, setResumeCheckpoint] = useState<ResumeCheckpoint | null>(null);
  const [logEntries, setLogEntries] = useState<LogEntry[]>([]);
  const [latestRunSummary, setLatestRunSummary] = useState<RunSummary | null>(null);
  const [latestSuggestions, setLatestSuggestions] = useState<NonNullable<StreamFinalResult['suggestions']>>([]);
  const [chatPrompt, setChatPrompt] = useState('');
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [chatLoading, setChatLoading] = useState(false);
  const [isChatOpen, setIsChatOpen] = useState(false);
  const [ragAgentStatus, setRagAgentStatus] = useState<RagAgentStatus | null>(null);
  const [preloadMessage, setPreloadMessage] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const [themeMenuOpen, setThemeMenuOpen] = useState(false);
  const [themePreference, setThemePreferenceState] = useState<ThemePreference>('auto');
  const logBodyRef = useRef<HTMLDivElement | null>(null);
  const chatMessagesEndRef = useRef<HTMLDivElement | null>(null);
  const logCounterRef = useRef(0);
  const hydratedRunIdRef = useRef('');

  useEffect(() => {
    const isAuth = sessionStorage.getItem('dbm_auth') === 'true';
    if (!isAuth) {
      window.location.replace('/login');
      return;
    }

    const username = sessionStorage.getItem('dbm_user') || 'user';
    const role = sessionStorage.getItem('dbm_role') || 'viewer';
    const roleLabels: Record<string, string> = {
      admin: 'Migration Admin',
      operator: 'Migration Operator',
      viewer: 'Viewer',
    };

    setSessionInfo({
      isAuthenticated: true,
      username,
      roleLabel: roleLabels[role] || 'Viewer',
      role,
      avatar: username.substring(0, 2).toUpperCase(),
    });

    setMounted(true);
    setThemePreferenceState(getStoredThemePreference());
  }, []);

  useEffect(() => {
    if (!mounted || !sessionInfo?.role) return;

    let cancelled = false;

    const loadConnectionProfiles = async () => {
      try {
        const items = await listConnectionProfiles(sessionInfo.role as 'admin' | 'operator' | 'viewer');
        if (!cancelled) {
          setConnections(items);
        }
      } catch (error) {
        if (!cancelled) {
          console.error('Failed to load connection profiles', error);
          setPreloadMessage({
            type: 'err',
            text: error instanceof Error ? error.message : 'Could not load connection profiles.',
          });
        }
      }
    };

    void loadConnectionProfiles();

    return () => {
      cancelled = true;
    };
  }, [mounted, sessionInfo]);

  useEffect(() => {
    if (!userMenuOpen) {
      setThemeMenuOpen(false);
    }
  }, [userMenuOpen]);

  useEffect(() => {
    if (!mounted) return;

    let cancelled = false;

    const loadRagAgentStatus = async () => {
      try {
        const result = await getJson<RagAgentStatus>(`${API_BASE}/rag-agent/status`);
        if (!cancelled) {
          setRagAgentStatus(result);
        }
      } catch (error) {
        if (!cancelled) {
          console.error('Failed to load RAG agent status', error);
          setRagAgentStatus({
            configured: false,
            provider: 'unavailable',
            model: '',
          });
        }
      }
    };

    void loadRagAgentStatus();

    return () => {
      cancelled = true;
    };
  }, [mounted]);

  useEffect(() => {
    if (!srcMigrateData && srcDataOnly) {
      setSrcMigrateData(true);
    }
  }, [srcDataOnly, srcMigrateData]);

  const bulkMode = srcMigrateAll;
  const supportsDataMovement = useMemo(() => {
    if (bulkMode) {
      return srcBulkTypes.some((type) => type === 'table' || type === 'view');
    }
    return srcSelection.objectType === 'table' || srcSelection.objectType === 'view';
  }, [bulkMode, srcBulkTypes, srcSelection.objectType]);

  const multiSelectMode = useMemo(
    () => !bulkMode && srcSelection.objectName === MULTI_SELECT_VALUE,
    [bulkMode, srcSelection.objectName]
  );

  useEffect(() => {
    if (supportsDataMovement) return;
    setSrcMigrateData(false);
    setSrcDataOnly(false);
    setSrcDataMode('insert');
    setSrcDataBatchSize('1000');
  }, [supportsDataMovement]);

  useEffect(() => {
    if (!logBodyRef.current) return;
    logBodyRef.current.scrollTop = logBodyRef.current.scrollHeight;
  }, [logEntries]);

  useEffect(() => {
    chatMessagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, chatLoading]);

  useEffect(() => {
    if (!srcMessage) return;
    const timer = window.setTimeout(() => setSrcMessage(null), 5000);
    return () => window.clearTimeout(timer);
  }, [srcMessage]);

  useEffect(() => {
    if (!tgtMessage) return;
    const timer = window.setTimeout(() => setTgtMessage(null), 5000);
    return () => window.clearTimeout(timer);
  }, [tgtMessage]);

  useEffect(() => {
    if (!preloadMessage) return;
    const timer = window.setTimeout(() => setPreloadMessage(null), 7000);
    return () => window.clearTimeout(timer);
  }, [preloadMessage]);

  const srcConnections = useMemo(
    () => connections.filter((connection) => connection.engine === srcEngine),
    [connections, srcEngine]
  );
  const tgtConnections = useMemo(
    () => connections.filter((connection) => connection.engine === tgtEngine),
    [connections, tgtEngine]
  );

  const srcObjectTypes = useMemo(
    () =>
      Object.entries(srcMetadata.objectSummary)
        .filter(([, count]) => Number(count) > 0)
        .map(([type, count]) => ({ type, count: Number(count) })),
    [srcMetadata.objectSummary]
  );

  const tgtObjectTypes = useMemo(
    () =>
      Object.entries(tgtMetadata.objectSummary)
        .filter(([, count]) => Number(count) > 0)
        .map(([type, count]) => ({ type, count: Number(count) })),
    [tgtMetadata.objectSummary]
  );

  const resolvedTgtDatabase = useMemo(
    () => resolveTargetValue(tgtSelection.database, srcSelection.database),
    [tgtSelection.database, srcSelection.database]
  );

  const resolvedTgtSchema = useMemo(
    () => resolveTargetValue(tgtSelection.schema, srcSelection.schema),
    [tgtSelection.schema, srcSelection.schema]
  );

  const dataOptionsEnabled = supportsDataMovement && (srcMigrateData || srcDataOnly);
  const canResume = Boolean(resumeCheckpoint) && !migrationActive;
  const canProceed = useMemo(() => {
    if (migrationActive) return false;
    if (!srcConnected || !tgtConnected) return false;
    if (!srcSelection.database || !srcSelection.schema || !resolvedTgtDatabase || !resolvedTgtSchema) return false;
    if (bulkMode) return srcBulkTypes.length > 0;
    if (multiSelectMode) {
      return Boolean(srcSelection.objectType && srcSelection.objectName && srcSelectedObjects.length);
    }
    return Boolean(srcSelection.objectType && srcSelection.objectName);
  }, [
    bulkMode,
    migrationActive,
    multiSelectMode,
    resolvedTgtDatabase,
    resolvedTgtSchema,
    srcBulkTypes.length,
    srcConnected,
    srcSelection.database,
    srcSelection.objectName,
    srcSelection.objectType,
    srcSelection.schema,
    srcSelectedObjects.length,
    tgtConnected,
  ]);

  const actionState = useMemo(
    () =>
      buildActionState({
        srcConnected,
        tgtConnected,
        srcDatabase: srcSelection.database,
        srcSchema: srcSelection.schema,
        tgtDatabase: resolvedTgtDatabase,
        tgtSchema: resolvedTgtSchema,
        bulkMode,
        multiSelectMode,
        selectedObjectType: srcSelection.objectType,
        selectedObjectName: srcSelection.objectName,
        selectedBulkTypeCount: srcBulkTypes.length,
        selectedObjectCount: srcSelectedObjects.length,
        migrationActive,
        stopRequested,
      }),
    [
      bulkMode,
      migrationActive,
      multiSelectMode,
      resolvedTgtDatabase,
      resolvedTgtSchema,
      srcBulkTypes.length,
      srcConnected,
      srcSelectedObjects.length,
      srcSelection.database,
      srcSelection.objectName,
      srcSelection.objectType,
      srcSelection.schema,
      stopRequested,
      tgtConnected,
    ]
  );

  const latestRunMeta = useMemo(() => {
    if (!latestRunSummary) {
      return 'No migration has been run in this session.';
    }
    return `${latestRunSummary.run_id || '--'} | ${latestRunSummary.source_db || '--'} -> ${latestRunSummary.target_db || '--'} | Start: ${formatDateTime(latestRunSummary.started_at)} | End: ${formatDateTime(latestRunSummary.completed_at)} | Duration: ${formatDuration(latestRunSummary.started_at, latestRunSummary.completed_at)}`;
  }, [latestRunSummary]);

  const latestRunTypes = useMemo(() => {
    const byType = latestRunSummary?.stats?.by_type;
    if (!byType || !Object.keys(byType).length) {
      return 'Per-type counters will appear here after the next run.';
    }
    return Object.entries(byType)
      .filter(([, counters]) => Number(counters.total || 0) > 0)
      .map(([type, counters]) => `${formatObjectTypeLabel(type)}: ${counters.success || 0}/${counters.total || 0} success`)
      .join(' | ');
  }, [latestRunSummary]);

  const getSelectedConnection = (type: 'src' | 'tgt') => {
    const connectionId = type === 'src' ? srcConnectionId : tgtConnectionId;
    return connections.find((connection) => connection.id === connectionId) || null;
  };

  const findMatchingConnection = (engine: string, requestDetails: Record<string, string>) => {
    const comparableKeys = Object.keys(requestDetails || {}).filter((key) => key !== 'database' && key !== 'schema');
    return connections.find((connection) => {
      if (connection.engine !== engine) return false;
      return comparableKeys.every((key) => String(connection.fields[key] || '') === String(requestDetails[key] || ''));
    }) || null;
  };

  const appendLog = (text: string, tone?: LogEntry['tone']) => {
    logCounterRef.current += 1;
    const normalized = normalizeLogPayload(text);
    const nextEntry: LogEntry = {
      id: logCounterRef.current,
      tone: tone || inferLogTone(text),
      text: normalized.text,
      timestamp: normalized.timestamp,
    };
    flushSync(() => {
      setLogEntries((current) => [...current, nextEntry]);
    });
  };

  const pushTimedMessage = (type: 'src' | 'tgt', message: { type: 'ok' | 'err'; text: string } | null) => {
    if (type === 'src') {
      setSrcMessage(message);
      return;
    }
    setTgtMessage(message);
  };

  const buildConnectionDetails = (type: 'src' | 'tgt') => {
    const selectedConnection = getSelectedConnection(type);
    if (!selectedConnection) return null;

    const selection = type === 'src' ? srcSelection : tgtSelection;
    const database = type === 'src' ? selection.database : resolveTargetValue(selection.database, srcSelection.database);
    const schema = type === 'src' ? selection.schema : resolveTargetValue(selection.schema, srcSelection.schema);
    return {
      ...selectedConnection.fields,
      ...(database ? { database } : {}),
      ...(schema ? { schema } : {}),
    };
  };

  const resetSourceMetadata = () => {
    setSrcConnected(false);
    setSrcSelection(createEmptySelection());
    setSrcMetadata(createEmptyMetadata());
    setSrcMigrateAll(false);
    setSrcBulkTypes([]);
    setSrcSelectedObjects([]);
    setSrcMigrateData(false);
    setSrcDataOnly(false);
    setSrcDataMode('insert');
    setSrcDataBatchSize('1000');
  };

  const resetTargetMetadata = () => {
    setTgtConnected(false);
    setTgtSelection(createEmptySelection());
    setTgtMetadata(createEmptyMetadata());
    setTgtTruncateBeforeLoad(false);
    setTgtDropAndCreateIfExists(false);
  };

  const resetExecutionState = () => {
    setMigrationActive(false);
    setMigrationRunId('');
    setStopRequested(false);
    setResumeCheckpoint(null);
    setLogEntries([]);
    setLatestRunSummary(null);
    setLatestSuggestions([]);
    setChatPrompt('');
    setMessages([]);
    setIsChatOpen(false);
    setPreloadMessage(null);
  };

  const swapConnections = async () => {
    if (migrationActive) return;
    if (!srcEngine || !tgtEngine || !srcConnectionId || !tgtConnectionId) return;

    const nextSrcEngine = tgtEngine;
    const nextTgtEngine = srcEngine;
    const nextSrcConnectionId = tgtConnectionId;
    const nextTgtConnectionId = srcConnectionId;
    const nextSrcConnection = connections.find((connection) => connection.id === nextSrcConnectionId) || null;
    const nextTgtConnection = connections.find((connection) => connection.id === nextTgtConnectionId) || null;
    const nextSrcMessage = tgtMessage;
    const nextTgtMessage = srcMessage;

    setSrcEngine(nextSrcEngine);
    setTgtEngine(nextTgtEngine);
    setSrcConnectionId(nextSrcConnectionId);
    setTgtConnectionId(nextTgtConnectionId);
    setSrcConnected(Boolean(nextSrcConnection));
    setTgtConnected(Boolean(nextTgtConnection));
    setSrcMessage(nextSrcMessage);
    setTgtMessage(nextTgtMessage);
    setSrcSelection(createEmptySelection());
    setTgtSelection(createEmptySelection());
    setSrcMetadata(createEmptyMetadata());
    setTgtMetadata(createEmptyMetadata());
    setSrcMigrateAll(false);
    setSrcBulkTypes([]);
    setSrcSelectedObjects([]);
    setSrcMigrateData(false);
    setSrcDataOnly(false);
    setSrcDataMode('insert');
    setSrcDataBatchSize('1000');
    setTgtTruncateBeforeLoad(false);
    setTgtDropAndCreateIfExists(false);
    setResumeCheckpoint(null);

    appendLog(
      `[SYSTEM] Swapped source and target connections: ${srcEngine || 'Unknown'} -> ${tgtEngine || 'Unknown'}`,
      'blue'
    );

    try {
      if (nextSrcEngine && nextSrcConnection) {
        await loadDatabases('src', nextSrcEngine, nextSrcConnection.fields);
      }
      if (nextTgtEngine && nextTgtConnection) {
        await loadDatabases('tgt', nextTgtEngine, nextTgtConnection.fields);
      }
      appendLog('[SYSTEM] Metadata refreshed after swap.', 'dim');
    } catch (error) {
      appendLog(`[SYSTEM] Swap refresh failed: ${String(error)}`, 'error');
    }
  };

  const handleLogout = () => {
    sessionStorage.removeItem('dbm_auth');
    sessionStorage.removeItem('dbm_user');
    sessionStorage.removeItem('dbm_role');
    window.location.replace('/login');
  };

  const handleThemeChange = (preference: ThemePreference) => {
    setThemePreference(preference);
    setThemePreferenceState(preference);
  };

  const handleEngineChange = (type: 'src' | 'tgt', engine: string) => {
    if (type === 'src') {
      setSrcEngine(engine);
      setSrcConnectionId('');
      setSrcMessage(null);
      resetSourceMetadata();
      if (engine) appendLog(`[SOURCE] Engine selected: ${engine}`, 'info');
      return;
    }

    setTgtEngine(engine);
    setTgtConnectionId('');
    setTgtMessage(null);
    resetTargetMetadata();
    if (engine) appendLog(`[TARGET] Engine selected: ${engine}`, 'info');
  };

  const handleConnectionChange = (type: 'src' | 'tgt', connectionId: string) => {
    const connection = connections.find((item) => item.id === connectionId);
    if (type === 'src') {
      setSrcConnectionId(connectionId);
      setSrcMessage(null);
      resetSourceMetadata();
      if (connection) appendLog(`[SOURCE] Connection selected: ${connection.name}`, 'info');
      return;
    }

    setTgtConnectionId(connectionId);
    setTgtMessage(null);
    resetTargetMetadata();
    if (connection) appendLog(`[TARGET] Connection selected: ${connection.name}`, 'info');
  };

  const loadDatabases = async (type: 'src' | 'tgt', engine: string, connectionDetails: Record<string, string>) => {
    const setMetadata = type === 'src' ? setSrcMetadata : setTgtMetadata;

    setMetadata((current) => ({ ...current, loadingDatabases: true, databases: [], schemas: [], objectSummary: {}, objects: [] }));

    try {
      const result = await postJson<string[]>('/metadata/databases', {
        database_type: engine,
        connection_details: connectionDetails,
      });

      if (result.status !== 'success') {
        throw new Error(result.message || 'Could not load databases.');
      }

      setMetadata((current) => ({
        ...current,
        databases: result.items || [],
      }));
    } finally {
      setMetadata((current) => ({
        ...current,
        loadingDatabases: false,
      }));
    }
  };

  const loadSchemas = async (type: 'src' | 'tgt', engine: string, connectionDetails: Record<string, string>, database: string) => {
    const setMetadata = type === 'src' ? setSrcMetadata : setTgtMetadata;

    setMetadata((current) => ({
      ...current,
      loadingSchemas: true,
      schemas: [],
      objectSummary: {},
      objects: [],
    }));

    try {
      const result = await postJson<string[]>('/metadata/schemas', {
        database_type: engine,
        connection_details: connectionDetails,
        database_name: database,
      });

      if (result.status !== 'success') {
        throw new Error(result.message || 'Could not load schemas.');
      }

      setMetadata((current) => ({
        ...current,
        schemas: result.items || [],
      }));
      return result.items || [];
    } finally {
      setMetadata((current) => ({
        ...current,
        loadingSchemas: false,
      }));
    }
  };

  const loadObjectSummary = async (type: 'src' | 'tgt', engine: string, connectionDetails: Record<string, string>, database: string, schema: string) => {
    const setMetadata = type === 'src' ? setSrcMetadata : setTgtMetadata;

    setMetadata((current) => ({
      ...current,
      objectSummary: {},
      objects: [],
    }));

    const result = await postJson<Record<string, number>>('/metadata/object-summary', {
      database_type: engine,
      connection_details: connectionDetails,
      database_name: database,
      schema_name: schema,
    });

    if (result.status !== 'success') {
      throw new Error(result.message || 'Could not load object summary.');
    }

    setMetadata((current) => ({
      ...current,
      objectSummary: result.items || {},
    }));
  };

  const loadObjects = async (engine: string, connectionDetails: Record<string, string>, database: string, schema: string, objectType: string) => {
    setSrcMetadata((current) => ({
      ...current,
      loadingObjects: true,
      objects: [],
    }));

    try {
      const result = await postJson<string[]>('/metadata/objects', {
        database_type: engine,
        connection_details: connectionDetails,
        database_name: database,
        schema_name: schema,
        object_type: objectType,
      });

      if (result.status !== 'success') {
        throw new Error(result.message || 'Could not load object names.');
      }

      setSrcMetadata((current) => ({
        ...current,
        objects: result.items || [],
      }));
    } finally {
      setSrcMetadata((current) => ({
        ...current,
        loadingObjects: false,
      }));
    }
  };

  const refreshDatabases = async (type: 'src' | 'tgt') => {
    const engine = type === 'src' ? srcEngine : tgtEngine;
    const selectedConnection = getSelectedConnection(type);
    if (!engine || !selectedConnection) return;

    try {
      await loadDatabases(type, engine, selectedConnection.fields);
      pushTimedMessage(type, { type: 'ok', text: 'Database list refreshed.' });
      appendLog(`[${type === 'src' ? 'SOURCE' : 'TARGET'}] Database catalog refreshed.`, 'dim');
    } catch (error) {
      pushTimedMessage(type, { type: 'err', text: String(error) });
      appendLog(`[${type === 'src' ? 'SOURCE' : 'TARGET'}] Database refresh failed: ${String(error)}`, 'error');
    }
  };

  const handleTestConnection = async (type: 'src' | 'tgt') => {
    const engine = type === 'src' ? srcEngine : tgtEngine;
    const selectedConnection = getSelectedConnection(type);

    if (!engine) {
      const nextMessage = { type: 'err' as const, text: 'Please select a database engine.' };
      pushTimedMessage(type, nextMessage);
      return;
    }

    if (!selectedConnection) {
      const nextMessage = { type: 'err' as const, text: 'Please select a saved connection profile.' };
      pushTimedMessage(type, nextMessage);
      return;
    }

    if (type === 'src') {
      setSrcTesting(true);
      setSrcMessage(null);
      resetSourceMetadata();
    } else {
      setTgtTesting(true);
      setTgtMessage(null);
      resetTargetMetadata();
    }

    try {
      appendLog(`[${type === 'src' ? 'SOURCE' : 'TARGET'}] Testing connection for ${selectedConnection.name}.`, 'blue');
      const result = await postJson<unknown>('/test-connection', {
        database_type: engine,
        connection_details: selectedConnection.fields,
      });

      const ok = result.status === 'success';
      const nextMessage = {
        type: ok ? ('ok' as const) : ('err' as const),
        text: result.message || (ok ? 'Connection tested successfully!' : 'Connection test failed.'),
      };

      if (ok) {
        await loadDatabases(type, engine, selectedConnection.fields);
      }

      if (type === 'src') {
        setSrcConnected(ok);
        pushTimedMessage(type, nextMessage);
      } else {
        setTgtConnected(ok);
        pushTimedMessage(type, nextMessage);
      }
      appendLog(
        `[${type === 'src' ? 'SOURCE' : 'TARGET'}] ${nextMessage.text}`,
        ok ? 'success' : 'error'
      );
    } catch (error) {
      const nextMessage = { type: 'err' as const, text: `Connection test error: ${String(error)}` };
      if (type === 'src') {
        setSrcConnected(false);
        pushTimedMessage(type, nextMessage);
      } else {
        setTgtConnected(false);
        pushTimedMessage(type, nextMessage);
      }
      appendLog(`[${type === 'src' ? 'SOURCE' : 'TARGET'}] ${nextMessage.text}`, 'error');
    } finally {
      if (type === 'src') setSrcTesting(false);
      else setTgtTesting(false);
    }
  };

  const handleDatabaseChange = async (type: 'src' | 'tgt', database: string) => {
    const engine = type === 'src' ? srcEngine : tgtEngine;
    const selectedConnection = getSelectedConnection(type);
    if (!selectedConnection) return;

    if (type === 'src') {
      setSrcSelection({
        database,
        schema: '',
        objectType: '',
        objectName: '',
      });
      setSrcMetadata((current) => ({ ...current, schemas: [], objectSummary: {}, objects: [] }));
      setSrcMigrateAll(false);
      setSrcBulkTypes([]);
      setSrcSelectedObjects([]);
    } else {
      setTgtSelection({
        database,
        schema: '',
        objectType: '',
        objectName: '',
      });
      setTgtMetadata((current) => ({ ...current, schemas: [], objectSummary: {}, objects: [] }));
    }

    if (!database) return;

    const resolvedDatabase = type === 'tgt' ? resolveTargetValue(database, srcSelection.database) : database;
    appendLog(
      `[${type === 'src' ? 'SOURCE' : 'TARGET'}] Database selected: ${database === SOURCE_DEFAULT_VALUE ? `Default -> ${resolvedDatabase}` : resolvedDatabase}`,
      'info'
    );

    if (type === 'tgt' && database === SOURCE_DEFAULT_VALUE) {
      setTgtMetadata((current) => ({ ...current, schemas: [], objectSummary: {}, objects: [] }));
      return;
    }

    try {
      await loadSchemas(type, engine, selectedConnection.fields, resolvedDatabase);
    } catch (error) {
      const nextMessage = { type: 'err' as const, text: String(error) };
      if (type === 'src') setSrcMessage(nextMessage);
      else setTgtMessage(nextMessage);
    }
  };

  const handleSchemaChange = async (type: 'src' | 'tgt', schema: string) => {
    const engine = type === 'src' ? srcEngine : tgtEngine;
    const selectedConnection = getSelectedConnection(type);
    const rawDatabase = type === 'src' ? srcSelection.database : tgtSelection.database;
    const database = type === 'src' ? rawDatabase : resolveTargetValue(rawDatabase, srcSelection.database);
    if (!selectedConnection || !database) return;

    if (type === 'src') {
      setSrcSelection((current) => ({
        ...current,
        schema,
        objectType: '',
        objectName: '',
      }));
      setSrcMetadata((current) => ({ ...current, objectSummary: {}, objects: [] }));
      setSrcMigrateAll(false);
      setSrcBulkTypes([]);
      setSrcSelectedObjects([]);
    } else {
      setTgtSelection((current) => ({
        ...current,
        schema,
        objectType: '',
        objectName: '',
      }));
      setTgtMetadata((current) => ({ ...current, objectSummary: {}, objects: [] }));
    }

    if (!schema) return;

    const resolvedSchema = type === 'tgt' ? resolveTargetValue(schema, srcSelection.schema) : schema;
    appendLog(
      `[${type === 'src' ? 'SOURCE' : 'TARGET'}] Schema selected: ${schema === SOURCE_DEFAULT_VALUE ? `Default -> ${resolvedSchema}` : resolvedSchema}`,
      'info'
    );

    if (type === 'tgt' && (rawDatabase === SOURCE_DEFAULT_VALUE || schema === SOURCE_DEFAULT_VALUE)) {
      setTgtMetadata((current) => ({ ...current, objectSummary: {}, objects: [] }));
      return;
    }

    try {
      await loadObjectSummary(type, engine, selectedConnection.fields, database, resolvedSchema);
    } catch (error) {
      const nextMessage = { type: 'err' as const, text: String(error) };
      if (type === 'src') setSrcMessage(nextMessage);
      else setTgtMessage(nextMessage);
    }
  };

  const handleSourceObjectTypeChange = async (objectType: string) => {
    const selectedConnection = getSelectedConnection('src');
    if (!selectedConnection) return;

    setSrcSelection((current) => ({
      ...current,
      objectType,
      objectName: '',
    }));
    setSrcMetadata((current) => ({ ...current, objects: [] }));
    setSrcSelectedObjects([]);

    if (!srcSelection.database || !srcSelection.schema || !objectType) return;
    appendLog(`[SOURCE] Object type selected: ${objectType}`, 'info');

    try {
      await loadObjects(srcEngine, selectedConnection.fields, srcSelection.database, srcSelection.schema, objectType);
    } catch (error) {
      setSrcMessage({ type: 'err', text: String(error) });
    }
  };

  const toggleBulkType = (type: string) => {
    setSrcBulkTypes((current) => (current.includes(type) ? current.filter((item) => item !== type) : [...current, type]));
    appendLog(`[SOURCE] Bulk object type toggled: ${type}`, 'dim');
  };

  const toggleSelectedObject = (objectName: string) => {
    setSrcSelectedObjects((current) => {
      const next = current.includes(objectName) ? current.filter((item) => item !== objectName) : [...current, objectName];
      return next;
    });
    appendLog(`[SOURCE] Object selection updated: ${objectName}`, 'dim');
  };

  const clearLogs = () => {
    setLogEntries([]);
  };

  const sendAiChat = async () => {
    if (!chatPrompt.trim() || chatLoading) return;
    const input = chatPrompt.trim();
    setMessages((current) => [...current, { role: 'user', content: input }]);
    setChatPrompt('');
    setChatLoading(true);
    try {
      const sqlContext = latestSuggestions
        .map((item) => item?.replacement || item?.description || item?.reason || '')
        .filter(Boolean)
        .join('\n');
      const res = await fetch(`${API_BASE}/ai-chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: input,
          sql: sqlContext,
          object_type: srcSelection.objectType || tgtSelection.objectType || '',
          source: srcEngine,
          target: tgtEngine,
        }),
      });
      const data = (await res.json().catch(() => null)) as Record<string, unknown> | null;
      if (!res.ok) {
        throw new Error(
          String(
            (data && ('detail' in data ? data.detail : data.error)) ||
              `HTTP ${res.status}`
          )
        );
      }
      console.log('AI response:', data);
      const content =
        ((data?.choices as Array<{ message?: { content?: string } }> | undefined)?.[0]?.message?.content as string | undefined) ||
        String(data?.response || '');
      setMessages((current) => [
        ...current,
        { role: 'assistant', content: content || 'AI chat returned no content.' },
      ]);
    } catch (error) {
      setMessages((current) => [...current, { role: 'assistant', content: String(error) }]);
    } finally {
      setChatLoading(false);
    }
  };

  const copyLogs = async () => {
    if (!logEntries.length || !navigator.clipboard) return;
    try {
      await navigator.clipboard.writeText(logEntries.map((entry) => entry.text).join('\n'));
      appendLog('[SYSTEM] Console logs copied to clipboard.', 'dim');
    } catch (error) {
      appendLog(`[SYSTEM] Copy failed: ${String(error)}`, 'error');
    }
  };

  const downloadLogs = () => {
    if (!logEntries.length) return;
    const blob = new Blob([logEntries.map((entry) => entry.text).join('\n')], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `migration-console-${Date.now()}.log`;
    link.click();
    URL.revokeObjectURL(url);
  };

  const stopCurrentMigration = async () => {
    if (!migrationActive || !migrationRunId || stopRequested) return;
    setStopRequested(true);
    try {
      const result = await postJson<unknown>(`/migration-control/stop/${encodeURIComponent(migrationRunId)}`, {});
      appendLog(`[MIGRATION] ${result.message || 'Stop request submitted.'}`, 'warn');
    } catch (error) {
      setStopRequested(false);
      appendLog(`[MIGRATION] Stop request failed: ${String(error)}`, 'error');
    }
  };

  const proceedMigrate = async (resumeFrom: ResumeCheckpoint | null = null) => {
    const sourceDetails = buildConnectionDetails('src');
    const targetDetails = buildConnectionDetails('tgt');
    const batchSize = Math.max(1, Number(srcDataBatchSize || 1000));

    if (!sourceDetails || !targetDetails || !canProceed) {
      appendLog('[MIGRATION] Complete the required selections before starting migration.', 'warn');
      return;
    }

    setMigrationActive(true);
    setStopRequested(false);
    setResumeCheckpoint(null);

    appendLog(`[MIGRATION] Job started - ${new Date().toLocaleString()}`, 'blue');
    appendLog('==========================================', 'dim');
    appendLog(`[MIGRATION] Source : ${srcEngine} / ${srcSelection.database}.${srcSelection.schema}`, 'info');
    appendLog(`[MIGRATION] Target : ${tgtEngine} / ${resolvedTgtDatabase}.${resolvedTgtSchema}`, 'info');
    if (bulkMode) {
      appendLog(`[MIGRATION] Scope  : ${srcBulkTypes.map((type) => formatObjectTypeLabel(type)).join(', ')}`, 'info');
    } else if (multiSelectMode) {
      appendLog(`[MIGRATION] Type   : ${formatObjectTypeLabel(srcSelection.objectType)}`, 'info');
      appendLog(`[MIGRATION] Scope  : ${srcSelectedObjects.length} selected object(s)`, 'info');
    } else {
      appendLog(`[MIGRATION] Type   : ${formatObjectTypeLabel(srcSelection.objectType)}`, 'info');
      appendLog(`[MIGRATION] Object : ${srcSelection.objectName}`, 'info');
    }
    appendLog(
      `[MIGRATION] Mode   : ${srcDataOnly ? `Data only (${srcDataMode})` : srcMigrateData ? `Schema + Data (${srcDataMode})` : 'Schema only'}`,
      'info'
    );
    if (dataOptionsEnabled) {
      appendLog(`[MIGRATION] Load   : ${tgtTruncateBeforeLoad ? 'Truncate before load' : 'Append or merge existing target data'}`, 'info');
      appendLog(`[MIGRATION] Batch  : ${batchSize} row(s) per batch`, 'info');
    }
    if (!srcDataOnly) {
      appendLog(
        `[MIGRATION] Target DDL : ${tgtDropAndCreateIfExists ? 'Drop and recreate target table when it already exists' : 'Create or validate target object normally'}`,
        'info'
      );
    }
    if (resumeFrom) {
      appendLog(`[MIGRATION] Resume : ${resumeFrom.object_type} ${resumeFrom.object_name}`, 'warn');
    }
    appendLog('==========================================', 'dim');

    const basePayload = {
      source_config: {
        database_type: srcEngine,
        connection_details: sourceDetails,
      },
      target_config: {
        database_type: tgtEngine,
        connection_details: targetDetails,
      },
      migrate_data: srcMigrateData,
      data_only: srcDataOnly,
      data_migration_mode: srcDataMode,
      data_batch_size: batchSize,
      truncate_before_load: tgtTruncateBeforeLoad,
      drop_and_create_if_exists: tgtDropAndCreateIfExists,
    };

    const useBulkEndpoint = bulkMode || multiSelectMode;
    const path = useBulkEndpoint ? '/agent-migrate/bulk/stream' : '/agent-migrate/stream';
    const payload = useBulkEndpoint
      ? {
          ...basePayload,
          object_types: bulkMode ? srcBulkTypes : [srcSelection.objectType],
          selected_objects: multiSelectMode ? { [srcSelection.objectType]: srcSelectedObjects } : undefined,
          continue_on_error: true,
          resume_from: resumeFrom || undefined,
        }
      : {
          ...basePayload,
          object_type: srcSelection.objectType,
          object_name: srcSelection.objectName,
        };

    try {
      let finalResult: StreamFinalResult | null = null;

      await postJsonStream(path, payload, (event) => {
        const eventType = String(event.type || '');
        if (eventType === 'meta') {
          const runId = String(event.run_id || '');
          setMigrationRunId(runId);
          appendLog(`[MIGRATION] Run ID: ${runId}`, 'blue');
          return;
        }

        if (eventType === 'log') {
          const message = String(event.message || '');
          appendLog(message, inferLogTone(message));
          return;
        }

        if (eventType === 'error') {
          appendLog(String(event.message || 'Migration stream failed.'), 'error');
          return;
        }

        if (eventType === 'final') {
          finalResult = (event.data || {}) as StreamFinalResult;
        }
      });

      const completedResult = finalResult as StreamFinalResult | null;

      if (completedResult?.run_summary) {
        setLatestRunSummary(completedResult.run_summary);
      }
      setLatestSuggestions(completedResult?.suggestions || []);

      if (completedResult?.summary) {
        appendLog(
          `[MIGRATION] Summary -> Total: ${completedResult.summary.total || 0}, Success: ${completedResult.summary.success || 0}, Error: ${completedResult.summary.error || 0}, Skipped: ${completedResult.summary.skipped || 0}`,
          completedResult.status === 'success' ? 'success' : 'warn'
        );
      } else if (completedResult?.object_result) {
        const objectResult = completedResult.object_result;
        appendLog(
          `[MIGRATION] ${objectResult.status || 'completed'} -> ${objectResult.object_type || '--'} ${objectResult.object_name || '--'} (${objectResult.rows_migrated || 0} rows)`,
          objectResult.status === 'success' ? 'success' : objectResult.status === 'error' ? 'error' : 'info'
        );
      }

      if (completedResult?.message) {
        appendLog(`[MIGRATION] ${completedResult.message}`, completedResult.status === 'success' ? 'success' : 'warn');
      }

      if (completedResult?.resume_checkpoint && ['partial', 'error', 'stopped'].includes(String(completedResult.status || ''))) {
        setResumeCheckpoint(completedResult.resume_checkpoint);
        appendLog(
          `[MIGRATION] Resume checkpoint saved for ${completedResult.resume_checkpoint.object_type} ${completedResult.resume_checkpoint.object_name}`,
          'warn'
        );
      } else if (completedResult?.status === 'success') {
        setResumeCheckpoint(null);
      }
    } catch (error) {
      appendLog(`[MIGRATION] ${String(error)}`, 'error');
    } finally {
      setMigrationActive(false);
      setStopRequested(false);
    }
  };

  const resumeMigration = async () => {
    if (!resumeCheckpoint) {
      appendLog('[MIGRATION] There is no failed or stopped checkpoint to resume from.', 'warn');
      return;
    }
    await proceedMigrate(resumeCheckpoint);
  };

  const resetAll = () => {
    setSrcEngine('');
    setTgtEngine('');
    setSrcConnectionId('');
    setTgtConnectionId('');
    setSrcMessage(null);
    setTgtMessage(null);
    resetSourceMetadata();
    resetTargetMetadata();
    resetExecutionState();
    hydratedRunIdRef.current = '';
  };

  useEffect(() => {
    if (!mounted || !sessionInfo || !requestedRunId) return;
    if (hydratedRunIdRef.current === requestedRunId) return;
    if (!connections.length) {
      setPreloadMessage({ type: 'err', text: 'No saved connection profiles are available for rerun preloading.' });
      hydratedRunIdRef.current = requestedRunId;
      return;
    }

    let cancelled = false;

    const hydrateFromHistory = async () => {
      try {
        const result = await getJson<HistoryItemResponse>(`${API_BASE}/migration-history/${encodeURIComponent(requestedRunId)}`);
        if (result.status && result.status !== 'success') {
          throw new Error(result.message || 'Could not load rerun context.');
        }

        const record = result.item;
        const payload = record?.job_request?.payload;
        const mode = record?.job_request?.mode || 'single';
        const sourceConfig = payload?.source_config;
        const targetConfig = payload?.target_config;
        const sourceEngine = sourceConfig?.database_type || '';
        const targetEngine = targetConfig?.database_type || '';
        const sourceDetails = sourceConfig?.connection_details || {};
        const targetDetails = targetConfig?.connection_details || {};

        if (!sourceEngine || !targetEngine) {
          throw new Error('The selected history record does not include enough source/target information to rerun.');
        }

        const sourceConnection = findMatchingConnection(sourceEngine, sourceDetails);
        const targetConnection = findMatchingConnection(targetEngine, targetDetails);
        if (!sourceConnection || !targetConnection) {
          throw new Error('Matching saved connection profiles were not found for this historical run.');
        }

        if (cancelled) return;

        resetSourceMetadata();
        resetTargetMetadata();
        resetExecutionState();
        setSrcEngine(sourceEngine);
        setTgtEngine(targetEngine);
        setSrcConnectionId(sourceConnection.id);
        setTgtConnectionId(targetConnection.id);
        setSrcConnected(true);
        setTgtConnected(true);
        setSrcMessage({ type: 'ok', text: `Loaded from rerun context: ${sourceConnection.name}` });
        setTgtMessage({ type: 'ok', text: `Loaded from rerun context: ${targetConnection.name}` });
        setLatestRunSummary(record?.run_summary || null);
        setSrcMigrateData(Boolean(payload?.migrate_data));
        setSrcDataOnly(Boolean(payload?.data_only));
        setSrcDataMode(payload?.data_migration_mode || 'insert');
        setSrcDataBatchSize(String(payload?.data_batch_size || 1000));
        setTgtTruncateBeforeLoad(Boolean(payload?.truncate_before_load));
        setTgtDropAndCreateIfExists(Boolean(payload?.drop_and_create_if_exists));

        await loadDatabases('src', sourceEngine, sourceConnection.fields);
        await loadDatabases('tgt', targetEngine, targetConnection.fields);

        const sourceDatabase = String(sourceDetails.database || '');
        const sourceSchema = String(sourceDetails.schema || '');
        const targetDatabase = String(targetDetails.database || '');
        const targetSchema = String(targetDetails.schema || '');

        if (sourceDatabase) {
          await loadSchemas('src', sourceEngine, sourceConnection.fields, sourceDatabase);
        }
        if (sourceDatabase && sourceSchema) {
          await loadObjectSummary('src', sourceEngine, sourceConnection.fields, sourceDatabase, sourceSchema);
        }
        if (targetDatabase) {
          await loadSchemas('tgt', targetEngine, targetConnection.fields, targetDatabase);
        }
        if (targetDatabase && targetSchema) {
          await loadObjectSummary('tgt', targetEngine, targetConnection.fields, targetDatabase, targetSchema);
        }

        const selectedObjectsMap = payload?.selected_objects || {};
        const selectedObjectTypes = Object.keys(selectedObjectsMap).filter((key) => Array.isArray(selectedObjectsMap[key]) && selectedObjectsMap[key].length > 0);
        const isSelectedObjectRun = selectedObjectTypes.length > 0;
        const selectedObjectType = isSelectedObjectRun
          ? selectedObjectTypes[0]
          : String(payload?.object_type || payload?.object_types?.[0] || '');
        const selectedObjectNames = selectedObjectType ? selectedObjectsMap[selectedObjectType] || [] : [];
        const isBulkAllRun = mode === 'bulk' && !isSelectedObjectRun;
        const useMultiSelect = isSelectedObjectRun;
        const singleObjectName = useMultiSelect ? MULTI_SELECT_VALUE : String(payload?.object_name || '');

        setSrcSelection({
          database: sourceDatabase,
          schema: sourceSchema,
          objectType: isBulkAllRun ? '' : selectedObjectType,
          objectName: isBulkAllRun ? '' : singleObjectName,
        });
        setTgtSelection({
          database: targetDatabase,
          schema: targetSchema,
          objectType: '',
          objectName: '',
        });

        if (isBulkAllRun) {
          setSrcMigrateAll(true);
          setSrcBulkTypes(payload?.object_types || []);
          setSrcSelectedObjects([]);
        } else {
          setSrcMigrateAll(false);
          setSrcBulkTypes([]);
          setSrcSelectedObjects(selectedObjectNames);
          if (sourceDatabase && sourceSchema && selectedObjectType) {
            await loadObjects(sourceEngine, sourceConnection.fields, sourceDatabase, sourceSchema, selectedObjectType);
          }
        }

        if (cancelled) return;

        hydratedRunIdRef.current = requestedRunId;
        setPreloadMessage({
          type: 'ok',
          text: `Rerun context loaded for ${requestedRunId}. Review the prefilled configuration, then start migration.`,
        });
        appendLog(`[SYSTEM] Rerun context loaded for historical run ${requestedRunId}.`, 'blue');
      } catch (error) {
        if (cancelled) return;
        hydratedRunIdRef.current = requestedRunId;
        const text = error instanceof Error ? error.message : 'Could not preload rerun context.';
        setPreloadMessage({ type: 'err', text });
        appendLog(`[SYSTEM] ${text}`, 'error');
      }
    };

    void hydrateFromHistory();

    return () => {
      cancelled = true;
    };
  }, [connections, mounted, requestedRunId, sessionInfo]);

  useEffect(() => {
    if (!tgtConnected) return;
    const selectedConnection = getSelectedConnection('tgt');
    if (!selectedConnection || !tgtSelection.database || tgtSelection.database === SOURCE_DEFAULT_VALUE || tgtMetadata.schemas.length) return;

    void loadSchemas('tgt', tgtEngine, selectedConnection.fields, resolvedTgtDatabase).catch((error) => {
      setTgtMessage({ type: 'err', text: String(error) });
    });
  }, [tgtConnected, tgtEngine, tgtSelection.database, tgtMetadata.schemas.length, resolvedTgtDatabase]);

  useEffect(() => {
    if (!tgtConnected) return;
    const selectedConnection = getSelectedConnection('tgt');
    if (!selectedConnection || !resolvedTgtDatabase || !resolvedTgtSchema) return;
    if (tgtSelection.database === SOURCE_DEFAULT_VALUE || tgtSelection.schema === SOURCE_DEFAULT_VALUE) return;
    if (Object.keys(tgtMetadata.objectSummary).length) return;

    void loadObjectSummary('tgt', tgtEngine, selectedConnection.fields, resolvedTgtDatabase, resolvedTgtSchema).catch((error) => {
      setTgtMessage({ type: 'err', text: String(error) });
    });
  }, [
    tgtConnected,
    tgtEngine,
    tgtSelection.database,
    tgtSelection.schema,
    tgtMetadata.objectSummary,
    resolvedTgtDatabase,
    resolvedTgtSchema,
  ]);

  if (!mounted || !sessionInfo) return null;

  return (
    <div className="migration-studio-page">
      <div className="topbar">
        <div className="brand">
          <div className="brand-icon">
            <i className="fa-solid fa-database"></i>
          </div>
          <div className="brand-name">
            <span className="db-text">Database</span> <span className="migrator-text">Migrator</span>
          </div>
        </div>

        <div className="topbar-actions">
          <a className="nav-btn" href="/home" title="Home">
            <i className="fa-solid fa-house"></i>
          </a>
          <button
            className={`nav-btn ${isChatOpen ? 'nav-btn-active' : ''}`}
            type="button"
            title="AI Chat"
            aria-label="AI Chat"
            onClick={() => setIsChatOpen((current) => !current)}
          >
            <i className="fa-solid fa-robot"></i>
          </button>

          <div className="user-menu">
            <button className="user-chip user-menu-toggle" type="button" title="Current user" onClick={() => setUserMenuOpen(!userMenuOpen)}>
              <div className="user-avatar">{sessionInfo.avatar}</div>
              <div className="user-meta">
                <div className="user-name">{sessionInfo.username}</div>
                <div className="user-role">{sessionInfo.roleLabel}</div>
              </div>
              <i className="fa-solid fa-chevron-down"></i>
            </button>
            {userMenuOpen && (
              <div className="user-dropdown open">
                <button className="menu-item" type="button">
                  <i className="fa-regular fa-bell"></i> Notifications
                </button>
                <button className={`menu-item${themeMenuOpen ? ' active' : ''}`} type="button" onClick={() => setThemeMenuOpen((current) => !current)}>
                  <i className="fa-solid fa-circle-half-stroke"></i> Theme
                  <span className="menu-trailing">
                    {themePreference === 'auto' ? 'Auto' : themePreference === 'dark' ? 'Dark' : 'Light'}
                    <i className={`fa-solid ${themeMenuOpen ? 'fa-chevron-up' : 'fa-chevron-down'}`}></i>
                  </span>
                </button>
                {themeMenuOpen ? (
                  <div className="theme-menu" role="group" aria-label="Theme options">
                    {(['auto', 'light', 'dark'] as ThemePreference[]).map((option) => (
                      <button
                        key={option}
                        className={`theme-option${themePreference === option ? ' selected' : ''}`}
                        type="button"
                        onClick={() => handleThemeChange(option)}
                      >
                        <span className="theme-option-copy">
                          <strong>{option === 'auto' ? 'Auto' : option === 'light' ? 'Light' : 'Dark'}</strong>
                          <span>
                            {option === 'auto'
                              ? 'Follow system appearance'
                              : option === 'light'
                                ? 'Bright workspace theme'
                                : 'Low-glare workspace theme'}
                          </span>
                        </span>
                        {themePreference === option ? <i className="fa-solid fa-check"></i> : null}
                      </button>
                    ))}
                  </div>
                ) : null}
                <button className="menu-item" type="button">
                  <i className="fa-solid fa-sliders"></i> Settings
                </button>
                <button className="menu-item" type="button" onClick={handleLogout}>
                  <i className="fa-solid fa-right-from-bracket"></i> Logout
                </button>
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="studio-shell">
        <div className={`studio-main ${isChatOpen ? 'chat-open' : ''}`}>
          <div className="page">
        <section className="hero">
          <PageHeader
            title={
              <>
                Agent-Powered <span className="highlight">Migration</span>
              </>
            }
            description="Configure trusted source and target systems, execute schema and data movement, and monitor the run from one premium operations workspace."
          />
        </section>

        <section className="grid">
          <div className="card source-card">
            <div className="card-head">
              <div className="card-head-left">
                <div className="card-icon config-card-icon" aria-hidden="true">
                  <span className="config-direction-icon source-config-icon">
                    <i className="fa-solid fa-database config-db-icon"></i>
                    <span className="config-arrow-badge">
                      <i className="fa-solid fa-arrow-down"></i>
                    </span>
                  </span>
                </div>
                <div>
                  <div className="card-title">Source Configuration</div>
                </div>
              </div>
              <div className={`pill ${srcConnected ? 'connected' : 'failed'}`}>{srcConnected ? 'Connected' : 'Disconnected'}</div>
            </div>

            <div className="form-group">
              <label htmlFor="srcEngine">
                Source Database <span className="req">*</span>
              </label>
              <select id="srcEngine" value={srcEngine} onChange={(event) => handleEngineChange('src', event.target.value)}>
                <option value="">-- Select database engine --</option>
                <option value="mysql">MySQL</option>
                <option value="postgresql">PostgreSQL</option>
                <option value="sqlserver">SQL Server</option>
                <option value="snowflake">Snowflake</option>
              </select>
            </div>

            <div className="check">
              <label htmlFor="srcConnection">Choose Connection</label>
              <div className="form-row">
                <div className="form-group">
                  <select id="srcConnection" value={srcConnectionId} disabled={!srcEngine} onChange={(event) => handleConnectionChange('src', event.target.value)}>
                    <option value="">-- Select connection --</option>
                    {srcConnections.map((connection) => (
                      <option key={connection.id} value={connection.id}>
                        {connection.name}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="btn-row">
                  <a className="btn btn-ghost btn-link" href="/connections" title="Manage Connections">
                    <i className="fa-solid fa-plug-circle-plus"></i>
                  </a>
                </div>
              </div>
            </div>

            <div className="btn-row">
              <button className="btn btn-primary" disabled={!srcEngine || !srcConnectionId || srcTesting} onClick={() => void handleTestConnection('src')}>
                <i className="fa-solid fa-plug-circle-bolt"></i> {srcTesting ? 'Testing...' : 'Test Connection'}
              </button>
            </div>

            {srcMessage && <div className={`status-message ${srcMessage.type}`}>{srcMessage.text}</div>}

            {srcConnected && (
              <div className="post-connection">
                <div className="section-label">Object Selection</div>

                <div className="inline-field">
                  <div className="form-group">
                    <label htmlFor="srcDatabase">Database</label>
                    <select id="srcDatabase" value={srcSelection.database} onChange={(event) => void handleDatabaseChange('src', event.target.value)}>
                      <option value="">-- Select database --</option>
                      {srcMetadata.databases.map((database) => (
                        <option key={database} value={database}>
                          {database}
                        </option>
                      ))}
                    </select>
                  </div>
                  <button className="icon-btn" type="button" title="Refresh databases" onClick={() => void refreshDatabases('src')}>
                    <i className={`fa-solid ${srcMetadata.loadingDatabases ? 'fa-arrows-rotate fa-spin' : 'fa-rotate-right'}`}></i>
                  </button>
                </div>

                <div className="form-group">
                  <label htmlFor="srcSchema">Schema</label>
                  <select
                    id="srcSchema"
                    value={srcSelection.schema}
                    disabled={!srcSelection.database || srcMetadata.loadingSchemas}
                    onChange={(event) => void handleSchemaChange('src', event.target.value)}
                  >
                    <option value="">-- Select schema --</option>
                    {srcMetadata.schemas.map((schema) => (
                      <option key={schema} value={schema}>
                        {schema}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="check">
                  <label>
                      <input
                        type="checkbox"
                        checked={srcMigrateAll}
                        disabled={!srcSelection.schema || !srcObjectTypes.length}
                        onChange={(event) => {
                          const checked = event.target.checked;
                          setSrcMigrateAll(checked);
                          setSrcBulkTypes(checked ? srcObjectTypes.map((item) => item.type) : []);
                          appendLog(`[SOURCE] Migration mode changed: ${checked ? 'Migrate All' : 'Single Object'}`, 'info');
                        }}
                      />
                    Migrate All
                  </label>
                  <small>Migrate every object in the selected object types using the backend execution order.</small>
                </div>

                {srcMigrateAll ? (
                  <div className="form-group">
                    <label>Object Types</label>
                    <div className="object-type-grid">
                      {srcObjectTypes.map((item) => (
                        <label key={item.type} className="object-check">
                          <input type="checkbox" checked={srcBulkTypes.includes(item.type)} onChange={() => toggleBulkType(item.type)} />
                          <span>{formatObjectTypeLabel(item.type, item.count)}</span>
                        </label>
                      ))}
                    </div>
                  </div>
                ) : (
                  <>
                    <div className="form-group">
                      <label htmlFor="srcObjectType">Object Type</label>
                      <select
                        id="srcObjectType"
                        value={srcSelection.objectType}
                        disabled={!srcSelection.schema}
                        onChange={(event) => void handleSourceObjectTypeChange(event.target.value)}
                      >
                        <option value="">-- Select object type --</option>
                        {srcObjectTypes.map((item) => (
                          <option key={item.type} value={item.type}>
                            {formatObjectTypeLabel(item.type, item.count)}
                          </option>
                        ))}
                      </select>
                    </div>

                    <div className="form-group">
                      <label htmlFor="srcObjectName">Object Name</label>
                      <select
                        id="srcObjectName"
                        value={srcSelection.objectName}
                        disabled={!srcSelection.objectType || srcMetadata.loadingObjects}
                        onChange={(event) => {
                          const nextValue = event.target.value;
                          setSrcSelection((current) => ({ ...current, objectName: nextValue }));
                          setSrcSelectedObjects([]);
                          if (nextValue) appendLog(`[SOURCE] Object selected: ${nextValue === MULTI_SELECT_VALUE ? 'Multi Select' : nextValue}`, 'info');
                        }}
                      >
                        <option value="">-- Select object name --</option>
                        <option value={MULTI_SELECT_VALUE}>Multi Select</option>
                        {srcMetadata.objects.map((item) => (
                          <option key={item} value={item}>
                            {item}
                          </option>
                        ))}
                      </select>
                    </div>
                    {multiSelectMode ? (
                      <div className="form-group">
                        <label>Selected Object Names</label>
                        <div className="object-type-grid">
                          {srcMetadata.objects.map((item) => (
                            <label key={item} className="object-check">
                              <input type="checkbox" checked={srcSelectedObjects.includes(item)} onChange={() => toggleSelectedObject(item)} />
                              <span>{item}</span>
                            </label>
                          ))}
                        </div>
                      </div>
                    ) : null}
                  </>
                )}

                {supportsDataMovement && (
                  <div className="data-movement-stack">
                    <div className="section-label">Data Movement</div>

                    <div className="inline-checks">
                      <div className="check">
                        <label>
                          <input
                            type="checkbox"
                            checked={srcMigrateData}
                            onChange={(event) => {
                              setSrcMigrateData(event.target.checked);
                              appendLog(`[SOURCE] Migrate data ${event.target.checked ? 'enabled' : 'disabled'}.`, 'dim');
                            }}
                          />
                          Migrate Data
                        </label>
                      </div>
                      <div className="check">
                        <label>
                          <input
                            type="checkbox"
                            checked={srcDataOnly}
                            onChange={(event) => {
                              const checked = event.target.checked;
                              setSrcDataOnly(checked);
                              if (checked) setSrcMigrateData(true);
                              appendLog(`[SOURCE] Data only ${checked ? 'enabled' : 'disabled'}.`, 'dim');
                            }}
                          />
                          Data Only
                        </label>
                      </div>
                    </div>

                    <div className="form-row two-col data-migration-mode">
                      <div className="form-group">
                        <label htmlFor="srcDataMode">Data Migration Mode</label>
                        <select
                          id="srcDataMode"
                          value={srcDataMode}
                          disabled={!dataOptionsEnabled}
                          onChange={(event) => {
                            setSrcDataMode(event.target.value);
                            appendLog(`[SOURCE] Data migration mode: ${event.target.value}`, 'dim');
                          }}
                        >
                          <option value="insert">Insert</option>
                          <option value="upsert">Upsert</option>
                          <option value="skip_existing">Skip Existing</option>
                        </select>
                      </div>

                      <div className="form-group">
                        <label htmlFor="srcDataBatchSize">Data Batch Size</label>
                        <input
                          id="srcDataBatchSize"
                          type="number"
                          min="1"
                          step="1"
                          value={srcDataBatchSize}
                          disabled={!dataOptionsEnabled}
                          onChange={(event) => {
                            setSrcDataBatchSize(event.target.value);
                            appendLog(`[SOURCE] Data batch size: ${event.target.value || '0'}`, 'dim');
                          }}
                        />
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>

          <div className="card target-card">
            <div className="card-head">
              <div className="card-head-left">
                <div className="card-icon config-card-icon" aria-hidden="true">
                  <span className="config-direction-icon target-config-icon">
                    <i className="fa-solid fa-database config-db-icon"></i>
                    <span className="config-arrow-badge">
                      <i className="fa-solid fa-arrow-up"></i>
                    </span>
                  </span>
                </div>
                <div>
                  <div className="card-title">Target Configuration</div>
                </div>
              </div>
              <div className={`pill ${tgtConnected ? 'connected' : 'failed'}`}>{tgtConnected ? 'Connected' : 'Disconnected'}</div>
            </div>

            <div className="form-group">
              <label htmlFor="tgtEngine">
                Target Database <span className="req">*</span>
              </label>
              <select id="tgtEngine" value={tgtEngine} onChange={(event) => handleEngineChange('tgt', event.target.value)}>
                <option value="">-- Select database engine --</option>
                <option value="mysql">MySQL</option>
                <option value="postgresql">PostgreSQL</option>
                <option value="sqlserver">SQL Server</option>
                <option value="snowflake">Snowflake</option>
              </select>
            </div>

            <div className="check">
              <label htmlFor="tgtConnection">Choose Connection</label>
              <div className="form-row">
                <div className="form-group">
                  <select id="tgtConnection" value={tgtConnectionId} disabled={!tgtEngine} onChange={(event) => handleConnectionChange('tgt', event.target.value)}>
                    <option value="">-- Select connection --</option>
                    {tgtConnections.map((connection) => (
                      <option key={connection.id} value={connection.id}>
                        {connection.name}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="btn-row">
                  <a className="btn btn-ghost btn-link" href="/connections" title="Manage Connections">
                    <i className="fa-solid fa-plug-circle-plus"></i>
                  </a>
                </div>
              </div>
            </div>

            <div className="btn-row">
              <button className="btn btn-primary" disabled={!tgtEngine || !tgtConnectionId || tgtTesting} onClick={() => void handleTestConnection('tgt')}>
                <i className="fa-solid fa-plug-circle-bolt"></i> {tgtTesting ? 'Testing...' : 'Test Connection'}
              </button>
            </div>

            {tgtMessage && <div className={`status-message ${tgtMessage.type}`}>{tgtMessage.text}</div>}

            {tgtConnected && (
              <div className="post-connection">
                <div className="section-label">Load Strategy</div>

                <div className="inline-checks">
                  <div className="check">
                    <label>
                      <input
                        type="checkbox"
                        checked={tgtTruncateBeforeLoad}
                        onChange={(event) => {
                          setTgtTruncateBeforeLoad(event.target.checked);
                          appendLog(`[TARGET] Truncate before load ${event.target.checked ? 'enabled' : 'disabled'}.`, 'dim');
                        }}
                      />
                      Truncate before load
                    </label>
                  </div>
                  <div className="check">
                    <label>
                      <input
                        type="checkbox"
                        checked={tgtDropAndCreateIfExists}
                        onChange={(event) => {
                          setTgtDropAndCreateIfExists(event.target.checked);
                          appendLog(`[TARGET] Drop and create if exists ${event.target.checked ? 'enabled' : 'disabled'}.`, 'dim');
                        }}
                      />
                      Drop and create if exists
                    </label>
                  </div>
                </div>

                <div className="section-label">Target Selection</div>

                <div className="inline-field">
                  <div className="form-group">
                    <label htmlFor="tgtDatabase">Database</label>
                    <select id="tgtDatabase" value={tgtSelection.database} onChange={(event) => void handleDatabaseChange('tgt', event.target.value)}>
                      <option value="">-- Select database --</option>
                      {srcSelection.database && <option value={SOURCE_DEFAULT_VALUE}>Default (Use Source Database)</option>}
                      {tgtMetadata.databases.map((database) => (
                        <option key={database} value={database}>
                          {database}
                        </option>
                      ))}
                    </select>
                  </div>
                  <button className="icon-btn" type="button" title="Refresh databases" onClick={() => void refreshDatabases('tgt')}>
                    <i className={`fa-solid ${tgtMetadata.loadingDatabases ? 'fa-arrows-rotate fa-spin' : 'fa-rotate-right'}`}></i>
                  </button>
                </div>

                <div className="form-group">
                  <label htmlFor="tgtSchema">Schema</label>
                  <select
                    id="tgtSchema"
                    value={tgtSelection.schema}
                    disabled={!tgtSelection.database || tgtMetadata.loadingSchemas}
                    onChange={(event) => void handleSchemaChange('tgt', event.target.value)}
                  >
                    <option value="">-- Select schema --</option>
                    {resolvedTgtDatabase && <option value={SOURCE_DEFAULT_VALUE}>Default (Use Source Schema)</option>}
                    {tgtMetadata.schemas.map((schema) => (
                      <option key={schema} value={schema}>
                        {schema}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="form-group">
                  <label htmlFor="tgtObjectType">Target Object Type</label>
                  <select
                    id="tgtObjectType"
                    value={tgtSelection.objectType}
                    disabled={!resolvedTgtSchema}
                    onChange={(event) => setTgtSelection((current) => ({ ...current, objectType: event.target.value }))}
                  >
                    <option value="">-- Existing objects in schema --</option>
                    {tgtObjectTypes.map((item) => (
                      <option key={item.type} value={item.type}>
                        {formatObjectTypeLabel(item.type, item.count)}
                      </option>
                    ))}
                  </select>
                </div>

              </div>
            )}
          </div>
        </section>

        <section className="action-section">
          <div className="action-head">
            <div>
              <h3 title={actionState.subtitle}>{actionState.title}</h3>
              {preloadMessage ? <div className={`status-message ${preloadMessage.type}`}>{preloadMessage.text}</div> : null}
            </div>
            <div className="console-actions">
              <button
                className="btn btn-ghost"
                type="button"
                disabled={migrationActive || !srcEngine || !tgtEngine || !srcConnectionId || !tgtConnectionId}
                onClick={() => void swapConnections()}
              >
                <i className="fa-solid fa-right-left"></i> Swap Connections
              </button>
              <button className="btn btn-ghost" type="button" onClick={resetAll}>
                <i className="fa-solid fa-rotate-left"></i> Reset
              </button>
              {canResume && (
                <button className="btn btn-ghost" type="button" onClick={() => void resumeMigration()}>
                  <i className="fa-solid fa-forward"></i> Resume
                </button>
              )}
              {migrationActive && (
                <button className="btn btn-ghost" type="button" disabled={stopRequested} onClick={() => void stopCurrentMigration()}>
                  <i className="fa-solid fa-stop"></i> {stopRequested ? 'Stopping...' : 'Stop'}
                </button>
              )}
              <button className="btn btn-primary" type="button" disabled={!canProceed} onClick={() => void proceedMigrate()}>
                <i className={`fa-solid ${bulkMode ? 'fa-layer-group' : 'fa-paper-plane'}`}></i> {bulkMode ? 'Migrate All' : 'Proceed to Migrate'}
              </button>
            </div>
          </div>
        </section>

        <section className="console">
          <div className="console-head">
            <div>
              <div className="console-title" title="Live backend logs and execution feedback">Migration Console</div>
            </div>
            <div className="console-actions">
              <button className="btn btn-ghost" type="button" disabled={!logEntries.length} onClick={() => void copyLogs()}>
                <i className="fa-regular fa-copy"></i> Copy Logs
              </button>
              <button className="btn btn-ghost" type="button" disabled={!logEntries.length} onClick={clearLogs}>
                <i className="fa-solid fa-broom"></i> Clear
              </button>
              <button className="btn btn-ghost" type="button" disabled={!logEntries.length} onClick={downloadLogs}>
                <i className="fa-solid fa-download"></i> Download
              </button>
            </div>
          </div>

          <div className="console-body" ref={logBodyRef}>
            {!logEntries.length ? (
              <div className="log-empty">
                <i className="fa-solid fa-terminal"></i> Awaiting operations...
              </div>
            ) : (
              logEntries.map((entry) => (
                <div key={entry.id} className={`log-line ll-${entry.tone}`}>
                  <span className="log-time">{entry.timestamp}</span>
                  <span className="log-text">{entry.text}</span>
                </div>
              ))
            )}
          </div>
        </section>

        <section className="run-snapshot">
          <div className="snapshot-head">
            <div>
              <div className="snapshot-title" title="Use this console-focused summary here and open full history for deeper diagnostics.">Latest Run Snapshot</div>
            </div>
            <div className="console-actions">
              <a className="btn btn-ghost" href="/migration-history">
                <i className="fa-solid fa-chart-line"></i> Open Full History
              </a>
            </div>
          </div>

          <div className="snapshot-grid">
            <div className="snapshot-kpis">
              <div className="snapshot-card">
                <div className="snapshot-label">Status</div>
                <div className="snapshot-value">{String(latestRunSummary?.status || 'Idle').toUpperCase()}</div>
              </div>
              <div className="snapshot-card">
                <div className="snapshot-label">Objects</div>
                <div className="snapshot-value">{latestRunSummary?.stats?.total_objects || 0}</div>
              </div>
              <div className="snapshot-card">
                <div className="snapshot-label">Success</div>
                <div className="snapshot-value">{latestRunSummary?.stats?.success_objects || 0}</div>
              </div>
              <div className="snapshot-card">
                <div className="snapshot-label">Errors</div>
                <div className="snapshot-value">{latestRunSummary?.stats?.error_objects || 0}</div>
              </div>
              <div className="snapshot-card">
                <div className="snapshot-label">Rows</div>
                <div className="snapshot-value">{latestRunSummary?.stats?.total_rows_migrated || 0}</div>
              </div>
            </div>

            <div className="snapshot-box">
              <div className="snapshot-label">Latest Run</div>
              <div className="snapshot-code">{latestRunMeta}</div>
              <div className="snapshot-meta">{latestRunTypes}</div>
            </div>
          </div>
        </section>

          </div>
        </div>

        <aside className={`ai-chat-panel ${isChatOpen ? 'open' : 'closed'}`} aria-hidden={!isChatOpen}>
          <div className="ai-chat-drawer-head">
            <div>
              <div className="snapshot-title">AI Chat</div>
              <div className="snapshot-meta">
                {ragAgentStatus?.configured
                  ? `Connected${ragAgentStatus.model ? ` to ${ragAgentStatus.model}` : ''}`
                  : 'AI chat is not configured'}
              </div>
            </div>
            <button className="icon-btn" type="button" onClick={() => setIsChatOpen(false)}>
              <i className="fa-solid fa-xmark"></i>
            </button>
          </div>

          <div className="ai-chat-drawer-body">
            <div className="chat-container">
              <div className="chat-messages">
                {!messages.length ? (
                  <div className="chat-empty">Ask the AI agent for migration help or a SQL fix.</div>
                ) : (
                  messages.map((message, index) => (
                    <div key={`${message.role}-${index}`} className={`message-row ${message.role}`}>
                      <div className={`message ${message.role}`}>{message.content}</div>
                    </div>
                  ))
                )}
                {chatLoading ? (
                  <div className="message-row assistant">
                    <div className="message assistant">AI is typing...</div>
                  </div>
                ) : null}
                <div ref={chatMessagesEndRef}></div>
              </div>

              <div className="chat-input-bar">
                <div className="snapshot-meta">Latest reusable suggestions: {latestSuggestions.length}</div>
                <textarea
                  className="chat-input"
                  value={chatPrompt}
                  placeholder="Ask the AI agent for a SQL fix or migration explanation."
                  onChange={(event) => setChatPrompt(event.target.value)}
                />
                <div className="btn-row">
                  <button className="btn btn-primary" type="button" disabled={chatLoading || !chatPrompt.trim()} onClick={() => void sendAiChat()}>
                    <i className="fa-solid fa-paper-plane"></i> {chatLoading ? 'Sending...' : 'Send'}
                  </button>
                </div>
              </div>
            </div>
          </div>
        </aside>
      </div>
    </div>
  );
}
