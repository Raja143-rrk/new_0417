'use client';

import Link from 'next/link';
import { useEffect, useMemo, useState } from 'react';
import PageHeader from '@/components/PageHeader';
import QueriesViewer from '@/components/QueriesViewer';
import { canAccessPage, getSession, hasPermission, logout } from '@/lib/rbac';
import './migration-history.css';

const API_BASE = 'http://127.0.0.1:8000/api';
const ALL_OBJECT_TYPES = ['table', 'view', 'storedprocedure', 'function', 'trigger', 'cursor', 'event', 'sequence', 'synonym'];
const PAGE_SIZE = 10;

type HistoryStatus = 'success' | 'error' | 'partial' | 'skipped' | string;

interface ObjectResult {
  object_type?: string;
  object_name?: string;
  status?: HistoryStatus;
  retry_count?: number;
  rows_migrated?: number;
  source_row_count?: number;
  target_row_count?: number;
  missing_row_count?: number;
  started_at?: string;
  completed_at?: string;
  error_message?: string;
  remediation?: string;
}

interface RunStats {
  total_objects?: number;
  success_objects?: number;
  error_objects?: number;
  skipped_objects?: number;
  total_rows_migrated?: number;
  total_source_rows?: number;
  total_target_rows?: number;
  total_missing_rows?: number;
  total_retries?: number;
  by_type?: Record<string, { total?: number; success?: number }>;
}

interface RunSummary {
  run_id?: string;
  status?: HistoryStatus;
  source_db?: string;
  target_db?: string;
  started_at?: string;
  completed_at?: string;
  execution_order?: string[];
  object_results?: ObjectResult[];
  stats?: RunStats;
}

interface HistoryRecord {
  run_summary?: RunSummary;
  logs?: string[];
}

interface ApiListResponse {
  status?: string;
  message?: string;
  items?: HistoryRecord[];
}

interface ApiItemResponse {
  status?: string;
  message?: string;
  item?: HistoryRecord;
}

interface Filters {
  search: string;
  status: string;
  objectType: string;
  dateFrom: string;
  dateTo: string;
}

function getStatusClass(status?: string): string {
  const value = String(status || '').toLowerCase();
  if (value === 'success') return 'status-success';
  if (value === 'error') return 'status-error';
  if (value === 'partial') return 'status-partial';
  return 'status-skipped';
}

function formatDateTime(value?: string): string {
  if (!value) return '--';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
}

function formatDuration(startValue?: string, endValue?: string): string {
  if (!startValue || !endValue) return '--';
  const start = new Date(startValue).getTime();
  const end = new Date(endValue).getTime();
  if (Number.isNaN(start) || Number.isNaN(end) || end < start) return '--';
  const totalSeconds = Math.floor((end - start) / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) return `${hours}h ${minutes}m ${String(seconds).padStart(2, '0')}s`;
  if (minutes > 0) return `${minutes}m ${String(seconds).padStart(2, '0')}s`;
  return `${seconds}s`;
}

function summarizeByType(byType?: RunStats['by_type']): string {
  return Object.entries(byType || {})
    .filter(([, counts]) => (counts?.total || 0) > 0)
    .map(([key, counts]) => `${key}:${counts?.success || 0}/${counts?.total || 0}`)
    .join(' | ');
}

function filterHistoryItems(items: HistoryRecord[], filters: Filters): HistoryRecord[] {
  return items.filter((item) => {
    const run = item?.run_summary || {};
    const objectResults = run.object_results || [];
    const startedAt = run.started_at ? new Date(run.started_at) : null;
    const haystack = [run.run_id, run.source_db, run.target_db, ...objectResults.map((result) => `${result.object_type || ''} ${result.object_name || ''}`)]
      .join(' ')
      .toLowerCase();
    const matchesDateFrom =
      !filters.dateFrom ||
      (startedAt && !Number.isNaN(startedAt.getTime()) && startedAt >= new Date(`${filters.dateFrom}T00:00:00`));
    const matchesDateTo =
      !filters.dateTo ||
      (startedAt && !Number.isNaN(startedAt.getTime()) && startedAt <= new Date(`${filters.dateTo}T23:59:59`));

    return (
      (!filters.status || String(run.status || '').toLowerCase() === filters.status) &&
      (!filters.objectType || objectResults.some((result) => result.object_type === filters.objectType)) &&
      (!filters.search || haystack.includes(filters.search)) &&
      matchesDateFrom &&
      matchesDateTo
    );
  });
}

function sortHistoryItems(items: HistoryRecord[]): HistoryRecord[] {
  return [...items].sort((left, right) => {
    const leftTime = new Date(left?.run_summary?.started_at || 0).getTime() || 0;
    const rightTime = new Date(right?.run_summary?.started_at || 0).getTime() || 0;
    return rightTime - leftTime;
  });
}

function downloadContent(content: string, filename: string, mime: string) {
  const blob = new Blob([content], { type: mime });
  const link = document.createElement('a');
  link.href = URL.createObjectURL(blob);
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  let data: T | null = null;
  try {
    data = (await response.json()) as T;
  } catch {
    data = null;
  }
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return data as T;
}

async function postJson<T>(url: string): Promise<T> {
  const response = await fetch(url, { method: 'POST' });
  let data: T | null = null;
  try {
    data = (await response.json()) as T;
  } catch {
    data = null;
  }
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return data as T;
}

export default function MigrationHistoryPage() {
  const session = getSession();
  const canOperateMigrations = hasPermission('operateMigrations', session.role);
  const [ready, setReady] = useState(false);
  const [items, setItems] = useState<HistoryRecord[]>([]);
  const [selectedRunId, setSelectedRunId] = useState('');
  const [selectedRecord, setSelectedRecord] = useState<HistoryRecord | null>(null);
  const [errorOnly, setErrorOnly] = useState(false);
  const [page, setPage] = useState(1);
  const [message, setMessage] = useState('');
  const [loading, setLoading] = useState(false);
  const [rerunLoading, setRerunLoading] = useState(false);
  const [showRerunActions, setShowRerunActions] = useState(false);
  const [queriesRunId, setQueriesRunId] = useState('');
  const [filters, setFilters] = useState<Filters>({
    search: '',
    status: '',
    objectType: '',
    dateFrom: '',
    dateTo: '',
  });

  useEffect(() => {
    if (!session.isAuthenticated) {
      window.location.replace('/login');
      return;
    }
    if (!canAccessPage('history', session.role)) {
      window.location.replace('/home');
      return;
    }
    setReady(true);
  }, []);

  const filteredItems = useMemo(() => sortHistoryItems(filterHistoryItems(items, filters)), [items, filters]);
  const totalPages = Math.max(1, Math.ceil(filteredItems.length / PAGE_SIZE));
  const safePage = Math.min(Math.max(1, page), totalPages);
  const pagedItems = useMemo(() => {
    const start = (safePage - 1) * PAGE_SIZE;
    return filteredItems.slice(start, start + PAGE_SIZE);
  }, [filteredItems, safePage]);

  const metrics = useMemo(() => {
    return filteredItems.reduce(
      (acc, item) => {
        const stats = item?.run_summary?.stats || {};
        acc.runs += 1;
        acc.objects += Number(stats.total_objects || 0);
        acc.success += Number(stats.success_objects || 0);
        acc.error += Number(stats.error_objects || 0);
        acc.rows += Number(stats.total_rows_migrated || 0);
        acc.missing += Number(stats.total_missing_rows || 0);
        return acc;
      },
      { runs: 0, objects: 0, success: 0, error: 0, rows: 0, missing: 0 }
    );
  }, [filteredItems]);

  useEffect(() => {
    if (!ready) return;
    void loadMigrationHistory();
  }, [ready]);

  useEffect(() => {
    setPage((current) => Math.min(current, totalPages));
    if (filteredItems.length === 0) {
      setSelectedRunId('');
      setSelectedRecord(null);
      return;
    }
    if (!filteredItems.some((item) => item.run_summary?.run_id === selectedRunId)) {
      setSelectedRunId(filteredItems[0]?.run_summary?.run_id || '');
    }
  }, [filteredItems, selectedRunId, totalPages]);

  useEffect(() => {
    if (!ready || !selectedRunId) return;
    void loadRecord(selectedRunId);
  }, [ready, selectedRunId]);

  async function loadMigrationHistory() {
    setLoading(true);
    setMessage('');
    try {
      const result = await getJson<ApiListResponse>(`${API_BASE}/migration-history?limit=50`);
      if (result.status && result.status !== 'success') {
        throw new Error(result.message || 'Could not load migration history.');
      }
      const incoming = result.items || [];
      setItems(incoming);
      if (!selectedRunId && incoming.length) {
        setSelectedRunId(incoming[0]?.run_summary?.run_id || '');
      }
    } catch (error) {
      setItems([]);
      setSelectedRecord(null);
      setMessage(error instanceof Error ? error.message : 'Could not load migration history.');
    } finally {
      setLoading(false);
    }
  }

  async function rerunSelectedMigration() {
    const runId = selectedRecord?.run_summary?.run_id;
    if (!runId || !canOperateMigrations) return;
    setRerunLoading(true);
    setMessage('');
    setShowRerunActions(false);
    try {
      const result = await postJson<{ status?: string; message?: string }>(`${API_BASE}/jobs/${encodeURIComponent(runId)}/rerun`);
      setMessage(result.message || `Migration rerun started for ${runId}.`);
      setShowRerunActions(true);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Could not start migration rerun.');
    } finally {
      setRerunLoading(false);
    }
  }

  async function loadRecord(runId: string) {
    try {
      const result = await getJson<ApiItemResponse>(`${API_BASE}/migration-history/${encodeURIComponent(runId)}`);
      if (result.status && result.status !== 'success') {
        throw new Error(result.message || 'Could not load migration run.');
      }
      setSelectedRecord(result.item || null);
    } catch {
      setSelectedRecord(null);
    }
  }

  function handleLogout() {
    logout();
    window.location.replace('/login');
  }

  function resetFilters() {
    setFilters({
      search: '',
      status: '',
      objectType: '',
      dateFrom: '',
      dateTo: '',
    });
    setPage(1);
  }

  function exportHistory(format: 'json' | 'csv') {
    if (!filteredItems.length) return;
    if (format === 'csv') {
      const rows = [
        ['run_id', 'status', 'source_db', 'target_db', 'started_at', 'completed_at', 'total_objects', 'success_objects', 'error_objects', 'skipped_objects', 'total_rows_migrated', 'total_source_rows', 'total_target_rows', 'total_missing_rows', 'total_retries'],
      ];
      filteredItems.forEach((item) => {
        const run = item.run_summary || {};
        const stats = run.stats || {};
        rows.push([
          run.run_id || '',
          String(run.status || ''),
          run.source_db || '',
          run.target_db || '',
          run.started_at || '',
          run.completed_at || '',
          String(stats.total_objects || 0),
          String(stats.success_objects || 0),
          String(stats.error_objects || 0),
          String(stats.skipped_objects || 0),
          String(stats.total_rows_migrated || 0),
          String(stats.total_source_rows || 0),
          String(stats.total_target_rows || 0),
          String(stats.total_missing_rows || 0),
          String(stats.total_retries || 0),
        ]);
      });
      const csv = rows.map((row) => row.map((cell) => `"${String(cell).replace(/"/g, '""')}"`).join(',')).join('\n');
      downloadContent(csv, `migration-history-${Date.now()}.csv`, 'text/csv');
      return;
    }
    downloadContent(JSON.stringify(filteredItems, null, 2), `migration-history-${Date.now()}.json`, 'application/json');
  }

  if (!ready) return null;

  const run = selectedRecord?.run_summary || {};
  const stats = run.stats || {};
  const allObjectResults = run.object_results || [];
  const errorResults = allObjectResults.filter((item) => item.status === 'error');
  const detailObjectResults = errorOnly ? errorResults : allObjectResults;
  const rerunStudioHref = selectedRecord?.run_summary?.run_id
    ? `/migration-studio?runId=${encodeURIComponent(selectedRecord.run_summary.run_id)}`
    : '/migration-studio';

  return (
    <div className="history-page">
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
          <Link className="nav-btn" href="/home" title="Home">
            <i className="fa-solid fa-house"></i>
          </Link>
          <Link className="nav-btn" href="/migration-studio" title="Migrator">
            <span className="nav-icon-chip migrator-icon-chip" aria-hidden="true">
              <span className="migrator-glyph">
                <i className="fa-solid fa-database migrator-db migrator-db-top"></i>
                <i className="fa-solid fa-database migrator-db migrator-db-bottom"></i>
                <i className="fa-solid fa-arrow-right migrator-arrow migrator-arrow-top"></i>
                <i className="fa-solid fa-arrow-left migrator-arrow migrator-arrow-bottom"></i>
              </span>
            </span>
          </Link>
          <button className="nav-btn" type="button" title="Logout" onClick={handleLogout}>
            <i className="fa-solid fa-right-from-bracket"></i>
          </button>
        </div>
      </div>

      <div className="page">
        <section className="hero">
          <PageHeader
            title={
              <>
                Migration <span>History</span>
              </>
            }
            description="Review run catalogs, failure patterns, and object-level execution results from an enterprise analytics workspace for engineering operations."
          />
        </section>

        <section className="history-grid">
          <div className="history-shell">
            <div className="history-head">
              <div>
                <div className="history-title">Run Catalog</div>
                <div className="history-sub">Refresh, export, and filter recent migration executions.</div>
              </div>
              <div className="console-actions">
                <button className="btn" type="button" onClick={() => void loadMigrationHistory()}>
                  <i className="fa-solid fa-rotate"></i> Refresh
                </button>
                <button className="btn" type="button" onClick={() => exportHistory('json')}>
                  <i className="fa-regular fa-file-code"></i> JSON
                </button>
                <button className="btn" type="button" onClick={() => exportHistory('csv')}>
                  <i className="fa-solid fa-file-csv"></i> CSV
                </button>
              </div>
            </div>
            <div className="history-body">
              <div className="history-metrics">
                {[
                  ['Runs', metrics.runs],
                  ['Objects', metrics.objects],
                  ['Success', metrics.success],
                  ['Errors', metrics.error],
                  ['Rows', metrics.rows],
                  ['Missing', metrics.missing],
                ].map(([label, value]) => (
                  <div key={String(label)} className="metric-card">
                    <div className="metric-label">{label}</div>
                    <div className="metric-value">{value}</div>
                  </div>
                ))}
              </div>

              <div className="history-filter-box">
                <div className="history-filter-head">
                  <div>
                    <div className="history-filter-title">Filter Menu</div>
                    <div className="history-filter-help">Narrow by run id, status, object family, or time window.</div>
                  </div>
                  <button className="btn" type="button" onClick={resetFilters}>
                    <i className="fa-solid fa-filter-circle-xmark"></i> Clear Filters
                  </button>
                </div>

                <div className="history-filters">
                  <div className="filter-field filter-span-2">
                    <label htmlFor="historySearch">Search</label>
                    <input
                      id="historySearch"
                      className="input"
                      type="text"
                      placeholder="Search by run id, source DB, target DB, or object name"
                      value={filters.search}
                      onChange={(event) => {
                        setFilters((current) => ({ ...current, search: event.target.value.toLowerCase() }));
                        setPage(1);
                      }}
                    />
                  </div>
                  <div className="filter-field">
                    <label htmlFor="historyStatusFilter">Status</label>
                    <select
                      id="historyStatusFilter"
                      value={filters.status}
                      onChange={(event) => {
                        setFilters((current) => ({ ...current, status: event.target.value }));
                        setPage(1);
                      }}
                    >
                      <option value="">All Statuses</option>
                      <option value="success">Success</option>
                      <option value="partial">Partial</option>
                      <option value="error">Error</option>
                      <option value="skipped">Skipped</option>
                    </select>
                  </div>
                  <div className="filter-field">
                    <label htmlFor="historyTypeFilter">Object Type</label>
                    <select
                      id="historyTypeFilter"
                      value={filters.objectType}
                      onChange={(event) => {
                        setFilters((current) => ({ ...current, objectType: event.target.value }));
                        setPage(1);
                      }}
                    >
                      <option value="">All Object Types</option>
                      {ALL_OBJECT_TYPES.map((type) => (
                        <option key={type} value={type}>
                          {type}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div className="filter-field filter-span-2">
                    <label>Date Range</label>
                    <div className="date-range">
                      <input
                        className="input"
                        type="date"
                        value={filters.dateFrom}
                        onChange={(event) => {
                          setFilters((current) => ({ ...current, dateFrom: event.target.value }));
                          setPage(1);
                        }}
                      />
                      <input
                        className="input"
                        type="date"
                        value={filters.dateTo}
                        onChange={(event) => {
                          setFilters((current) => ({ ...current, dateTo: event.target.value }));
                          setPage(1);
                        }}
                      />
                    </div>
                  </div>
                </div>
              </div>

              <div className="section-label">Run Catalog</div>
              {message ? (
                <div className="history-empty">
                  {message}
                  {showRerunActions ? (
                    <span className="history-inline-actions">
                      <Link className="btn btn-primary" href={rerunStudioHref}>
                        <i className="fa-solid fa-wave-square"></i> Open Migration Studio
                      </Link>
                      <Link className="btn" href="/jobs-schedule">
                        <i className="fa-solid fa-list-check"></i> Open Jobs
                      </Link>
                    </span>
                  ) : null}
                </div>
              ) : null}
              {loading ? <div className="history-empty">Loading migration history...</div> : null}
              <div className="history-list">
                {!loading && pagedItems.length === 0 ? <div className="history-empty">No migration runs stored yet.</div> : null}
                {pagedItems.map((item) => {
                  const currentRun = item.run_summary || {};
                  const currentStats = currentRun.stats || {};
                  return (
                    <div
                      key={currentRun.run_id || `${currentRun.started_at}-${currentRun.target_db}`}
                      className={`history-item-shell${selectedRunId === currentRun.run_id ? ' active' : ''}`}
                    >
                      <button
                        type="button"
                        className={`history-item${selectedRunId === currentRun.run_id ? ' active' : ''}`}
                        onClick={() => setSelectedRunId(currentRun.run_id || '')}
                      >
                        <div className="history-item-top">
                          <div className="history-run-id">{currentRun.run_id || 'unknown-run'}</div>
                          <div className={`history-status ${getStatusClass(currentRun.status)}`}>{currentRun.status || 'unknown'}</div>
                        </div>
                        <div className="history-meta">
                          {currentRun.source_db || '--'} to {currentRun.target_db || '--'}
                        </div>
                        <div className="history-meta">
                          Start: {formatDateTime(currentRun.started_at)} | End: {formatDateTime(currentRun.completed_at)} | Duration:{' '}
                          {formatDuration(currentRun.started_at, currentRun.completed_at)}
                        </div>
                        <div className="history-item-bottom">
                          <div className="history-object-line">{summarizeByType(currentStats.by_type) || 'No object counters'}</div>
                          <div className="history-statline">
                            <span className="history-stat history-objects">Objects {currentStats.total_objects || 0}</span>
                            <span className="history-stat history-success">Success {currentStats.success_objects || 0}</span>
                            <span className="history-stat history-errors">Errors {currentStats.error_objects || 0}</span>
                            <span className="history-stat history-rows">Rows {currentStats.total_rows_migrated || 0}</span>
                            <span className="history-stat history-errors">Missing {currentStats.total_missing_rows || 0}</span>
                          </div>
                        </div>
                      </button>
                      <div className="history-item-actions">
                        <button className="btn btn-secondary" type="button" onClick={() => setQueriesRunId(currentRun.run_id || '')} disabled={!currentRun.run_id}>
                          <i className="fa-solid fa-code"></i> View Transformed Queries
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>

              <div className="history-pagination">
                <div className="history-page-info">
                  Page {safePage} of {totalPages} | {filteredItems.length} run(s)
                </div>
                <div className="console-actions">
                  <button className="btn" type="button" disabled={safePage <= 1} onClick={() => setPage((current) => Math.max(1, current - 1))}>
                    Previous
                  </button>
                  <button className="btn" type="button" disabled={safePage >= totalPages} onClick={() => setPage((current) => Math.min(totalPages, current + 1))}>
                    Next
                  </button>
                </div>
              </div>
            </div>
          </div>

          <div className="history-detail">
            <div className="history-head">
              <div>
                <div className="history-title">Run Details</div>
                <div className="history-sub">Stats, object results, and stored logs</div>
              </div>
              <div className="console-actions">
                <button
                  className="btn btn-primary"
                  type="button"
                  onClick={() => void rerunSelectedMigration()}
                  disabled={!selectedRecord || !selectedRecord.run_summary?.run_id || !canOperateMigrations || rerunLoading}
                >
                  <i className="fa-solid fa-rotate-right"></i> {rerunLoading ? 'Starting...' : 'Rerun Migration'}
                </button>
                <button className={`btn${errorOnly ? ' btn-primary' : ''}`} type="button" onClick={() => setErrorOnly((current) => !current)}>
                  <i className="fa-solid fa-triangle-exclamation"></i> Errors Only
                </button>
                <div className={`history-status ${getStatusClass(run.status)}`}>{run.status ? String(run.status).toUpperCase() : 'IDLE'}</div>
              </div>
            </div>
            <div className="history-detail-body">
              {!selectedRecord ? (
                <div className="history-empty">Select a migration run to inspect its full details.</div>
              ) : (
                <>
                  <div className="detail-section">
                    <div className="detail-kv">
                      <div className="detail-box">
                        <strong>Run ID</strong>
                        <div className="detail-code">{run.run_id || '--'}</div>
                      </div>
                      <div className="detail-box">
                        <strong>Window</strong>
                        <div className="detail-code">{formatDateTime(run.started_at)} to {formatDateTime(run.completed_at)}</div>
                      </div>
                      <div className="detail-box">
                        <strong>Duration</strong>
                        <div className="detail-code">{formatDuration(run.started_at, run.completed_at)}</div>
                      </div>
                      <div className="detail-box">
                        <strong>Source to Target</strong>
                        <div className="detail-code">{run.source_db || '--'} to {run.target_db || '--'}</div>
                      </div>
                      <div className="detail-box span-2">
                        <strong>Execution Order</strong>
                        <div className="detail-code">{(run.execution_order || []).join(' -> ') || '--'}</div>
                      </div>
                    </div>
                  </div>
                  <div className="detail-section">
                    <div className="detail-label">Run Totals</div>
                    <div className="detail-stats">
                      {[
                        ['Objects', stats.total_objects || 0],
                        ['Success', stats.success_objects || 0],
                        ['Errors', stats.error_objects || 0],
                        ['Skipped', stats.skipped_objects || 0],
                        ['Rows', stats.total_rows_migrated || 0],
                        ['Source Count', stats.total_source_rows || 0],
                        ['Target Count', stats.total_target_rows || 0],
                        ['Missing', stats.total_missing_rows || 0],
                        ['Retries', stats.total_retries || 0],
                      ].map(([label, value]) => (
                        <div key={String(label)} className="metric-card">
                          <div className="metric-label">{label}</div>
                          <div className="metric-value">{value}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="detail-section">
                    <div className="detail-label">Objects {errorOnly ? '(Errors Only)' : ''}</div>
                    <div className="detail-table">
                      {detailObjectResults.length ? (
                        detailObjectResults.map((item, index) => (
                          <div key={`${item.object_type}-${item.object_name}-${index}`} className="detail-row">
                            <div className="detail-row-main">{item.object_type || '--'} :: {item.object_name || '--'}</div>
                            <div className="detail-row-chips">
                              <div className="detail-chip">{item.retry_count || 0} retry</div>
                              <div className="detail-chip">{item.rows_migrated || 0} rows</div>
                              <div className="detail-chip">Source {item.source_row_count ?? '--'}</div>
                              <div className="detail-chip">Target {item.target_row_count ?? '--'}</div>
                              <div className="detail-chip">Missing {item.missing_row_count ?? '--'}</div>
                              <div className="detail-chip">{formatDuration(item.started_at, item.completed_at)}</div>
                              <div className={`detail-status ${getStatusClass(item.status)}`}>{item.status || 'unknown'}</div>
                            </div>
                          </div>
                        ))
                      ) : (
                        <div className="history-empty">{errorOnly ? 'No error objects in this run.' : 'No object results recorded.'}</div>
                      )}
                    </div>
                  </div>
                  <div className="detail-section">
                    <div className="detail-label">Missing Record Summary</div>
                    <div className="detail-table">
                      {allObjectResults.filter((item) => Number(item.missing_row_count || 0) > 0).length ? (
                        allObjectResults
                          .filter((item) => Number(item.missing_row_count || 0) > 0)
                          .map((item, index) => (
                            <div key={`missing-${item.object_name}-${index}`} className="detail-box">
                              <strong>{item.object_type || '--'} :: {item.object_name || '--'}</strong>
                              <div className="detail-code">
                                Source count: {item.source_row_count ?? '--'} | Target count: {item.target_row_count ?? '--'} | Missing records: {item.missing_row_count ?? '--'}
                              </div>
                            </div>
                          ))
                      ) : (
                        <div className="history-empty">No missing records detected in this run.</div>
                      )}
                    </div>
                  </div>
                  <div className="detail-section">
                    <div className="detail-label">Error Summary</div>
                    <div className="detail-table">
                      {errorResults.length ? (
                        errorResults.map((item, index) => (
                          <div key={`error-${item.object_name}-${index}`} className="detail-box">
                            <strong>{item.object_type || '--'} :: {item.object_name || '--'}</strong>
                            <div className="detail-code">{item.error_message || item.remediation || 'No error message stored.'}</div>
                          </div>
                        ))
                      ) : (
                        <div className="history-empty">This run has no recorded errors.</div>
                      )}
                    </div>
                  </div>
                  <div className="detail-section">
                    <div className="detail-label">Stored Logs</div>
                    <div className="detail-box">
                      <div className="history-log-box">{(selectedRecord.logs || []).join('\n') || 'No logs stored.'}</div>
                    </div>
                  </div>
                </>
              )}
            </div>
          </div>
        </section>
      </div>

      {queriesRunId ? <QueriesViewer runId={queriesRunId} onClose={() => setQueriesRunId('')} /> : null}
    </div>
  );
}
