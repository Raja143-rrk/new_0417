'use client';

import Link from 'next/link';
import { useEffect, useMemo, useState } from 'react';
import PageHeader from '@/components/PageHeader';
import { canAccessPage, getSession, hasPermission, logout } from '@/lib/rbac';
import './jobs-schedule.css';

const API_BASE = '/api';

interface TaskRecord {
  object_type: string;
  object_name: string;
  status: string;
  source_db: string;
  target_db: string;
  rows_migrated?: number;
  source_row_count?: number;
  target_row_count?: number;
  missing_row_count?: number;
}

interface ScheduleRecord {
  trigger_type?: 'scheduled_trigger' | 'event_trigger';
  enabled?: boolean;
  description?: string | null;
  timezone?: string | null;
  cron_expression?: string | null;
  start_at?: string | null;
  event_name?: string | null;
  last_triggered_at?: string | null;
  trigger_count?: number;
  last_run_status?: string | null;
  next_run_at?: string | null;
  last_run_id?: string | null;
}

interface JobRecord {
  job_id: string;
  job_name: string;
  status: string;
  source_db: string;
  target_db: string;
  task_count: number;
  successful_tasks: number;
  failed_tasks: number;
  skipped_tasks: number;
  run_id?: string;
  started_at?: string;
  completed_at?: string;
  tasks?: TaskRecord[];
  schedule?: ScheduleRecord | null;
}

interface ApiJobsResponse {
  status?: string;
  message?: string;
  items?: JobRecord[];
  item?: JobRecord;
}

function formatDate(value?: string | null): string {
  if (!value) return '--';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
}

function toDateTimeLocalValue(value?: string | null): string {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  const pad = (input: number) => String(input).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function getStatusClass(status?: string): string {
  const key = String(status || '').toLowerCase();
  if (key === 'success') return 'status-success';
  if (key === 'error') return 'status-error';
  if (key === 'partial') return 'status-partial';
  return 'status-stopped';
}

function getTaskTypeSummary(tasks?: TaskRecord[]): string {
  const counts: Record<string, number> = {};
  (tasks || []).forEach((task) => {
    counts[task.object_type] = (counts[task.object_type] || 0) + 1;
  });
  return Object.entries(counts)
    .map(([key, value]) => `${key}: ${value}`)
    .join(' | ') || 'No task details available.';
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

async function getJson<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(url, options);
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

export default function JobsSchedulePage() {
  const [ready, setReady] = useState(false);
  const [sessionRole, setSessionRole] = useState<'admin' | 'operator' | 'viewer'>('viewer');
  const [jobs, setJobs] = useState<JobRecord[]>([]);
  const [selectedJobId, setSelectedJobId] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [triggerFilter, setTriggerFilter] = useState('');
  const [search, setSearch] = useState('');
  const [scheduleState, setScheduleState] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [scheduleForm, setScheduleForm] = useState({
    trigger_type: 'scheduled_trigger',
    enabled: true,
    description: '',
    timezone: 'Asia/Calcutta',
    cron_expression: '0 0/30 * * * ?',
    start_at: '',
    event_name: '',
  });

  const canManageSchedules = hasPermission('manageSchedules', sessionRole);
  const canOperateMigrations = hasPermission('operateMigrations', sessionRole);
  const isViewOnly = hasPermission('viewOnly', sessionRole);

  useEffect(() => {
    const session = getSession();
    if (!session.isAuthenticated) {
      window.location.replace('/login');
      return;
    }
    if (!canAccessPage('jobs', session.role)) {
      window.location.replace('/home');
      return;
    }
    setSessionRole(session.role);
    setReady(true);
  }, []);

  useEffect(() => {
    if (!ready) return;
    void loadJobs();
  }, [ready]);

  const filteredJobs = useMemo(() => {
    return jobs.filter((job) => {
      const text = [job.job_id, job.job_name, job.source_db, job.target_db, ...(job.tasks || []).map((task) => `${task.object_type} ${task.object_name}`)]
        .join(' ')
        .toLowerCase();
      const scheduleType = job.schedule?.trigger_type || 'none';
      return (!search || text.includes(search.toLowerCase())) && (!statusFilter || job.status.toLowerCase() === statusFilter) && (!triggerFilter || scheduleType === triggerFilter);
    });
  }, [jobs, search, statusFilter, triggerFilter]);

  const selectedJob = useMemo(() => filteredJobs.find((job) => job.job_id === selectedJobId) || null, [filteredJobs, selectedJobId]);

  useEffect(() => {
    if (!filteredJobs.length) {
      setSelectedJobId('');
      return;
    }
    if (!filteredJobs.some((job) => job.job_id === selectedJobId)) {
      setSelectedJobId(filteredJobs[0].job_id);
    }
  }, [filteredJobs, selectedJobId]);

  useEffect(() => {
    if (!selectedJob) return;
    setScheduleForm({
      trigger_type: selectedJob.schedule?.trigger_type || 'scheduled_trigger',
      enabled: selectedJob.schedule?.enabled ?? true,
      description: selectedJob.schedule?.description || '',
      timezone: selectedJob.schedule?.timezone || 'Asia/Calcutta',
      cron_expression: selectedJob.schedule?.cron_expression || '0 0/30 * * * ?',
      start_at: toDateTimeLocalValue(selectedJob.schedule?.start_at),
      event_name: selectedJob.schedule?.event_name || '',
    });
    setScheduleState(
      selectedJob.schedule
        ? `Current template schedule saved. Last trigger: ${formatDate(selectedJob.schedule.last_triggered_at)} | Trigger count: ${selectedJob.schedule.trigger_count || 0} | Last run status: ${selectedJob.schedule.last_run_status || '--'} | Next run: ${formatDate(selectedJob.schedule.next_run_at)}`
        : 'No schedule is attached to this template yet.'
    );
  }, [selectedJob]);

  useEffect(() => {
    if (!ready || !selectedJobId) return;
    void refreshSelectedJob(selectedJobId);
  }, [ready, selectedJobId]);

  async function loadJobs() {
    setLoading(true);
    setError('');
    try {
      const data = await getJson<ApiJobsResponse>(`${API_BASE}/jobs?limit=100`);
      if (data.status && data.status !== 'success') {
        throw new Error(data.message || 'Could not load jobs.');
      }
      setJobs(data.items || []);
    } catch (loadError) {
      setJobs([]);
      setError(loadError instanceof Error ? loadError.message : 'Could not load jobs.');
    } finally {
      setLoading(false);
    }
  }

  async function refreshSelectedJob(jobId: string) {
    const data = await getJson<ApiJobsResponse>(`${API_BASE}/jobs/${encodeURIComponent(jobId)}`);
    const incoming = data.item;
    if (!incoming) return;
    setJobs((current) => {
      const index = current.findIndex((job) => job.job_id === jobId);
      if (index === -1) return current;
      const next = [...current];
      next[index] = incoming;
      return next;
    });
  }

  async function saveSchedule() {
    if (!selectedJob || !canManageSchedules) return;
    const payload = {
      trigger_type: scheduleForm.trigger_type,
      enabled: scheduleForm.enabled,
      description: scheduleForm.description.trim() || null,
      timezone: scheduleForm.timezone.trim() || 'Asia/Calcutta',
      cron_expression: scheduleForm.cron_expression.trim() || null,
      start_at: scheduleForm.start_at || null,
      event_name: scheduleForm.event_name.trim() || null,
    };
    const result = await getJson<ApiJobsResponse>(`${API_BASE}/jobs/${encodeURIComponent(selectedJob.job_id)}/schedule`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    setScheduleState(result.message || 'Schedule saved.');
    await refreshSelectedJob(selectedJob.job_id);
  }

  async function recordEventTrigger() {
    if (!selectedJob || !canManageSchedules) return;
    const eventName = scheduleForm.event_name.trim();
    const url = `${API_BASE}/jobs/${encodeURIComponent(selectedJob.job_id)}/event-trigger${eventName ? `?event_name=${encodeURIComponent(eventName)}` : ''}`;
    const result = await getJson<ApiJobsResponse>(url, { method: 'POST' });
    setScheduleState(result.message || 'Event trigger recorded.');
    await refreshSelectedJob(selectedJob.job_id);
  }

  async function rerunJob() {
    if (!selectedJob || !canOperateMigrations) return;
    const result = await getJson<ApiJobsResponse>(`${API_BASE}/jobs/${encodeURIComponent(selectedJob.job_id)}/rerun`, { method: 'POST' });
    setScheduleState(result.message || 'Job rerun started.');
    window.setTimeout(() => {
      void loadJobs();
    }, 1500);
  }

  function exportJobs(format: 'json' | 'csv') {
    if (!filteredJobs.length) return;
    if (format === 'csv') {
      const rows = [
        ['job_id', 'job_name', 'status', 'source_db', 'target_db', 'task_count', 'successful_tasks', 'failed_tasks', 'skipped_tasks', 'trigger_type', 'cron_expression', 'event_name', 'last_run_id', 'last_run_status', 'next_run_at'],
      ];
      filteredJobs.forEach((job) => {
        rows.push([
          job.job_id,
          job.job_name,
          job.status,
          job.source_db,
          job.target_db,
          String(job.task_count),
          String(job.successful_tasks),
          String(job.failed_tasks),
          String(job.skipped_tasks),
          job.schedule?.trigger_type || '',
          job.schedule?.cron_expression || '',
          job.schedule?.event_name || '',
          job.schedule?.last_run_id || '',
          job.schedule?.last_run_status || '',
          job.schedule?.next_run_at || '',
        ]);
      });
      const csv = rows.map((row) => row.map((cell) => `"${String(cell ?? '').replace(/"/g, '""')}"`).join(',')).join('\n');
      downloadContent(csv, `jobs-schedule-${Date.now()}.csv`, 'text/csv');
      return;
    }
    downloadContent(JSON.stringify(filteredJobs, null, 2), `jobs-schedule-${Date.now()}.json`, 'application/json');
  }

  function handleLogout() {
    logout();
    window.location.replace('/login');
  }

  if (!ready) return null;

  return (
    <div className="jobs-page">
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
          <Link className="nav-btn" href="/home" title="Home" aria-label="Home">
            <i className="fa-solid fa-house"></i>
          </Link>
          <Link className="nav-btn" href="/migration-history" title="Run History" aria-label="Run History">
            <i className="fa-solid fa-chart-line"></i>
          </Link>
          <button className="nav-btn" type="button" title="Logout" aria-label="Logout" onClick={handleLogout}>
            <i className="fa-solid fa-right-from-bracket"></i>
          </button>
        </div>
      </div>

      <div className="page">
        <section className="hero">
          <PageHeader
            title={
              <>
                Jobs &amp; <span>Schedule</span>
              </>
            }
            description="Review jobs, schedules, and latest runs."
          />
        </section>

        <section className="shell">
          <div className="panel">
            <div className="panel-head">
              <div>
                <div className="panel-title">Job Templates</div>
                <div className="panel-sub">Runs, status, and schedule.</div>
              </div>
              <div className="toolbar">
                <input className="input search" type="text" placeholder="Search source, target, run id..." value={search} onChange={(event) => setSearch(event.target.value)} />
                <select className="select" value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
                  <option value="">All Statuses</option>
                  <option value="success">Success</option>
                  <option value="partial">Partial</option>
                  <option value="error">Error</option>
                  <option value="stopped">Stopped</option>
                </select>
                <select className="select" value={triggerFilter} onChange={(event) => setTriggerFilter(event.target.value)}>
                  <option value="">All Triggers</option>
                  <option value="scheduled_trigger">Scheduled Trigger</option>
                  <option value="event_trigger">Event Trigger</option>
                  <option value="none">No Schedule</option>
                </select>
                <button className="btn" type="button" onClick={() => exportJobs('json')}>
                  <i className="fa-regular fa-file-code"></i> JSON
                </button>
                <button className="btn" type="button" onClick={() => exportJobs('csv')}>
                  <i className="fa-solid fa-file-csv"></i> CSV
                </button>
                <button className="btn" type="button" onClick={() => void loadJobs()}>
                  <i className="fa-solid fa-rotate"></i> Refresh
                </button>
              </div>
            </div>
            <div className="panel-body">
              <div className="job-list">
                {loading ? <div className="empty">Loading jobs...</div> : null}
                {error ? <div className="empty">{error}</div> : null}
                {!loading && !error && !filteredJobs.length ? <div className="empty">No jobs match the current filter.</div> : null}
                {filteredJobs.map((job) => {
                  const schedule = job.schedule;
                  const scheduleLabel = !schedule
                    ? 'No schedule'
                    : schedule.trigger_type === 'event_trigger'
                      ? `Event: ${schedule.event_name || 'configured'}`
                      : `Cron: ${schedule.cron_expression || 'configured'}`;
                  return (
                    <button key={job.job_id} type="button" className={`job-card${job.job_id === selectedJobId ? ' active' : ''}`} onClick={() => setSelectedJobId(job.job_id)}>
                      <div className="job-top">
                        <div className="job-name">{job.job_name}</div>
                        <div className={`chip ${getStatusClass(job.status)}`}>{job.status}</div>
                      </div>
                      <div className="job-meta">
                        Template ID: {job.job_id}
                        <br />
                        Latest execution window: {formatDate(job.started_at)} to {formatDate(job.completed_at)}
                      </div>
                      <div className="chip-row job-chips">
                        <div className="chip">{job.task_count} task(s)</div>
                        <div className="chip">{job.successful_tasks} success</div>
                        <div className="chip">{job.failed_tasks} failed</div>
                        <div className="chip">{scheduleLabel}</div>
                      </div>
                    </button>
                  );
                })}
              </div>
            </div>
          </div>

          <div className="panel">
            <div className="panel-head">
              <div>
                <div className="panel-title">Template Details</div>
                <div className="panel-sub">
                  {selectedJob ? `${selectedJob.source_db} to ${selectedJob.target_db} | ${selectedJob.task_count} task(s) | ${selectedJob.run_id || 'No run id'}` : 'Select a job to view details.'}
                </div>
              </div>
            </div>
            <div className="panel-body">
              {!selectedJob ? (
                <div className="empty">Select a migration job from the left catalog.</div>
              ) : (
                <>
                  <div className="section">
                    <div className="metric-grid">
                      {[
                        ['Job Status', selectedJob.status],
                        ['Tasks', selectedJob.task_count],
                        ['Successful', selectedJob.successful_tasks],
                        ['Failed', selectedJob.failed_tasks],
                      ].map(([label, value]) => (
                        <div key={String(label)} className="metric">
                          <div className="metric-label">{label}</div>
                          <div className="metric-value">{value}</div>
                        </div>
                      ))}
                    </div>
                    <div className="note">
                      <strong>Summary:</strong> latest run includes <strong>{selectedJob.task_count}</strong> task(s). Breakdown: <strong>{getTaskTypeSummary(selectedJob.tasks)}</strong>.
                    </div>
                  </div>

                  <div className="section">
                    <h3 className="section-title">Schedule Designer</h3>
                    <div className="schedule-grid">
                      <div className="field">
                        <label htmlFor="triggerType">Trigger Type</label>
                        <select
                          id="triggerType"
                          className="select"
                          value={scheduleForm.trigger_type}
                          disabled={!canManageSchedules}
                          onChange={(event) => setScheduleForm((current) => ({ ...current, trigger_type: event.target.value as 'scheduled_trigger' | 'event_trigger' }))}
                        >
                          <option value="scheduled_trigger">Scheduled Trigger</option>
                          <option value="event_trigger">Event Trigger</option>
                        </select>
                      </div>
                      <div className="field">
                        <label htmlFor="scheduleEnabled">Status</label>
                        <select
                          id="scheduleEnabled"
                          className="select"
                          value={scheduleForm.enabled ? 'true' : 'false'}
                          disabled={!canManageSchedules}
                          onChange={(event) => setScheduleForm((current) => ({ ...current, enabled: event.target.value === 'true' }))}
                        >
                          <option value="true">Enabled</option>
                          <option value="false">Disabled</option>
                        </select>
                      </div>
                      {scheduleForm.trigger_type !== 'event_trigger' ? (
                        <div className="field full">
                          <label htmlFor="cronExpression">Cron Expression</label>
                          <input
                            id="cronExpression"
                            className="input"
                            value={scheduleForm.cron_expression}
                            disabled={!canManageSchedules}
                            onChange={(event) => setScheduleForm((current) => ({ ...current, cron_expression: event.target.value }))}
                          />
                        </div>
                      ) : (
                        <div className="field full">
                          <label htmlFor="eventName">Event Name</label>
                          <input
                            id="eventName"
                            className="input"
                            value={scheduleForm.event_name}
                            placeholder="e.g. source-schema-ready"
                            disabled={!canManageSchedules}
                            onChange={(event) => setScheduleForm((current) => ({ ...current, event_name: event.target.value }))}
                          />
                        </div>
                      )}
                      <div className="field">
                        <label htmlFor="scheduleTimezone">Timezone</label>
                        <input
                          id="scheduleTimezone"
                          className="input"
                          value={scheduleForm.timezone}
                          disabled={!canManageSchedules}
                          onChange={(event) => setScheduleForm((current) => ({ ...current, timezone: event.target.value }))}
                        />
                      </div>
                      <div className="field">
                        <label htmlFor="scheduleStartAt">Start At</label>
                        <input
                          id="scheduleStartAt"
                          type="datetime-local"
                          className="input"
                          value={scheduleForm.start_at}
                          disabled={!canManageSchedules}
                          onChange={(event) => setScheduleForm((current) => ({ ...current, start_at: event.target.value }))}
                        />
                      </div>
                      <div className="field full">
                        <label htmlFor="scheduleDescription">Description</label>
                        <textarea
                          id="scheduleDescription"
                          className="textarea"
                          value={scheduleForm.description}
                          disabled={!canManageSchedules}
                          onChange={(event) => setScheduleForm((current) => ({ ...current, description: event.target.value }))}
                        />
                      </div>
                    </div>

                    <div className="hint">Use cron or event trigger.</div>

                    <div className="schedule-actions">
                      <button className="btn" type="button" onClick={() => void rerunJob()} disabled={!canOperateMigrations}>
                        <i className="fa-solid fa-play"></i> Run Template Now
                      </button>
                      <button className="btn btn-primary" type="button" onClick={() => void saveSchedule()} disabled={!canManageSchedules}>
                        <i className="fa-solid fa-floppy-disk"></i> Save Schedule
                      </button>
                      {scheduleForm.trigger_type === 'event_trigger' ? (
                        <button className="btn" type="button" onClick={() => void recordEventTrigger()} disabled={!canManageSchedules}>
                          <i className="fa-solid fa-bolt"></i> Record Event Trigger
                        </button>
                      ) : null}
                    </div>

                    <div className="hint schedule-state">{scheduleState}</div>
                    {isViewOnly ? <div className="note top-gap">Viewer access is read-only.</div> : null}
                  </div>

                  <div className="section">
                    <h3 className="section-title">Latest Tasks</h3>
                    <div className="task-list">
                      {(selectedJob.tasks || []).length ? (
                        selectedJob.tasks?.map((task, index) => (
                          <div key={`${task.object_type}-${task.object_name}-${index}`} className="task-row">
                            <div className="task-head">
                              <div className="task-main">{task.object_type} :: {task.object_name}</div>
                              <div className={`chip ${getStatusClass(task.status)}`}>{task.status}</div>
                            </div>
                            <div className="task-sub">
                              Source: {task.source_db} | Target: {task.target_db} | Rows migrated: {task.rows_migrated || 0} | Source count: {task.source_row_count ?? '--'} | Target count: {task.target_row_count ?? '--'} | Missing: {task.missing_row_count ?? '--'}
                            </div>
                          </div>
                        ))
                      ) : (
                        <div className="empty">No task records available for this job.</div>
                      )}
                    </div>
                  </div>
                </>
              )}
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}
