'use client';

import { useState, useEffect } from 'react';
import PageHeader from '@/components/PageHeader';
import { deleteConnectionProfile, listConnectionProfiles, saveConnectionProfile } from '@/lib/connectionProfiles';
import { RoleKey } from '@/lib/rbac';
import './connections.css';

interface ConnectionField {
  id: string;
  label: string;
  type: string;
  ph: string;
  def?: string;
}

interface ConnectionProfile {
  id: string;
  name: string;
  engine: string;
  fields: Record<string, string>;
}

interface ConnectionTestResponse {
  status?: string;
  message?: string;
}

const DB_SCHEMAS: Record<string, ConnectionField[]> = {
  sqlserver: [
    { id: 'host', label: 'Host', type: 'text', ph: 'e.g. 192.168.1.10' },
    { id: 'port', label: 'Port', type: 'number', ph: '1433', def: '1433' },
    { id: 'username', label: 'Username', type: 'text', ph: 'sa' },
    { id: 'password', label: 'Password', type: 'password', ph: '********' },
  ],
  mysql: [
    { id: 'host', label: 'Host', type: 'text', ph: 'localhost' },
    { id: 'port', label: 'Port', type: 'number', ph: '3306', def: '3306' },
    { id: 'username', label: 'Username', type: 'text', ph: 'root' },
    { id: 'password', label: 'Password', type: 'password', ph: '********' },
  ],
  snowflake: [
    { id: 'account', label: 'Account / Host', type: 'text', ph: 'xy12345.eu-north-1.aws' },
    { id: 'warehouse', label: 'Warehouse', type: 'text', ph: 'COMPUTE_WH' },
    { id: 'role', label: 'Role', type: 'text', ph: 'SYSADMIN' },
    { id: 'username', label: 'Username', type: 'text', ph: 'my_user' },
    { id: 'password', label: 'Password', type: 'password', ph: '********' },
  ],
  postgresql: [
    { id: 'host', label: 'Host', type: 'text', ph: 'localhost' },
    { id: 'port', label: 'Port', type: 'number', ph: '5432', def: '5432' },
    { id: 'username', label: 'Username', type: 'text', ph: 'postgres' },
    { id: 'password', label: 'Password', type: 'password', ph: '********' },
  ],
  oracle: [
    { id: 'host', label: 'Host / TNS', type: 'text', ph: 'oracle-host or ORCL' },
    { id: 'port', label: 'Port', type: 'number', ph: '1521', def: '1521' },
    { id: 'username', label: 'Username', type: 'text', ph: 'system' },
    { id: 'password', label: 'Password', type: 'password', ph: '********' },
  ],
  mongodb: [{ id: 'uri', label: 'Connection URI', type: 'text', ph: 'mongodb+srv://user:pass@cluster.mongodb.net' }],
  redshift: [
    { id: 'host', label: 'Cluster Endpoint', type: 'text', ph: 'cluster.region.redshift.amazonaws.com' },
    { id: 'port', label: 'Port', type: 'number', ph: '5439', def: '5439' },
    { id: 'username', label: 'Username', type: 'text', ph: 'awsuser' },
    { id: 'password', label: 'Password', type: 'password', ph: '********' },
  ],
  azuresql: [
    { id: 'host', label: 'Server Name', type: 'text', ph: 'server.database.windows.net' },
    { id: 'port', label: 'Port', type: 'number', ph: '1433', def: '1433' },
    { id: 'username', label: 'Username', type: 'text', ph: 'sqladmin@server' },
    { id: 'password', label: 'Password', type: 'password', ph: '********' },
  ],
};

const DB_LABELS: Record<string, string> = {
  sqlserver: 'SQL Server',
  mysql: 'MySQL',
  oracle: 'Oracle',
  snowflake: 'Snowflake',
  postgresql: 'PostgreSQL',
  mongodb: 'MongoDB',
  redshift: 'Amazon Redshift',
  azuresql: 'Azure SQL',
};

export default function ConnectionsPage() {
  const [mounted, setMounted] = useState(false);
  const [sessionInfo, setSessionInfo] = useState<any>(null);
  const [connectionName, setConnectionName] = useState('');
  const [selectedEngine, setSelectedEngine] = useState('');
  const [formFields, setFormFields] = useState<Record<string, string>>({});
  const [connections, setConnections] = useState<ConnectionProfile[]>([]);
  const [selectedConnection, setSelectedConnection] = useState<ConnectionProfile | null>(null);
  const [panelOpen, setPanelOpen] = useState(false);
  const [filterEngine, setFilterEngine] = useState('');
  const [editorStatus, setEditorStatus] = useState('New Profile');
  const [testingConnection, setTestingConnection] = useState(false);
  const [message, setMessage] = useState<{ type: 'ok' | 'err', text: string } | null>(null);

  useEffect(() => {
    const isAuth = sessionStorage.getItem('dbm_auth') === 'true';
    if (!isAuth) {
      window.location.replace('/login');
      return;
    }

    const username = sessionStorage.getItem('dbm_user') || 'user';
    const role = sessionStorage.getItem('dbm_role') || 'viewer';

    const roleLabels: { [key: string]: string } = {
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
  }, []);

  useEffect(() => {
    if (!mounted || !sessionInfo?.role) return;

    let cancelled = false;

    const loadConnections = async () => {
      try {
        const items = await listConnectionProfiles(sessionInfo.role as RoleKey);
        if (!cancelled) {
          setConnections(items);
        }
      } catch (error) {
        if (!cancelled) {
          setMessage({ type: 'err', text: error instanceof Error ? error.message : 'Could not load connections.' });
        }
      }
    };

    void loadConnections();

    return () => {
      cancelled = true;
    };
  }, [mounted, sessionInfo]);

  useEffect(() => {
    if (!message) return;
    const timeout = window.setTimeout(() => {
      setMessage(null);
    }, 5000);

    return () => window.clearTimeout(timeout);
  }, [message]);

  const handleEngineChange = (engine: string) => {
    setSelectedEngine(engine);
    setFormFields({});
    setEditorStatus('New Profile');
    setConnectionName('');
  };

  const handleFieldChange = (fieldId: string, value: string) => {
    setFormFields(prev => ({
      ...prev,
      [fieldId]: value,
    }));
  };

  const handleTestConnection = async () => {
    if (!selectedEngine) {
      setMessage({ type: 'err', text: 'Please select an engine.' });
      return;
    }

    setTestingConnection(true);
    try {
      const response = await fetch('/api/test-connection', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          database_type: selectedEngine,
          connection_details: formFields,
        }),
      });

      const data = (await response.json().catch(() => null)) as ConnectionTestResponse | null;

      if (response.ok && data?.status === 'success') {
        setMessage({ type: 'ok', text: data.message || 'Connection tested successfully!' });
      } else {
        setMessage({ type: 'err', text: data?.message || 'Connection test failed.' });
      }
    } catch (error) {
      setMessage({ type: 'err', text: 'Connection test error: ' + String(error) });
    } finally {
      setTestingConnection(false);
    }
  };

  const handleSaveConnection = async () => {
    if (!connectionName || !selectedEngine) {
      setMessage({ type: 'err', text: 'Please fill in connection name and engine.' });
      return;
    }

    try {
      const result = await saveConnectionProfile({
        id: selectedConnection?.id || null,
        name: connectionName,
        engine: selectedEngine,
        fields: formFields,
        actorRole: (sessionInfo?.role || 'viewer') as RoleKey,
        actorUsername: sessionInfo?.username,
      });
      const savedProfile = result.item;
      if (savedProfile) {
        setConnections((current) => {
          const existingIndex = current.findIndex((item) => item.id === savedProfile.id);
          if (existingIndex === -1) {
            return [...current, savedProfile].sort((a, b) => a.name.localeCompare(b.name));
          }
          const next = [...current];
          next[existingIndex] = savedProfile;
          return next.sort((a, b) => a.name.localeCompare(b.name));
        });
      }
      resetForm();
      setPanelOpen(true);
      setMessage({ type: 'ok', text: result.message });
    } catch (error) {
      setMessage({ type: 'err', text: error instanceof Error ? error.message : 'Could not save connection profile.' });
    }
  };

  const handleResetForm = () => {
    resetForm();
  };

  const resetForm = () => {
    setConnectionName('');
    setSelectedEngine('');
    setFormFields({});
    setEditorStatus('New Profile');
    setSelectedConnection(null);
  };

  const handleSelectConnection = (conn: ConnectionProfile) => {
    setSelectedConnection(conn);
    setConnectionName(conn.name);
    setSelectedEngine(conn.engine);
    setFormFields(conn.fields);
    setEditorStatus('Editing Profile');
  };

  const handleDeleteConnection = async (id: string) => {
    try {
      const result = await deleteConnectionProfile(id, (sessionInfo?.role || 'viewer') as RoleKey);
      setConnections((current) => current.filter((c) => c.id !== id));
      if (selectedConnection?.id === id) {
        resetForm();
      }
      setMessage({ type: 'ok', text: result });
    } catch (error) {
      setMessage({ type: 'err', text: error instanceof Error ? error.message : 'Could not delete connection profile.' });
    }
  };

  const handleLogout = () => {
    sessionStorage.removeItem('dbm_auth');
    sessionStorage.removeItem('dbm_user');
    sessionStorage.removeItem('dbm_role');
    window.location.replace('/login');
  };

  if (!mounted || !sessionInfo) return null;

  const filteredConnections = filterEngine
    ? connections.filter(c => c.engine === filterEngine)
    : connections;

  const currentSchema = selectedEngine ? DB_SCHEMAS[selectedEngine] || [] : [];

  return (
    <div className="connections-page">
      {/* Topbar */}
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
          <a className="nav-btn" href="/home" title="Home" aria-label="Home">
            <i className="fa-solid fa-house"></i>
          </a>
          <a className="nav-btn" href="/migration-studio" title="Migrator" aria-label="Migrator">
            <span className="nav-icon-chip migrator-icon-chip" aria-hidden="true">
              <span className="migrator-glyph">
                <i className="fa-solid fa-database migrator-db migrator-db-top"></i>
                <i className="fa-solid fa-database migrator-db migrator-db-bottom"></i>
                <i className="fa-solid fa-arrow-right migrator-arrow migrator-arrow-top"></i>
                <i className="fa-solid fa-arrow-left migrator-arrow migrator-arrow-bottom"></i>
              </span>
            </span>
          </a>
          <button className="nav-btn" type="button" title="Logout" aria-label="Logout" onClick={handleLogout}>
            <i className="fa-solid fa-right-from-bracket"></i>
          </button>
        </div>
      </div>

      {/* Main Content */}
      <div className="page">
        {/* Hero Section */}
        <section className="hero">
          <PageHeader
            title={
              <>
                Manage reusable <span className="title-accent">database</span> connections
              </>
            }
            description="Create and maintain named connection profiles once, then select them from source and target dropdowns on the migration page. The migration workflow, metadata loading, and object selection remain unchanged."
          />
        </section>

        {/* Layout */}
        <section className="layout">
          {/* Editor Card */}
          <div className="card">
            <div className="card-head">
              <div>
                <div className="card-title">Connection Editor</div>
                <div className="card-sub">Create or update saved connection profiles for all configured database engines.</div>
              </div>
              <div className="status">
                <i className="fa-solid fa-circle"></i> {editorStatus}
              </div>
            </div>

            <div className="card-body">
              <form
                onSubmit={e => {
                  e.preventDefault();
                  handleSaveConnection();
                }}
              >
                <div className="form-row">
                  <div className="form-group">
                    <label htmlFor="connectionName">Connection Name</label>
                    <input
                      className="input"
                      id="connectionName"
                      placeholder="e.g. Snowflake QA"
                      value={connectionName}
                      onChange={e => setConnectionName(e.target.value)}
                    />
                  </div>
                  <div className="form-group">
                    <label htmlFor="connectionEngine">Database Engine</label>
                    <select value={selectedEngine} onChange={e => handleEngineChange(e.target.value)}>
                      <option value="">-- Select engine --</option>
                      {Object.entries(DB_LABELS).map(([key, label]) => (
                        <option key={key} value={key}>
                          {label}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                {/* Dynamic Fields */}
                {selectedEngine && currentSchema.length > 0 && (
                  <div className="dynamic-fields">
                    <div className="form-row">
                      {currentSchema.map(field => (
                        <div key={field.id} className="form-group">
                          <label htmlFor={field.id}>{field.label}</label>
                          <input
                            className="input"
                            id={field.id}
                            type={field.type}
                            placeholder={field.ph}
                            value={formFields[field.id] || ''}
                            onChange={e => handleFieldChange(field.id, e.target.value)}
                          />
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                <div className="btn-row">
                  <button
                    className="btn"
                    type="button"
                    onClick={handleTestConnection}
                    disabled={testingConnection || !selectedEngine}
                  >
                    <i className="fa-solid fa-plug-circle-bolt"></i> Test Connection
                  </button>
                  <button className="btn btn-primary" type="submit">
                    <i className="fa-regular fa-floppy-disk"></i> Save Connection
                  </button>
                  <button className="btn" type="button" onClick={handleResetForm}>
                    <i className="fa-solid fa-rotate-left"></i> Reset
                  </button>
                </div>

                {message && (
                  <div className={`msg ${message.type}`}>
                    {message.type === 'ok' ? <i className="fa-solid fa-check"></i> : <i className="fa-solid fa-exclamation"></i>} {message.text}
                  </div>
                )}
              </form>
            </div>
          </div>

          {/* Sidebar Card */}
          <aside className="card">
            <div className="card-head">
              <div>
                <div className="card-title">Saved Connections</div>
                <div className="card-sub">Profiles stored locally on this browser.</div>
              </div>
            </div>

            <div className="card-body">
              <div className="form-group">
                <label htmlFor="filterEngine">Filter by Engine</label>
                <select value={filterEngine} onChange={e => setFilterEngine(e.target.value)}>
                  <option value="">All Engines</option>
                  {Object.entries(DB_LABELS).map(([key, label]) => (
                    <option key={key} value={key}>
                      {label}
                    </option>
                  ))}
                </select>
              </div>

              <div className="btn-row">
                <button
                  className="btn"
                  type="button"
                  onClick={() => setPanelOpen(!panelOpen)}
                >
                  <i className="fa-solid fa-link"></i> Existing Connections
                </button>
              </div>

              {panelOpen && (
                <div className="connection-panel">
                  <div className="connection-list">
                    {filteredConnections.length === 0 ? (
                      <div className="empty">No saved connections yet.</div>
                    ) : (
                      filteredConnections.map(conn => (
                        <button
                          key={conn.id}
                          className={`connection-item ${selectedConnection?.id === conn.id ? 'active' : ''}`}
                          onClick={() => handleSelectConnection(conn)}
                          type="button"
                        >
                          <div className="connection-name">{conn.name}</div>
                          <div className="connection-meta">{DB_LABELS[conn.engine] || conn.engine}</div>
                          <div className="connection-actions">
                            <button
                              className="btn btn-sm"
                              onClick={e => {
                                e.stopPropagation();
                                handleSelectConnection(conn);
                              }}
                              type="button"
                            >
                              <i className="fa-solid fa-pencil"></i> Edit
                            </button>
                            <button
                              className="btn btn-sm btn-danger"
                              onClick={e => {
                                e.stopPropagation();
                                handleDeleteConnection(conn.id);
                              }}
                              type="button"
                            >
                              <i className="fa-solid fa-trash"></i> Delete
                            </button>
                          </div>
                        </button>
                      ))
                    )}
                  </div>
                </div>
              )}

            </div>
          </aside>
        </section>
      </div>
    </div>
  );
}
