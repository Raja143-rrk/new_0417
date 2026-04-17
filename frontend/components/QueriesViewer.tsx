'use client';

import { useEffect, useMemo, useState } from 'react';
import './QueriesViewer.css';

const API_BASE = 'http://127.0.0.1:8000/api';

interface QueryObject {
  name?: string;
  type?: string;
  query?: string;
}

interface QueriesResponse {
  status?: string;
  message?: string;
  full_script?: string;
  objects?: QueryObject[];
}

interface QueriesViewerProps {
  runId: string;
  onClose: () => void;
}

type TabKey = 'full' | 'objects';

export default function QueriesViewer({ runId, onClose }: QueriesViewerProps) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [fullScript, setFullScript] = useState('');
  const [objects, setObjects] = useState<QueryObject[]>([]);
  const [activeTab, setActiveTab] = useState<TabKey>('full');
  const [expandedKeys, setExpandedKeys] = useState<Record<string, boolean>>({});

  useEffect(() => {
    let cancelled = false;

    const loadQueries = async () => {
      setLoading(true);
      setError('');
      try {
        const response = await fetch(`${API_BASE}/migration/${encodeURIComponent(runId)}/queries`, {
          cache: 'no-store',
        });
        const data = (await response.json().catch(() => null)) as QueriesResponse | null;
        if (!response.ok || (data?.status && data.status !== 'success')) {
          throw new Error(data?.message || `HTTP ${response.status}`);
        }
        if (cancelled) return;
        const nextObjects = data?.objects || [];
        setFullScript(data?.full_script || '');
        setObjects(nextObjects);
        setExpandedKeys(
          nextObjects.reduce<Record<string, boolean>>((acc, item, index) => {
            if (index === 0) {
              acc[`${item.type || 'object'}:${item.name || index}`] = true;
            }
            return acc;
          }, {})
        );
      } catch (loadError) {
        if (!cancelled) {
          setFullScript('');
          setObjects([]);
          setError(loadError instanceof Error ? loadError.message : 'Could not load transformed queries.');
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    void loadQueries();

    return () => {
      cancelled = true;
    };
  }, [runId]);

  const hasQueries = useMemo(() => Boolean(fullScript.trim()) || objects.length > 0, [fullScript, objects]);

  const handleCopy = async () => {
    if (!fullScript.trim()) return;
    try {
      await navigator.clipboard.writeText(fullScript);
    } catch (error) {
      console.error('Could not copy SQL script', error);
    }
  };

  const toggleObject = (key: string) => {
    setExpandedKeys((current) => ({ ...current, [key]: !current[key] }));
  };

  return (
    <div className="queries-viewer-backdrop" role="presentation" onClick={onClose}>
      <div className="queries-viewer" role="dialog" aria-modal="true" aria-label="Transformed Queries" onClick={(event) => event.stopPropagation()}>
        <div className="queries-viewer-head">
          <div>
            <div className="queries-viewer-title">Transformed Queries</div>
            <div className="queries-viewer-subtitle">Run {runId}</div>
          </div>
          <button className="btn" type="button" onClick={onClose}>
            <i className="fa-solid fa-xmark"></i> Close
          </button>
        </div>

        <div className="queries-viewer-tabs">
          <button className={`queries-tab${activeTab === 'full' ? ' active' : ''}`} type="button" onClick={() => setActiveTab('full')}>
            Full Script
          </button>
          <button className={`queries-tab${activeTab === 'objects' ? ' active' : ''}`} type="button" onClick={() => setActiveTab('objects')}>
            Object-wise Queries
          </button>
        </div>

        <div className="queries-viewer-body">
          {loading ? <div className="queries-loading">Loading transformed queries...</div> : null}
          {!loading && error ? <div className="queries-error">{error}</div> : null}
          {!loading && !error && !hasQueries ? <div className="queries-empty">No transformed queries were stored for this run.</div> : null}

          {!loading && !error && hasQueries && activeTab === 'full' ? (
            <>
              <div className="queries-viewer-toolbar">
                <button className="btn" type="button" onClick={handleCopy} disabled={!fullScript.trim()}>
                  <i className="fa-regular fa-copy"></i> Copy
                </button>
              </div>
              <pre className="queries-code">{fullScript || 'No full script available.'}</pre>
            </>
          ) : null}

          {!loading && !error && hasQueries && activeTab === 'objects' ? (
            <div className="queries-object-list">
              {objects.map((item, index) => {
                const key = `${item.type || 'object'}:${item.name || index}`;
                const expanded = Boolean(expandedKeys[key]);
                return (
                  <div key={key} className="queries-object-card">
                    <button className="queries-object-head" type="button" onClick={() => toggleObject(key)}>
                      <div>
                        <div className="queries-object-title">{item.name || 'Unnamed object'}</div>
                        <div className="queries-object-meta">{item.type || 'object'}</div>
                      </div>
                      <i className={`fa-solid ${expanded ? 'fa-chevron-up' : 'fa-chevron-down'}`}></i>
                    </button>
                    {expanded ? (
                      <div className="queries-object-body">
                        <pre className="queries-code">{item.query || 'No transformed query stored.'}</pre>
                      </div>
                    ) : null}
                  </div>
                );
              })}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
