'use client';

import Link from 'next/link';
import { useEffect, useState } from 'react';
import PageHeader from '@/components/PageHeader';
import { canAccessPage, getSession, logout } from '@/lib/rbac';
import './advanced-connectors.css';

const sections = [
  {
    title: 'Experimental Relational',
    description: 'These engines are planned for adapter-based implementation, but they are not part of the supported production migration flow yet.',
    items: [
      { name: 'Oracle', badge: 'Planned', detail: 'Needs dedicated metadata, DDL extraction, PK lookup, preflight validation, and routine conversion support.' },
      { name: 'DB2', badge: 'Planned', detail: 'Requires adapter implementation for schema discovery, object extraction, and engine-specific data migration SQL.' },
      { name: 'Teradata', badge: 'Planned', detail: 'Needs connector, metadata APIs, type mapping, and target-aware transformation logic.' },
    ],
  },
  {
    title: 'Cloud And Non-Standard',
    description: 'These connectors either need partial-specialized support or a separate migration model from the current relational engine.',
    items: [
      { name: 'Amazon Redshift', badge: 'Partial', detail: 'Close to PostgreSQL, but should be stabilized as a separate adapter before production use.' },
      { name: 'Azure SQL', badge: 'Partial', detail: 'Close to SQL Server, but should be exposed only after adapter-specific testing and validation rules.' },
      { name: 'Google Cloud SQL', badge: 'Partial', detail: 'Depends on whether the instance is MySQL or PostgreSQL; production support should be split by backing engine.' },
      { name: 'MongoDB', badge: 'Separate Module', detail: 'Document databases need a different extraction and migration model than the current relational DDL engine.' },
    ],
  },
];

export default function AdvancedConnectorsPage() {
  const [ready, setReady] = useState(false);

  useEffect(() => {
    const session = getSession();
    if (!session.isAuthenticated) {
      window.location.replace('/login');
      return;
    }
    if (!canAccessPage('advanced', session.role)) {
      window.location.replace('/home');
      return;
    }
    setReady(true);
  }, []);

  if (!ready) return null;

  return (
    <div className="advanced-page">
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
          <button className="nav-btn" type="button" title="Logout" aria-label="Logout" onClick={() => { logout(); window.location.replace('/login'); }}>
            <i className="fa-solid fa-right-from-bracket"></i>
          </button>
        </div>
      </div>

      <div className="page">
        <section className="hero">
          <PageHeader
            title={
              <>
                Advanced <span>Connectors</span>
              </>
            }
            description="Experimental and custom connectors live here until they are production-ready."
          />
          <div className="hero-meta">
            <div className="hero-meta-card">
              <span className="hero-meta-label">Current Scope</span>
              <strong>Planned + Partial</strong>
            </div>
            <div className="hero-meta-card">
              <span className="hero-meta-label">Production Path</span>
              <strong>Adapter Validation</strong>
            </div>
            <div className="hero-meta-card">
              <span className="hero-meta-label">Primary Use</span>
              <strong>Roadmap Review</strong>
            </div>
          </div>
        </section>

        <section className="grid">
          {sections.map((section) => (
            <div key={section.title} className="card">
              <div className="card-head">
                <div>
                  <h3>{section.title}</h3>
                  <p>{section.description}</p>
                </div>
                <div className="section-pill">{section.items.length} Connectors</div>
              </div>
              <div className="connector-list">
                {section.items.map((item) => (
                  <div key={item.name} className="connector">
                    <div className="connector-head">
                      <div className="connector-name">{item.name}</div>
                      <div className="badge">{item.badge}</div>
                    </div>
                    <small>{item.detail}</small>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </section>

        <section className="note">
          <h3>Recommended Product Structure</h3>
          <p>Use the main migration page for production-supported engines only: MySQL, PostgreSQL, SQL Server, and Snowflake. Keep this advanced module for planned or experimental connectors until each adapter reaches the same quality bar.</p>
        </section>
      </div>
    </div>
  );
}
