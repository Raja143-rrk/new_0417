'use client';

import { useState, useEffect } from 'react';
import PageHeader from '@/components/PageHeader';
import './home.css';

interface SessionInfo {
  isAuthenticated: boolean;
  username: string;
  roleLabel: string;
  role: string;
}

export default function HomePage() {
  const [mounted, setMounted] = useState(false);
  const [sessionInfo, setSessionInfo] = useState<SessionInfo | null>(null);
  const [visibleMenuItems, setVisibleMenuItems] = useState({
    userManagement: false,
    connections: false,
    advanced: false,
  });

  useEffect(() => {
    // Check authentication and RBAC
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
    });

    // Set role-based menu visibility
    const showAdvancedAndConnections = role === 'admin';
    const showUserManagement = role === 'admin';

    setVisibleMenuItems({
      userManagement: showUserManagement,
      connections: showAdvancedAndConnections,
      advanced: showAdvancedAndConnections,
    });

    setMounted(true);
  }, []);

  const handleLogout = () => {
    sessionStorage.removeItem('dbm_auth');
    sessionStorage.removeItem('dbm_user');
    sessionStorage.removeItem('dbm_role');
    window.location.replace('/login');
  };

  if (!mounted || !sessionInfo) return null;

  return (
    <div className="home-page">
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
          <div id="userLabel">
            {sessionInfo.username} | {sessionInfo.roleLabel}
          </div>
          <button className="btn btn-logout" onClick={handleLogout}>
            <i className="fa-solid fa-right-from-bracket"></i> Logout
          </button>
        </div>
      </div>

      {/* Main Content */}
      <div className="page-container">
        {/* Hero Section */}
        <section className="hero">
          <PageHeader
            title={
              <>
                Advanced <span className="highlight">Migration</span> Control Hub
              </>
            }
            description="Run AI-assisted schema and data migrations from one command center. The interface is optimized for high-trust execution, operational visibility, and clean separation between production workflows, advanced connectors, and run analytics."
            titleClassName="hero-title"
          />

          <div className="hero-stats">
            <div className="stat-card">
              <div className="stat-number">4</div>
              <div className="stat-label">Core Engines</div>
            </div>
            <div className="stat-card">
              <div className="stat-number">1</div>
              <div className="stat-label">Unified Workspace</div>
            </div>
            <div className="stat-card">
              <div className="stat-number">Local</div>
              <div className="stat-label">History + Stats</div>
            </div>
          </div>
        </section>

        <section className="workspace-panel">
          <div className="workspace-head">
            <div className="workspace-icon">
              <img src="/workspace-access-icon.png" alt="" className="workspace-icon-image" aria-hidden="true" />
            </div>
            <div>
              <h2>Workspace Access</h2>
              <p>Open the required migration workspace directly from one control panel.</p>
            </div>
          </div>

          <div className="workspace-actions">
            <a className="action-tile primary-action" href="/migration-studio">
              <span className="action-icon">
                <span className="action-icon-chip migrator-icon-chip" aria-hidden="true">
                  <span className="migrator-glyph">
                    <i className="fa-solid fa-database migrator-db migrator-db-top"></i>
                    <i className="fa-solid fa-database migrator-db migrator-db-bottom"></i>
                    <i className="fa-solid fa-arrow-right migrator-arrow migrator-arrow-top"></i>
                    <i className="fa-solid fa-arrow-left migrator-arrow migrator-arrow-bottom"></i>
                  </span>
                </span>
              </span>
              <span className="action-copy">
                <strong>Migration Studio</strong>
                <span>Open the main migration execution workspace.</span>
              </span>
            </a>

            <a className="action-tile" href="/jobs-schedule">
              <span className="action-icon">
                <i className="fa-solid fa-calendar-check"></i>
              </span>
              <span className="action-copy">
                <strong>Jobs &amp; Schedule</strong>
                <span>Manage templates, triggers, and reruns.</span>
              </span>
            </a>

            {visibleMenuItems.connections && (
              <a className="action-tile" href="/connections">
                <span className="action-icon">
                  <i className="fa-solid fa-plug-circle-plus"></i>
                </span>
                <span className="action-copy">
                  <strong>Connections</strong>
                  <span>Maintain saved source and target profiles.</span>
                </span>
              </a>
            )}

            {visibleMenuItems.advanced && (
              <a className="action-tile" href="/advanced-connectors">
                <span className="action-icon">
                  <i className="fa-solid fa-plug-circle-bolt"></i>
                </span>
                <span className="action-copy">
                  <strong>Advanced Connectors</strong>
                  <span>Inspect planned and partial adapter coverage.</span>
                </span>
              </a>
            )}

            <a className="action-tile" href="/migration-history">
              <span className="action-icon">
                <i className="fa-solid fa-chart-line"></i>
              </span>
              <span className="action-copy">
                <strong>Run History</strong>
                <span>Review runs, failures, and analytics.</span>
              </span>
            </a>

            {visibleMenuItems.userManagement && (
              <a className="action-tile" href="/add-user">
                <span className="action-icon">
                  <i className="fa-solid fa-user-plus"></i>
                </span>
                <span className="action-copy">
                  <strong>User Management</strong>
                  <span>Provision access and review existing users.</span>
                </span>
              </a>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}
