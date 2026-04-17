'use client';

import Link from 'next/link';
import { FormEvent, useEffect, useState } from 'react';
import { canAccessPage, createUser, deleteUser, getSession, getUsers, hasPermission, logout, RoleKey, roleLabel, updateUser, UserRecord } from '@/lib/rbac';
import './add-user.css';

type FormRole = 'Migration Operator' | 'Migration Admin' | 'Viewer';

const roleToKey: Record<FormRole, RoleKey> = {
  'Migration Operator': 'operator',
  'Migration Admin': 'admin',
  Viewer: 'viewer',
};

export default function AddUserPage() {
  const [ready, setReady] = useState(false);
  const [username, setUsername] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [role, setRole] = useState<FormRole>('Migration Operator');
  const [sessionRole, setSessionRole] = useState<RoleKey>('viewer');
  const [sessionUsername, setSessionUsername] = useState('');
  const [users, setUsers] = useState<UserRecord[]>([]);
  const [showAddUserForm, setShowAddUserForm] = useState(false);
  const [showExistingUsers, setShowExistingUsers] = useState(false);
  const [loadingUsers, setLoadingUsers] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [editingUser, setEditingUser] = useState<UserRecord | null>(null);
  const [editEmail, setEditEmail] = useState('');
  const [editRole, setEditRole] = useState<RoleKey>('operator');
  const [okMsg, setOkMsg] = useState('');
  const [errMsg, setErrMsg] = useState('');

  useEffect(() => {
    const session = getSession();
    if (!session.isAuthenticated) {
      window.location.replace('/login');
      return;
    }
    if (!canAccessPage('user_management', session.role) || !hasPermission('manageUsers', session.role)) {
      window.location.replace('/home');
      return;
    }
    setSessionRole(session.role);
    setSessionUsername(session.username);
    setReady(true);
  }, []);

  useEffect(() => {
    if (!ready || !showExistingUsers || sessionRole !== 'admin') return;
    void loadUsers();
  }, [ready, sessionRole, showExistingUsers]);

  async function loadUsers() {
    setLoadingUsers(true);
    setErrMsg('');
    try {
      const items = await getUsers(sessionRole);
      setUsers(items);
    } catch (error) {
      setErrMsg(error instanceof Error ? error.message : 'Could not load existing users.');
    } finally {
      setLoadingUsers(false);
    }
  }

  function handleLogout() {
    logout();
    window.location.replace('/login');
  }

  function openEditModal(user: UserRecord) {
    setEditingUser(user);
    setEditEmail(user.email || '');
    setEditRole(user.role);
  }

  async function handleUpdateUser(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!editingUser) return;
    setErrMsg('');
    setOkMsg('');
    setSubmitting(true);
    try {
      const result = await updateUser({
        username: editingUser.username || '',
        email: editEmail,
        role: editRole,
        actorRole: sessionRole,
      });
      setOkMsg(result?.message || 'User updated successfully.');
      setEditingUser(null);
      await loadUsers();
    } catch (error) {
      setErrMsg(error instanceof Error ? error.message : 'Could not update user.');
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDeleteUser(user: UserRecord) {
    if (!user.username) return;
    const approved = window.confirm(`Delete user ${user.username}?`);
    if (!approved) return;
    setErrMsg('');
    setOkMsg('');
    try {
      const result = await deleteUser(user.username, sessionRole);
      setOkMsg(result?.message || 'User deleted successfully.');
      await loadUsers();
    } catch (error) {
      setErrMsg(error instanceof Error ? error.message : 'Could not delete user.');
    }
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setOkMsg('');
    setErrMsg('');

    const trimmedUsername = username.trim();
    const trimmedEmail = email.trim();
    if (!trimmedUsername || !password || !trimmedEmail) {
      setErrMsg('Username, user mail, and password are required.');
      return;
    }

    setSubmitting(true);
    try {
      const result = await createUser({
        username: trimmedUsername,
        password,
        email: trimmedEmail,
        role: roleToKey[role],
        actorRole: sessionRole,
        invitedBy: sessionUsername,
      });

      setUsername('');
      setEmail('');
      setPassword('');
      setRole('Migration Operator');
      setOkMsg(result?.message || 'User created successfully.');
      if (showExistingUsers) {
        await loadUsers();
      }
    } catch (error) {
      setErrMsg(error instanceof Error ? error.message : 'Could not create user.');
    } finally {
      setSubmitting(false);
    }
  }

  if (!ready) return null;

  return (
    <div className="add-user-page">
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
          <Link className="nav-btn" href="/migration-studio" title="Migrator" aria-label="Migrator">
            <span className="nav-icon-chip migrator-icon-chip" aria-hidden="true">
              <span className="migrator-glyph">
                <i className="fa-solid fa-database migrator-db migrator-db-top"></i>
                <i className="fa-solid fa-database migrator-db migrator-db-bottom"></i>
                <i className="fa-solid fa-arrow-right migrator-arrow migrator-arrow-top"></i>
                <i className="fa-solid fa-arrow-left migrator-arrow migrator-arrow-bottom"></i>
              </span>
            </span>
          </Link>
          <button className="nav-btn" type="button" title="Logout" aria-label="Logout" onClick={handleLogout}>
            <i className="fa-solid fa-right-from-bracket"></i>
          </button>
        </div>
      </div>

      <div className="page">
        <div className="grid">
          <section className="card">
            <div className="eyebrow">Access Administration</div>
            <h1>Access Administration</h1>

            <div className="action-row">
              <button className="btn secondary" type="button" onClick={() => setShowAddUserForm((current) => !current)}>
                <i className={`fa-solid ${showAddUserForm ? 'fa-angle-up' : 'fa-user-plus'}`}></i>
                {showAddUserForm ? 'Hide Add User' : 'Add User'}
              </button>
            </div>

            {showAddUserForm ? (
              <form onSubmit={(event) => void handleSubmit(event)}>
                <div className="form-group">
                  <label htmlFor="newUsername">Username</label>
                  <input
                    className="input"
                    id="newUsername"
                    value={username}
                    placeholder="Enter username"
                    onChange={(event) => setUsername(event.target.value)}
                    disabled={submitting}
                  />
                </div>

                <div className="form-group">
                  <label htmlFor="newUserEmail">User Mail</label>
                  <input
                    className="input"
                    id="newUserEmail"
                    type="email"
                    value={email}
                    placeholder="Enter user email"
                    onChange={(event) => setEmail(event.target.value)}
                    disabled={submitting}
                  />
                </div>

                <div className="form-group">
                  <label htmlFor="newPassword">Password</label>
                  <input
                    className="input"
                    id="newPassword"
                    type="password"
                    value={password}
                    placeholder="Enter password"
                    onChange={(event) => setPassword(event.target.value)}
                    disabled={submitting}
                  />
                </div>

                <div className="form-group">
                  <label htmlFor="newRole">Role</label>
                  <select id="newRole" value={role} onChange={(event) => setRole(event.target.value as FormRole)} disabled={submitting}>
                    <option value="Migration Operator">Migration Operator</option>
                    <option value="Migration Admin">Migration Admin</option>
                    <option value="Viewer">Viewer</option>
                  </select>
                </div>

                <div className="action-row">
                  <button className="btn primary" type="submit" disabled={submitting}>
                    <i className="fa-solid fa-user-plus"></i> {submitting ? 'Saving User...' : 'Save User'}
                  </button>
                </div>
              </form>
            ) : null}

            {okMsg ? <div className="msg ok">{okMsg}</div> : null}
            {errMsg ? <div className="msg err">{errMsg}</div> : null}
          </section>

          {sessionRole === 'admin' ? (
            <aside className="card">
              <div className="eyebrow">Admin Directory</div>
              <h2>Access Directory</h2>

              <div className="action-row">
                <button
                  className="btn secondary"
                  type="button"
                  onClick={() => setShowExistingUsers((current) => !current)}
                >
                  <i className={`fa-solid ${showExistingUsers ? 'fa-eye-slash' : 'fa-users'}`}></i>
                  {showExistingUsers ? 'Hide Existing Users' : 'Existing Users'}
                </button>
              </div>

              {showExistingUsers ? (
                <div className="user-list">
                  {loadingUsers ? <div className="empty">Loading users...</div> : null}
                  {!loadingUsers && !users.length ? <div className="empty">No user records found.</div> : null}
                  {!loadingUsers
                    ? users.map((user) => (
                        <div key={user.username || user.email} className="user-item">
                          <strong>{user.username}</strong>
                          <div>{user.email || '--'}</div>
                          <div>{roleLabel(user.role)}</div>
                          <div className="action-row user-actions">
                            <button className="btn secondary" type="button" onClick={() => openEditModal(user)}>
                              <i className="fa-solid fa-pen"></i> Edit
                            </button>
                            <button className="btn secondary" type="button" onClick={() => void handleDeleteUser(user)}>
                              <i className="fa-solid fa-trash"></i> Delete
                            </button>
                          </div>
                        </div>
                      ))
                    : null}
                </div>
              ) : null}
            </aside>
          ) : null}
        </div>

        {editingUser ? (
          <div className="modal-backdrop" role="presentation" onClick={() => setEditingUser(null)}>
            <div className="modal-card" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
              <div className="eyebrow">User Management</div>
              <h2>Edit User</h2>
              <form onSubmit={(event) => void handleUpdateUser(event)}>
                <div className="form-group">
                  <label>Username</label>
                  <input className="input" value={editingUser.username || ''} disabled />
                </div>
                <div className="form-group">
                  <label htmlFor="editEmail">User Mail</label>
                  <input id="editEmail" className="input" type="email" value={editEmail} onChange={(event) => setEditEmail(event.target.value)} />
                </div>
                <div className="form-group">
                  <label htmlFor="editRole">Role</label>
                  <select id="editRole" value={editRole} onChange={(event) => setEditRole(event.target.value as RoleKey)}>
                    <option value="operator">Migration Operator</option>
                    <option value="admin">Migration Admin</option>
                    <option value="viewer">Viewer</option>
                  </select>
                </div>
                <div className="action-row">
                  <button className="btn primary" type="submit" disabled={submitting}>
                    Save Changes
                  </button>
                  <button className="btn secondary" type="button" onClick={() => setEditingUser(null)}>
                    Cancel
                  </button>
                </div>
              </form>
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
