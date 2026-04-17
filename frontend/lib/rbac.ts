'use client';

export type RoleKey = 'admin' | 'operator' | 'viewer';

export interface UserRecord {
  username?: string;
  password: string;
  email?: string;
  role: RoleKey;
  home: string;
  created_at?: string;
  updated_at?: string;
}

export interface SessionInfo {
  isAuthenticated: boolean;
  username: string;
  role: RoleKey;
  roleLabel: string;
  email?: string;
}

const AUTH_KEY = 'dbm_auth';
const USER_KEY = 'dbm_user';
const ROLE_KEY = 'dbm_role';
const EMAIL_KEY = 'dbm_email';
const API_BASE = 'http://127.0.0.1:8000/api';

export const ROLE_DEFINITIONS: Record<
  RoleKey,
  {
    key: RoleKey;
    label: string;
    pages: string[];
    permissions: Record<string, boolean>;
  }
> = {
  admin: {
    key: 'admin',
    label: 'Migration Admin',
    pages: ['home', 'migrator', 'history', 'jobs', 'connections', 'advanced', 'user_management'],
    permissions: {
      manageUsers: true,
      manageConnections: true,
      useAdvancedConnectors: true,
      manageSchedules: true,
      operateMigrations: true,
      viewOnly: false,
    },
  },
  operator: {
    key: 'operator',
    label: 'Migration Operator',
    pages: ['home', 'migrator', 'history', 'jobs'],
    permissions: {
      manageUsers: false,
      manageConnections: false,
      useAdvancedConnectors: false,
      manageSchedules: true,
      operateMigrations: true,
      viewOnly: false,
    },
  },
  viewer: {
    key: 'viewer',
    label: 'Viewer',
    pages: ['home', 'migrator', 'history', 'jobs'],
    permissions: {
      manageUsers: false,
      manageConnections: false,
      useAdvancedConnectors: false,
      manageSchedules: false,
      operateMigrations: false,
      viewOnly: true,
    },
  },
};

const LEGACY_ROLE_MAP: Record<string, RoleKey> = {
  'Migration Admin': 'admin',
  'Migration Operator': 'operator',
  Viewer: 'viewer',
  admin: 'admin',
  operator: 'operator',
  viewer: 'viewer',
};

export function normalizeRole(roleValue: unknown): RoleKey {
  return LEGACY_ROLE_MAP[String(roleValue || '').trim()] || 'viewer';
}

export function roleLabel(roleValue: unknown): string {
  const key = normalizeRole(roleValue);
  return ROLE_DEFINITIONS[key].label;
}

function normalizeHomePath(homeValue: unknown): string {
  const raw = String(homeValue || '').trim();
  if (!raw) return '/home';

  const legacyMap: Record<string, string> = {
    'home.html': '/home',
    '/home.html': '/home',
    'login.html': '/login',
    '/login.html': '/login',
    'migration-history.html': '/migration-history',
    '/migration-history.html': '/migration-history',
    'jobs-schedule.html': '/jobs-schedule',
    '/jobs-schedule.html': '/jobs-schedule',
    'advanced-connectors.html': '/advanced-connectors',
    '/advanced-connectors.html': '/advanced-connectors',
    'add-user.html': '/add-user',
    '/add-user.html': '/add-user',
    'connection-manager.html': '/connections',
    '/connection-manager.html': '/connections',
    'database-migrator-dark.html': '/migration-studio',
    '/database-migrator-dark.html': '/migration-studio',
  };

  return legacyMap[raw] || raw;
}

function canUseStorage(): boolean {
  return typeof window !== 'undefined';
}

function storeSession(session: {
  username: string;
  role: RoleKey;
  roleLabel: string;
  home: string;
  email?: string;
}) {
  if (!canUseStorage()) return;
  window.sessionStorage.setItem(AUTH_KEY, 'true');
  window.sessionStorage.setItem(USER_KEY, session.username);
  window.sessionStorage.setItem(ROLE_KEY, normalizeRole(session.role));
  window.sessionStorage.setItem(EMAIL_KEY, session.email || '');
}

export function getSession(): SessionInfo {
  if (!canUseStorage()) {
    return {
      isAuthenticated: false,
      username: '',
      role: 'viewer',
      roleLabel: ROLE_DEFINITIONS.viewer.label,
      email: '',
    };
  }

  const username = window.sessionStorage.getItem(USER_KEY) || '';
  const role = normalizeRole(window.sessionStorage.getItem(ROLE_KEY));
  return {
    isAuthenticated: window.sessionStorage.getItem(AUTH_KEY) === 'true',
    username,
    role,
    roleLabel: roleLabel(role),
    email: window.sessionStorage.getItem(EMAIL_KEY) || '',
  };
}

export async function login(username: string, password: string) {
  const response = await fetch(`${API_BASE}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      username: username.trim(),
      password,
    }),
  });

  const payload = (await response.json().catch(() => null)) as
    | { item?: { username?: string; role?: RoleKey; role_label?: string; home?: string; email?: string }; detail?: string }
    | null;

  if (!response.ok || !payload?.item?.username || !payload.item.role) {
    return null;
  }

  const session = {
    username: payload.item.username,
    role: normalizeRole(payload.item.role),
    roleLabel: payload.item.role_label || roleLabel(payload.item.role),
    home: normalizeHomePath(payload.item.home),
    email: payload.item.email || '',
  };

  storeSession(session);
  return session;
}

export function logout(): void {
  if (!canUseStorage()) {
    return;
  }

  window.sessionStorage.removeItem(AUTH_KEY);
  window.sessionStorage.removeItem(USER_KEY);
  window.sessionStorage.removeItem(ROLE_KEY);
  window.sessionStorage.removeItem(EMAIL_KEY);
}

export async function getUsers(actorRole: RoleKey): Promise<UserRecord[]> {
  const response = await fetch(`${API_BASE}/users?actor_role=${encodeURIComponent(actorRole)}`, {
    cache: 'no-store',
  });
  const payload = (await response.json().catch(() => null)) as { items?: UserRecord[]; detail?: string } | null;
  if (!response.ok) {
    throw new Error(payload?.detail || 'Could not load users.');
  }
  return payload?.items || [];
}

export async function createUser(payload: {
  username: string;
  password: string;
  email: string;
  role: RoleKey;
  actorRole: RoleKey;
  invitedBy?: string;
}) {
  const response = await fetch(`${API_BASE}/users`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      username: payload.username.trim(),
      password: payload.password,
      email: payload.email.trim(),
      role: payload.role,
      actor_role: payload.actorRole,
      invited_by: payload.invitedBy || null,
    }),
  });
  const body = (await response.json().catch(() => null)) as
    | { status?: string; message?: string; item?: UserRecord; email_sent?: boolean; detail?: string }
    | null;
  if (!response.ok) {
    throw new Error(body?.detail || body?.message || 'Could not create user.');
  }
  return body;
}

export async function updateUser(payload: {
  username: string;
  email: string;
  role: RoleKey;
  actorRole: RoleKey;
}) {
  const response = await fetch(`${API_BASE}/users/${encodeURIComponent(payload.username)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      email: payload.email.trim(),
      role: payload.role,
      actor_role: payload.actorRole,
    }),
  });
  const body = (await response.json().catch(() => null)) as
    | { status?: string; message?: string; item?: UserRecord; detail?: string }
    | null;
  if (!response.ok) {
    throw new Error(body?.detail || body?.message || 'Could not update user.');
  }
  return body;
}

export async function deleteUser(username: string, actorRole: RoleKey) {
  const response = await fetch(
    `${API_BASE}/users/${encodeURIComponent(username)}?actor_role=${encodeURIComponent(actorRole)}`,
    {
      method: 'DELETE',
    }
  );
  const body = (await response.json().catch(() => null)) as
    | { status?: string; message?: string; detail?: string }
    | null;
  if (!response.ok) {
    throw new Error(body?.detail || body?.message || 'Could not delete user.');
  }
  return body;
}

export function canAccessPage(pageKey: string, roleValue?: unknown): boolean {
  const role = normalizeRole(roleValue || getSession().role);
  return ROLE_DEFINITIONS[role].pages.includes(pageKey);
}

export function hasPermission(permissionKey: string, roleValue?: unknown): boolean {
  const role = normalizeRole(roleValue || getSession().role);
  return Boolean(ROLE_DEFINITIONS[role].permissions[permissionKey]);
}
