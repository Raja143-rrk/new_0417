(function () {
  'use strict';

  const USERS_KEY = 'dbm_users';
  const AUTH_KEY = 'dbm_auth';
  const USER_KEY = 'dbm_user';
  const ROLE_KEY = 'dbm_role';

  const ROLE_DEFINITIONS = {
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
        viewOnly: false
      }
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
        viewOnly: false
      }
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
        viewOnly: true
      }
    }
  };

  const LEGACY_ROLE_MAP = {
    'Migration Admin': 'admin',
    'Migration Operator': 'operator',
    Viewer: 'viewer',
    admin: 'admin',
    operator: 'operator',
    viewer: 'viewer'
  };

  const DEFAULT_USERS = {
    admin: { password: 'Migrator@123', role: 'admin', home: 'home.html' },
    operator: { password: 'Welcome@123', role: 'operator', home: 'home.html' },
    viewer: { password: 'Viewer@123', role: 'viewer', home: 'home.html' }
  };

  function normalizeRole(roleValue) {
    return LEGACY_ROLE_MAP[String(roleValue || '').trim()] || 'viewer';
  }

  function roleLabel(roleValue) {
    const roleKey = normalizeRole(roleValue);
    return ROLE_DEFINITIONS[roleKey].label;
  }

  function normalizeUserRecord(userRecord) {
    const role = normalizeRole(userRecord?.role);
    return {
      password: userRecord?.password || '',
      role,
      home: userRecord?.home || 'home.html'
    };
  }

  function bootstrapUsers() {
    const existing = localStorage.getItem(USERS_KEY);
    if (!existing) {
      localStorage.setItem(USERS_KEY, JSON.stringify(DEFAULT_USERS));
      return { ...DEFAULT_USERS };
    }
    try {
      const parsed = JSON.parse(existing);
      const normalized = Object.fromEntries(
        Object.entries(parsed || {}).map(([username, meta]) => [username, normalizeUserRecord(meta)])
      );
      if (!normalized.admin) normalized.admin = { ...DEFAULT_USERS.admin };
      if (!normalized.operator) normalized.operator = { ...DEFAULT_USERS.operator };
      if (!normalized.viewer) normalized.viewer = { ...DEFAULT_USERS.viewer };
      localStorage.setItem(USERS_KEY, JSON.stringify(normalized));
      return normalized;
    } catch (_) {
      localStorage.setItem(USERS_KEY, JSON.stringify(DEFAULT_USERS));
      return { ...DEFAULT_USERS };
    }
  }

  function getUsers() {
    return bootstrapUsers();
  }

  function saveUsers(users) {
    const normalized = Object.fromEntries(
      Object.entries(users || {}).map(([username, meta]) => [username, normalizeUserRecord(meta)])
    );
    localStorage.setItem(USERS_KEY, JSON.stringify(normalized));
  }

  function getSession() {
    const username = sessionStorage.getItem(USER_KEY) || '';
    const role = normalizeRole(sessionStorage.getItem(ROLE_KEY));
    return {
      isAuthenticated: sessionStorage.getItem(AUTH_KEY) === 'true',
      username,
      role,
      roleLabel: roleLabel(role)
    };
  }

  function login(username, password) {
    const users = getUsers();
    const user = users[username];
    if (!user || user.password !== password) return null;
    sessionStorage.setItem(AUTH_KEY, 'true');
    sessionStorage.setItem(USER_KEY, username);
    sessionStorage.setItem(ROLE_KEY, normalizeRole(user.role));
    return {
      username,
      role: normalizeRole(user.role),
      roleLabel: roleLabel(user.role),
      home: user.home || 'home.html'
    };
  }

  function logout() {
    sessionStorage.removeItem(AUTH_KEY);
    sessionStorage.removeItem(USER_KEY);
    sessionStorage.removeItem(ROLE_KEY);
  }

  function requireAuth(redirectTo) {
    if (getSession().isAuthenticated) return true;
    window.location.replace(redirectTo || 'login.html');
    return false;
  }

  function canAccessPage(pageKey, roleValue) {
    const roleKey = normalizeRole(roleValue || getSession().role);
    return ROLE_DEFINITIONS[roleKey].pages.includes(pageKey);
  }

  function requirePageAccess(pageKey, fallback) {
    if (!requireAuth('login.html')) return false;
    if (canAccessPage(pageKey)) return true;
    window.location.replace(fallback || 'home.html');
    return false;
  }

  function hasPermission(permissionKey, roleValue) {
    const roleKey = normalizeRole(roleValue || getSession().role);
    return Boolean(ROLE_DEFINITIONS[roleKey].permissions[permissionKey]);
  }

  window.DBM_RBAC = {
    ROLE_DEFINITIONS,
    DEFAULT_USERS,
    normalizeRole,
    roleLabel,
    getUsers,
    saveUsers,
    getSession,
    login,
    logout,
    requireAuth,
    canAccessPage,
    requirePageAccess,
    hasPermission
  };
})();
