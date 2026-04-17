'use client';

export type ThemePreference = 'auto' | 'light' | 'dark';
export type ResolvedTheme = 'light' | 'dark';

const STORAGE_KEY = 'dbm_theme_preference';

function getThemeQuery(): MediaQueryList | null {
  if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return null;
  return window.matchMedia('(prefers-color-scheme: dark)');
}

export function getStoredThemePreference(): ThemePreference {
  if (typeof window === 'undefined') return 'auto';
  const value = window.localStorage.getItem(STORAGE_KEY);
  return value === 'light' || value === 'dark' || value === 'auto' ? value : 'auto';
}

export function resolveTheme(preference: ThemePreference): ResolvedTheme {
  if (preference === 'light' || preference === 'dark') return preference;
  return getThemeQuery()?.matches ? 'dark' : 'light';
}

export function applyTheme(preference: ThemePreference) {
  if (typeof document === 'undefined') return;
  const resolved = resolveTheme(preference);
  const root = document.documentElement;
  const body = document.body;

  root.dataset.themePreference = preference;
  root.dataset.theme = resolved;
  body.dataset.themePreference = preference;
  body.dataset.theme = resolved;
  body.style.colorScheme = resolved;
}

export function setThemePreference(preference: ThemePreference) {
  if (typeof window !== 'undefined') {
    window.localStorage.setItem(STORAGE_KEY, preference);
  }
  applyTheme(preference);
}

export function subscribeToSystemTheme(listener: () => void) {
  const query = getThemeQuery();
  if (!query) return () => undefined;

  const handleChange = () => {
    if (getStoredThemePreference() === 'auto') {
      listener();
    }
  };

  if (typeof query.addEventListener === 'function') {
    query.addEventListener('change', handleChange);
    return () => query.removeEventListener('change', handleChange);
  }

  query.addListener(handleChange);
  return () => query.removeListener(handleChange);
}
