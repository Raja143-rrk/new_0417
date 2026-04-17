'use client';

import { useEffect } from 'react';
import { applyTheme, getStoredThemePreference, subscribeToSystemTheme } from '@/lib/theme';

export default function ThemeInitializer() {
  useEffect(() => {
    applyTheme(getStoredThemePreference());
    return subscribeToSystemTheme(() => {
      applyTheme(getStoredThemePreference());
    });
  }, []);

  return null;
}
