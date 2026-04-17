'use client';

import type { ConnectionProfile } from '@/lib/migrationStudio';
import type { RoleKey } from '@/lib/rbac';

const API_BASE = '/api';

interface ConnectionListResponse {
  items?: ConnectionProfile[];
  detail?: string;
}

interface ConnectionSaveResponse {
  item?: ConnectionProfile;
  message?: string;
  detail?: string;
}

interface ConnectionDeleteResponse {
  message?: string;
  detail?: string;
}

export async function listConnectionProfiles(actorRole: RoleKey): Promise<ConnectionProfile[]> {
  const response = await fetch(`${API_BASE}/connections?actor_role=${encodeURIComponent(actorRole)}`, {
    cache: 'no-store',
  });
  const payload = (await response.json().catch(() => null)) as ConnectionListResponse | null;
  if (!response.ok) {
    throw new Error(payload?.detail || 'Could not load connection profiles.');
  }
  return payload?.items || [];
}

export async function saveConnectionProfile(params: {
  id?: string | null;
  name: string;
  engine: string;
  fields: Record<string, string>;
  actorRole: RoleKey;
  actorUsername?: string;
}): Promise<{ item: ConnectionProfile | null; message: string }> {
  const response = await fetch(`${API_BASE}/connections`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id: params.id || null,
      name: params.name.trim(),
      engine: params.engine,
      fields: params.fields,
      actor_role: params.actorRole,
      actor_username: params.actorUsername || null,
    }),
  });
  const payload = (await response.json().catch(() => null)) as ConnectionSaveResponse | null;
  if (!response.ok) {
    throw new Error(payload?.detail || 'Could not save connection profile.');
  }
  return {
    item: payload?.item || null,
    message: payload?.message || 'Connection profile saved successfully.',
  };
}

export async function deleteConnectionProfile(profileId: string, actorRole: RoleKey): Promise<string> {
  const response = await fetch(
    `${API_BASE}/connections/${encodeURIComponent(profileId)}?actor_role=${encodeURIComponent(actorRole)}`,
    {
      method: 'DELETE',
    }
  );
  const payload = (await response.json().catch(() => null)) as ConnectionDeleteResponse | null;
  if (!response.ok) {
    throw new Error(payload?.detail || 'Could not delete connection profile.');
  }
  return payload?.message || 'Connection profile deleted successfully.';
}
