import json
import threading
import time

from app.adapters.common import normalize_snowflake_account
from app.adapters.registry import get_adapter, normalize_db_type

SNOWFLAKE_CACHE_TTL_SECONDS = 300
_snowflake_cache_lock = threading.Lock()
_snowflake_connection_cache = {}


class CachedConnection:
    def __init__(self, connection):
        self._connection = connection

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def close(self):
        return None


def _close_connection_safely(connection):
    try:
        connection.close()
    except Exception:
        pass


def _cleanup_expired_snowflake_connections(now):
    expired_keys = [
        key for key, entry in _snowflake_connection_cache.items()
        if entry["expires_at"] <= now
    ]
    for key in expired_keys:
        _close_connection_safely(_snowflake_connection_cache[key]["connection"])
        del _snowflake_connection_cache[key]


def _build_snowflake_cache_key(details):
    normalized = dict(details)
    account = normalized.get("account") or normalized.get("host")
    normalized["account"] = normalize_snowflake_account(account)
    return json.dumps(normalized, sort_keys=True, default=str)


def get_connection(db_type, details):
    normalized_db_type = normalize_db_type(db_type)
    adapter = get_adapter(normalized_db_type)

    if normalized_db_type != "Snowflake":
        return adapter.connect(details)

    cache_key = _build_snowflake_cache_key(details)
    now = time.time()
    with _snowflake_cache_lock:
        _cleanup_expired_snowflake_connections(now)
        cached_entry = _snowflake_connection_cache.get(cache_key)
        if cached_entry:
            cached_entry["expires_at"] = now + SNOWFLAKE_CACHE_TTL_SECONDS
            return CachedConnection(cached_entry["connection"])

        connection = adapter.connect(details)
        _snowflake_connection_cache[cache_key] = {
            "connection": connection,
            "expires_at": now + SNOWFLAKE_CACHE_TTL_SECONDS,
        }
        return CachedConnection(connection)


__all__ = ["get_connection", "normalize_db_type", "normalize_snowflake_account"]
