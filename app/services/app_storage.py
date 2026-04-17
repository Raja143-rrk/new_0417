from __future__ import annotations

import hashlib
import json
import os
import secrets
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from app.config import load_environment

try:
    import pymysql
    from pymysql.cursors import DictCursor
except Exception:  # pragma: no cover
    pymysql = None
    DictCursor = None


load_environment()

_LOCK = threading.Lock()
_SCHEMA_READY = False
_LEGACY_USERS_FILE = Path(__file__).resolve().parents[2] / "app_data" / "users.json"

ROLE_LABELS = {
    "admin": "Migration Admin",
    "operator": "Migration Operator",
    "viewer": "Viewer",
}

DEFAULT_USERS = (
    {"username": "admin", "password": "Migrator@123", "email": "", "role": "admin"},
    {"username": "operator", "password": "Welcome@123", "email": "", "role": "operator"},
    {"username": "viewer", "password": "Viewer@123", "email": "", "role": "viewer"},
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env(name: str, fallback: str = "") -> str:
    app_specific = os.getenv(name, "").strip()
    if app_specific:
        return app_specific
    if name.startswith("APP_DB_"):
        legacy_name = name.replace("APP_DB_", "MIGRATION_JOB_STORE_", 1)
        return os.getenv(legacy_name, fallback).strip()
    return os.getenv(name, fallback).strip()


def is_enabled() -> bool:
    return bool(
        pymysql is not None
        and _env("APP_DB_HOST")
        and _env("APP_DB_USER")
        and _env("APP_DB_PASSWORD")
        and _env("APP_DB_DATABASE")
    )


def get_connection():
    if not is_enabled():
        return None
    return pymysql.connect(
        host=_env("APP_DB_HOST", "localhost"),
        port=int(_env("APP_DB_PORT", "3306")),
        user=_env("APP_DB_USER"),
        password=_env("APP_DB_PASSWORD"),
        database=_env("APP_DB_DATABASE"),
        charset="utf8mb4",
        autocommit=False,
        cursorclass=DictCursor,
    )


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    iterations = 200000
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), iterations)
    return f"pbkdf2_sha256${iterations}${salt}${digest.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algorithm, iterations_raw, salt, expected_hash = str(stored_hash or "").split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        iterations = int(iterations_raw)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), iterations)
        return secrets.compare_digest(digest.hex(), expected_hash)
    except Exception:
        return False


def _normalize_role(role: str) -> str:
    normalized = str(role or "viewer").strip().lower()
    return normalized if normalized in ROLE_LABELS else "viewer"


def _row_to_user(row: dict) -> dict:
    role = _normalize_role(row.get("role"))
    return {
        "username": row.get("username", ""),
        "email": row.get("email", ""),
        "role": role,
        "role_label": ROLE_LABELS.get(role, "Viewer"),
        "home": row.get("home") or "/home",
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
        "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else None,
    }


def _load_legacy_users() -> list[dict]:
    if not _LEGACY_USERS_FILE.exists():
        return list(DEFAULT_USERS)
    try:
        raw = json.loads(_LEGACY_USERS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return list(DEFAULT_USERS)

    users = []
    for username, payload in dict(raw or {}).items():
        users.append(
            {
                "username": str(username).strip(),
                "password": str(payload.get("password") or ""),
                "email": str(payload.get("email") or "").strip(),
                "role": _normalize_role(str(payload.get("role") or "viewer")),
            }
        )
    if not users:
        return list(DEFAULT_USERS)
    return users


def initialize_app_storage() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _LOCK:
        if _SCHEMA_READY:
            return
        connection = get_connection()
        if connection is None:
            return
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS app_users (
                      username VARCHAR(100) PRIMARY KEY,
                      password_hash VARCHAR(255) NOT NULL,
                      email VARCHAR(255) NOT NULL DEFAULT '',
                      role VARCHAR(20) NOT NULL,
                      home VARCHAR(255) NOT NULL DEFAULT '/home',
                      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS app_connection_profiles (
                      id VARCHAR(64) PRIMARY KEY,
                      name VARCHAR(255) NOT NULL,
                      engine VARCHAR(50) NOT NULL,
                      fields_json JSON NOT NULL,
                      created_by VARCHAR(100) NULL,
                      updated_by VARCHAR(100) NULL,
                      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                      UNIQUE KEY uq_connection_profile_name (name)
                    )
                    """
                )
                cursor.execute("SELECT COUNT(*) AS total FROM app_users")
                total_users = int((cursor.fetchone() or {}).get("total") or 0)
                if total_users == 0:
                    for item in _load_legacy_users():
                        cursor.execute(
                            """
                            INSERT INTO app_users (username, password_hash, email, role, home)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (
                                item["username"],
                                hash_password(item["password"]),
                                item.get("email", ""),
                                _normalize_role(item.get("role", "viewer")),
                                "/home",
                            ),
                        )
            connection.commit()
            _SCHEMA_READY = True
        except Exception:
            try:
                connection.rollback()
            except Exception:
                pass
            raise
        finally:
            connection.close()


def list_users() -> list[dict]:
    initialize_app_storage()
    connection = get_connection()
    if connection is None:
        raise RuntimeError("Application MySQL storage is not configured.")
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT username, email, role, home, created_at, updated_at
                FROM app_users
                ORDER BY username ASC
                """
            )
            rows = cursor.fetchall() or []
        return [_row_to_user(row) for row in rows]
    finally:
        connection.close()


def authenticate_user(username: str, password: str) -> dict | None:
    initialize_app_storage()
    connection = get_connection()
    if connection is None:
        raise RuntimeError("Application MySQL storage is not configured.")
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT username, password_hash, email, role, home, created_at, updated_at
                FROM app_users
                WHERE username = %s
                LIMIT 1
                """,
                (username.strip(),),
            )
            row = cursor.fetchone()
        if not row or not verify_password(password, row.get("password_hash", "")):
            return None
        return _row_to_user(row)
    finally:
        connection.close()


def create_user(username: str, password: str, email: str, role: str) -> dict:
    initialize_app_storage()
    clean_username = username.strip()
    clean_email = email.strip()
    normalized_role = _normalize_role(role)

    if not clean_username:
        raise ValueError("Username is required.")
    if not password:
        raise ValueError("Password is required.")
    if not clean_email:
        raise ValueError("User email is required.")
    if "@" not in clean_email or "." not in clean_email.split("@")[-1]:
        raise ValueError("Enter a valid email address.")
    if normalized_role not in ROLE_LABELS:
        raise ValueError("Invalid role selected.")

    connection = get_connection()
    if connection is None:
        raise RuntimeError("Application MySQL storage is not configured.")
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM app_users WHERE username = %s LIMIT 1", (clean_username,))
            if cursor.fetchone():
                raise ValueError("That username already exists.")
            cursor.execute(
                """
                INSERT INTO app_users (username, password_hash, email, role, home)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (clean_username, hash_password(password), clean_email, normalized_role, "/home"),
            )
            connection.commit()
            cursor.execute(
                """
                SELECT username, email, role, home, created_at, updated_at
                FROM app_users
                WHERE username = %s
                LIMIT 1
                """,
                (clean_username,),
            )
            row = cursor.fetchone() or {}
        return _row_to_user(row)
    except Exception:
        try:
            connection.rollback()
        except Exception:
            pass
        raise
    finally:
        connection.close()


def update_user(username: str, email: str, role: str) -> dict:
    initialize_app_storage()
    clean_username = username.strip()
    clean_email = email.strip()
    normalized_role = _normalize_role(role)

    if not clean_username:
        raise ValueError("Username is required.")
    if not clean_email:
        raise ValueError("User email is required.")
    if "@" not in clean_email or "." not in clean_email.split("@")[-1]:
        raise ValueError("Enter a valid email address.")
    if normalized_role not in ROLE_LABELS:
        raise ValueError("Invalid role selected.")

    connection = get_connection()
    if connection is None:
        raise RuntimeError("Application MySQL storage is not configured.")
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM app_users WHERE username = %s LIMIT 1", (clean_username,))
            if not cursor.fetchone():
                raise ValueError("User not found.")
            cursor.execute(
                """
                UPDATE app_users
                SET email = %s,
                    role = %s
                WHERE username = %s
                """,
                (clean_email, normalized_role, clean_username),
            )
            connection.commit()
            cursor.execute(
                """
                SELECT username, email, role, home, created_at, updated_at
                FROM app_users
                WHERE username = %s
                LIMIT 1
                """,
                (clean_username,),
            )
            row = cursor.fetchone() or {}
        return _row_to_user(row)
    except Exception:
        try:
            connection.rollback()
        except Exception:
            pass
        raise
    finally:
        connection.close()


def delete_user(username: str) -> None:
    initialize_app_storage()
    clean_username = username.strip()
    if not clean_username:
        raise ValueError("Username is required.")
    if clean_username == "admin":
        raise ValueError("The default admin user cannot be deleted.")

    connection = get_connection()
    if connection is None:
        raise RuntimeError("Application MySQL storage is not configured.")
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM app_users WHERE username = %s LIMIT 1", (clean_username,))
            if not cursor.fetchone():
                raise ValueError("User not found.")
            cursor.execute("DELETE FROM app_users WHERE username = %s", (clean_username,))
            connection.commit()
    except Exception:
        try:
            connection.rollback()
        except Exception:
            pass
        raise
    finally:
        connection.close()


def list_connection_profiles() -> list[dict]:
    initialize_app_storage()
    connection = get_connection()
    if connection is None:
        raise RuntimeError("Application MySQL storage is not configured.")
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, name, engine, fields_json, created_by, updated_by, created_at, updated_at
                FROM app_connection_profiles
                ORDER BY name ASC
                """
            )
            rows = cursor.fetchall() or []
        items = []
        for row in rows:
            items.append(
                {
                    "id": str(row.get("id") or ""),
                    "name": str(row.get("name") or ""),
                    "engine": str(row.get("engine") or ""),
                    "fields": row.get("fields_json") if isinstance(row.get("fields_json"), dict) else json.loads(row.get("fields_json") or "{}"),
                    "created_by": row.get("created_by"),
                    "updated_by": row.get("updated_by"),
                    "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
                    "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else None,
                }
            )
        return items
    finally:
        connection.close()


def save_connection_profile(
    profile_id: str | None,
    name: str,
    engine: str,
    fields: dict,
    actor_username: str | None,
) -> dict:
    initialize_app_storage()
    clean_id = str(profile_id or "").strip() or uuid.uuid4().hex
    clean_name = str(name or "").strip()
    clean_engine = str(engine or "").strip().lower()

    if not clean_name:
        raise ValueError("Connection name is required.")
    if not clean_engine:
        raise ValueError("Connection engine is required.")
    if not isinstance(fields, dict) or not fields:
        raise ValueError("Connection fields are required.")

    normalized_fields = {str(key): str(value or "") for key, value in fields.items()}
    connection = get_connection()
    if connection is None:
        raise RuntimeError("Application MySQL storage is not configured.")
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id
                FROM app_connection_profiles
                WHERE name = %s AND id <> %s
                LIMIT 1
                """,
                (clean_name, clean_id),
            )
            if cursor.fetchone():
                raise ValueError("A connection profile with that name already exists.")

            cursor.execute("SELECT created_by FROM app_connection_profiles WHERE id = %s LIMIT 1", (clean_id,))
            existing = cursor.fetchone()
            if existing:
                cursor.execute(
                    """
                    UPDATE app_connection_profiles
                    SET name = %s,
                        engine = %s,
                        fields_json = %s,
                        updated_by = %s
                    WHERE id = %s
                    """,
                    (clean_name, clean_engine, json.dumps(normalized_fields, ensure_ascii=True), actor_username, clean_id),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO app_connection_profiles (id, name, engine, fields_json, created_by, updated_by)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        clean_id,
                        clean_name,
                        clean_engine,
                        json.dumps(normalized_fields, ensure_ascii=True),
                        actor_username,
                        actor_username,
                    ),
                )
            connection.commit()
            cursor.execute(
                """
                SELECT id, name, engine, fields_json, created_by, updated_by, created_at, updated_at
                FROM app_connection_profiles
                WHERE id = %s
                LIMIT 1
                """,
                (clean_id,),
            )
            row = cursor.fetchone() or {}
        return {
            "id": str(row.get("id") or clean_id),
            "name": str(row.get("name") or clean_name),
            "engine": str(row.get("engine") or clean_engine),
            "fields": row.get("fields_json") if isinstance(row.get("fields_json"), dict) else json.loads(row.get("fields_json") or "{}"),
            "created_by": row.get("created_by"),
            "updated_by": row.get("updated_by"),
            "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
            "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else None,
        }
    except Exception:
        try:
            connection.rollback()
        except Exception:
            pass
        raise
    finally:
        connection.close()


def delete_connection_profile(profile_id: str) -> None:
    initialize_app_storage()
    connection = get_connection()
    if connection is None:
        raise RuntimeError("Application MySQL storage is not configured.")
    try:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM app_connection_profiles WHERE id = %s", (str(profile_id or "").strip(),))
        connection.commit()
    except Exception:
        try:
            connection.rollback()
        except Exception:
            pass
        raise
    finally:
        connection.close()
