from __future__ import annotations

from functools import lru_cache
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency
    load_dotenv = None


_APP_DIR = Path(__file__).resolve().parent


@lru_cache(maxsize=1)
def load_environment() -> None:
    if load_dotenv is None:
        return

    for env_name in (".env", ".env.migrate", ".env.rag"):
        env_path = _APP_DIR / env_name
        if env_path.exists():
            load_dotenv(env_path, override=False)
