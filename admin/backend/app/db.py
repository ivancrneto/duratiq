"""Database access — a single shared duratiq ``SqlStore`` built from settings."""

from __future__ import annotations

from functools import lru_cache

from duratiq import SqlStore

from .core.config import settings


@lru_cache(maxsize=1)
def get_store() -> SqlStore:
    """The process-wide store. Cached so we reuse one engine / connection pool."""
    return SqlStore(url=settings.database_url)
