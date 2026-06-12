"""FastAPI dependencies: a read session and the admin-token auth gate."""

from __future__ import annotations

from collections.abc import Iterator

from fastapi import Header, HTTPException, status
from sqlalchemy.orm import Session

from .core.config import settings
from .db import get_store


def get_session() -> Iterator[Session]:
    """Yield a short-lived read session. Overridden in tests."""
    store = get_store()
    with store.Session() as session:
        yield session


def require_token(authorization: str | None = Header(default=None)) -> None:
    """Gate ``/api/*`` behind a single shared bearer token.

    If ``ADMIN_TOKEN`` is unset the gate is open (intended for local dev only).
    """
    if not settings.admin_token:
        return
    if authorization != f"Bearer {settings.admin_token}":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing admin token",
            headers={"WWW-Authenticate": "Bearer"},
        )
