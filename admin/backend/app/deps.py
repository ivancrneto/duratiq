"""FastAPI dependencies: a read session and the admin-token auth gate."""

from __future__ import annotations

from collections.abc import Callable, Iterator

from fastapi import Header, HTTPException, status
from sqlalchemy.orm import Session

from .broker import enqueue_tick
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


def get_enqueue() -> Callable[[str], None]:
    """Return the tick-enqueue callable, or 503 if no broker is configured.

    Evaluated before the handler body, so a retry with no broker fails fast and
    never mutates the run.
    """
    if not settings.broker_url:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="retry requires a broker: set DURATIQ_BROKER_URL",
        )
    return enqueue_tick
