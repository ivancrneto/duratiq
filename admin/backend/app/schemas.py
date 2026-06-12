"""Pydantic read models. Serialised straight from the duratiq ORM rows."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class RunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    version: int
    status: str
    input: Any | None = None
    result: Any | None = None
    error: Any | None = None
    idempotency_key: str | None = None
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class RunListOut(BaseModel):
    items: list[RunOut]
    total: int
    limit: int
    offset: int


class StepOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: str
    seq: int
    kind: str
    name: str
    status: str
    input: Any | None = None
    result: Any | None = None
    error: Any | None = None
    attempt: int
    scheduled_at: datetime
    completed_at: datetime | None = None


class StatsOut(BaseModel):
    total: int
    by_status: dict[str, int]
