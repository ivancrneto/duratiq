"""Read queries over the duratiq tables. No writes — this admin is view-only."""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from duratiq import SqlStore
from duratiq.models import WorkflowRun, WorkflowSearchAttribute, WorkflowStep


def _apply_search_attributes(query, search_attributes: dict[str, Any] | None):
    """AND an equality filter per requested search attribute (a run must carry all)."""
    for key, value in (search_attributes or {}).items():
        matching = select(WorkflowSearchAttribute.run_id).where(
            WorkflowSearchAttribute.key == key,
            WorkflowSearchAttribute.value_index == SqlStore._search_index(value),
        )
        query = query.where(WorkflowRun.id.in_(matching))
    return query


def list_runs(
    session: Session,
    *,
    status: str | None = None,
    name: str | None = None,
    search_attributes: dict[str, Any] | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[WorkflowRun], int]:
    filtered = select(WorkflowRun)
    if status:
        filtered = filtered.where(WorkflowRun.status == status)
    if name:
        filtered = filtered.where(WorkflowRun.name == name)
    filtered = _apply_search_attributes(filtered, search_attributes)

    total = session.scalar(select(func.count()).select_from(filtered.subquery())) or 0
    rows = list(session.scalars(filtered.order_by(WorkflowRun.created_at.desc()).limit(limit).offset(offset)))
    return rows, total


def get_run(session: Session, run_id: str) -> WorkflowRun | None:
    return session.get(WorkflowRun, run_id)


def get_search_attributes(session: Session, run_id: str) -> dict[str, Any]:
    rows = session.scalars(select(WorkflowSearchAttribute).where(WorkflowSearchAttribute.run_id == run_id))
    return {row.key: row.value for row in rows}


def list_steps(session: Session, run_id: str) -> list[WorkflowStep]:
    return list(session.scalars(select(WorkflowStep).where(WorkflowStep.run_id == run_id).order_by(WorkflowStep.seq)))


def status_counts(session: Session) -> dict[str, int]:
    rows = session.execute(select(WorkflowRun.status, func.count()).group_by(WorkflowRun.status)).all()
    return {status: count for status, count in rows}
