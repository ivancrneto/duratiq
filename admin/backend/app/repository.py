"""Read queries over the duratiq tables. No writes — this admin is view-only."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from duratiq.models import WorkflowRun, WorkflowStep


def list_runs(
    session: Session,
    *,
    status: str | None = None,
    name: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[WorkflowRun], int]:
    filtered = select(WorkflowRun)
    if status:
        filtered = filtered.where(WorkflowRun.status == status)
    if name:
        filtered = filtered.where(WorkflowRun.name == name)

    total = session.scalar(select(func.count()).select_from(filtered.subquery())) or 0
    rows = list(
        session.scalars(
            filtered.order_by(WorkflowRun.created_at.desc()).limit(limit).offset(offset)
        )
    )
    return rows, total


def get_run(session: Session, run_id: str) -> WorkflowRun | None:
    return session.get(WorkflowRun, run_id)


def list_steps(session: Session, run_id: str) -> list[WorkflowStep]:
    return list(
        session.scalars(
            select(WorkflowStep).where(WorkflowStep.run_id == run_id).order_by(WorkflowStep.seq)
        )
    )


def status_counts(session: Session) -> dict[str, int]:
    rows = session.execute(
        select(WorkflowRun.status, func.count()).group_by(WorkflowRun.status)
    ).all()
    return {status: count for status, count in rows}
