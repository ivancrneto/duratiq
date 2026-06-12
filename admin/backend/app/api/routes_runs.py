"""``/api`` routes — runs, steps, and summary stats. All read-only, token-gated."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from .. import repository
from ..deps import get_session, require_token
from ..schemas import RunListOut, RunOut, StatsOut, StepOut

router = APIRouter(prefix="/api", dependencies=[Depends(require_token)])


@router.get("/stats", response_model=StatsOut)
def get_stats(session: Session = Depends(get_session)) -> StatsOut:
    by_status = repository.status_counts(session)
    return StatsOut(total=sum(by_status.values()), by_status=by_status)


@router.get("/runs", response_model=RunListOut)
def list_runs(
    status: str | None = None,
    name: str | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session),
) -> RunListOut:
    rows, total = repository.list_runs(
        session, status=status, name=name, limit=limit, offset=offset
    )
    return RunListOut(
        items=[RunOut.model_validate(r) for r in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/runs/{run_id}", response_model=RunOut)
def get_run(run_id: str, session: Session = Depends(get_session)) -> RunOut:
    run = repository.get_run(session, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    return RunOut.model_validate(run)


@router.get("/runs/{run_id}/steps", response_model=list[StepOut])
def get_steps(run_id: str, session: Session = Depends(get_session)) -> list[StepOut]:
    if repository.get_run(session, run_id) is None:
        raise HTTPException(status_code=404, detail="run not found")
    return [StepOut.model_validate(s) for s in repository.list_steps(session, run_id)]
