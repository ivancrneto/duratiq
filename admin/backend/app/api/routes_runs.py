"""``/api`` routes — runs, steps, and summary stats. All read-only, token-gated."""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from duratiq import SqlStore

from .. import actions, repository
from ..db import get_store
from ..deps import get_enqueue, get_session, require_token
from ..schemas import ActionResult, RunListOut, RunOut, StatsOut, StepOut

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


@router.post("/runs/{run_id}/cancel", response_model=ActionResult)
def cancel_run(run_id: str, store: SqlStore = Depends(get_store)) -> ActionResult:
    try:
        status = actions.cancel_run(store, run_id)
    except actions.RunNotFound:
        raise HTTPException(status_code=404, detail="run not found") from None
    except actions.NotActionable as exc:
        raise HTTPException(status_code=409, detail=exc.message) from None
    return ActionResult(id=run_id, status=status)


@router.post("/runs/{run_id}/retry", response_model=ActionResult)
def retry_run(
    run_id: str,
    store: SqlStore = Depends(get_store),
    enqueue: Callable[[str], None] = Depends(get_enqueue),
) -> ActionResult:
    try:
        actions.retry_run(store, run_id)
    except actions.RunNotFound:
        raise HTTPException(status_code=404, detail="run not found") from None
    except actions.NotActionable as exc:
        raise HTTPException(status_code=409, detail=exc.message) from None
    enqueue(run_id)
    return ActionResult(id=run_id, status="PENDING", enqueued=True)
