"""``/api`` routes — runs, steps, and summary stats. All read-only, token-gated."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from duratiq import SqlStore

from .. import actions, repository
from ..db import get_store
from ..deps import get_enqueue, get_session, require_token
from ..schemas import ActionResult, RunDetailOut, RunListOut, RunOut, SignalIn, StatsOut, StepOut


def _parse_search_attributes(sa: str | None) -> dict[str, Any] | None:
    """Parse the ``sa`` query param: a JSON object of ``{key: value}`` equality
    filters, e.g. ``?sa={"region":"eu"}``. Raises 400 on anything else."""
    if not sa:
        return None
    try:
        parsed = json.loads(sa)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="sa must be a JSON object") from None
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="sa must be a JSON object")
    return parsed


router = APIRouter(prefix="/api", dependencies=[Depends(require_token)])


@router.get("/stats", response_model=StatsOut)
def get_stats(session: Session = Depends(get_session)) -> StatsOut:
    by_status = repository.status_counts(session)
    return StatsOut(total=sum(by_status.values()), by_status=by_status)


@router.get("/runs", response_model=RunListOut)
def list_runs(
    status: str | None = None,
    name: str | None = None,
    sa: str | None = Query(None, description='Search-attribute filter as a JSON object, e.g. {"region":"eu"}'),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session),
) -> RunListOut:
    rows, total = repository.list_runs(
        session, status=status, name=name, search_attributes=_parse_search_attributes(sa), limit=limit, offset=offset
    )
    return RunListOut(
        items=[RunOut.model_validate(r) for r in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/runs/{run_id}", response_model=RunDetailOut)
def get_run(run_id: str, session: Session = Depends(get_session)) -> RunDetailOut:
    run = repository.get_run(session, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    detail = RunDetailOut.model_validate(run)
    return detail.model_copy(update={"search_attributes": repository.get_search_attributes(session, run_id)})


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


@router.post("/runs/{run_id}/signal", response_model=ActionResult)
def signal_run(
    run_id: str,
    body: SignalIn,
    store: SqlStore = Depends(get_store),
    enqueue: Callable[[str], None] = Depends(get_enqueue),
) -> ActionResult:
    try:
        status = actions.signal_run(store, run_id, body.name, body.payload)
    except actions.RunNotFound:
        raise HTTPException(status_code=404, detail="run not found") from None
    except actions.NotActionable as exc:
        raise HTTPException(status_code=409, detail=exc.message) from None
    enqueue(run_id)
    return ActionResult(id=run_id, status=status, enqueued=True)
