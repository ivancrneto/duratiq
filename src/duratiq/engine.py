"""The durable-execution engine: replay-from-top with DB-memoized steps.

A workflow run advances one *tick* at a time. Each tick replays the orchestrator
from the start; recorded steps return their memoized results, and the first
not-ready point raises :class:`Suspend`, which releases the worker. A tick is
re-requested whenever an activity completes (and later: a timer fires or a signal
arrives), driving the run forward until it returns.
"""

from __future__ import annotations

from typing import Any, Protocol
from uuid import uuid4

from .context import WorkflowContext
from .exceptions import ActivityFailed, Suspend
from .models import WorkflowRun
from .registry import Registry
from .store import SqlStore

_TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}


class Driver(Protocol):
    """Transport that carries ticks and activity dispatches. See ``drivers/``."""

    def request_tick(self, run_id: str) -> None: ...

    def dispatch_activity(
        self, run_id: str, seq: int, name: str, args: list, kwargs: dict, max_retries: int
    ) -> None: ...


class Engine:
    def __init__(self, registry: Registry, store: SqlStore, driver: Driver | None = None) -> None:
        self.registry = registry
        self.store = store
        self.driver = driver

    # --------------------------------------------------------------- client
    def start(self, name: str, *, idempotency_key: str | None = None, **kwargs: Any) -> str:
        wf = self.registry.get_workflow(name)  # validate name early
        if idempotency_key:
            existing = self.store.find_by_idempotency_key(idempotency_key)
            if existing is not None:
                return existing.id
        run_id = uuid4().hex
        self.store.create_run(
            run_id=run_id,
            name=name,
            version=wf.version,
            input=kwargs,
            idempotency_key=idempotency_key,
        )
        self.driver.request_tick(run_id)
        return run_id

    def get(self, run_id: str) -> WorkflowRun | None:
        return self.store.get_run(run_id)

    # ----------------------------------------------------------------- core
    def tick(self, run_id: str) -> None:
        scheduled: list = []
        with self.store.locked_run(run_id) as session:
            run = self.store.get_run(run_id, session=session)
            if run is None or run.status in _TERMINAL:
                return

            wf = self.registry.get_workflow(run.name)
            steps = self.store.get_steps(run_id, session=session)
            ctx = WorkflowContext(run_id, steps)

            try:
                result = wf.fn(ctx, **(run.input or {}))
            except Suspend:
                self.store.update_run(run_id, session=session, status="SUSPENDED")
            except ActivityFailed as exc:
                self.store.update_run(
                    run_id, session=session, status="FAILED",
                    error={"type": "ActivityFailed", "message": str(exc)},
                )
            except Exception as exc:  # noqa: BLE001 - workflow code may raise anything
                self.store.update_run(
                    run_id, session=session, status="FAILED",
                    error={"type": type(exc).__name__, "message": str(exc)},
                )
            else:
                self.store.update_run(run_id, session=session, status="COMPLETED", result={"value": result})

            # Record any newly-scheduled activities inside the same transaction.
            for sa in ctx.scheduled:
                self.store.create_step(
                    run_id, sa.seq, kind="ACTIVITY", name=sa.name,
                    input={"args": sa.args, "kwargs": sa.kwargs}, status="SCHEDULED", session=session,
                )
            scheduled = list(ctx.scheduled)

        # Dispatch only after the tick transaction has committed, so we never put a
        # message on the broker for a step that got rolled back.
        for sa in scheduled:
            self.driver.dispatch_activity(run_id, sa.seq, sa.name, sa.args, sa.kwargs, sa.max_retries)

    def report_activity_result(self, run_id: str, seq: int, result: Any, error: BaseException | None) -> None:
        if error is None:
            self.store.complete_step(run_id, seq, status="COMPLETED", result={"value": result})
        else:
            self.store.complete_step(
                run_id, seq, status="FAILED",
                error={"type": type(error).__name__, "message": str(error)},
            )
        self.driver.request_tick(run_id)
