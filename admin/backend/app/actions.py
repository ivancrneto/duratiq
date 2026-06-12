"""Run mutations for the admin, performed store-only (the admin has no engine).

These mirror ``duratiq.Engine.cancel`` / ``Engine.retry`` but run against the DB
directly, using ``SqlStore.locked_run`` for the same per-run serialisation. The
actual re-tick after a retry is enqueued by the caller via the broker.
"""

from __future__ import annotations

from duratiq import SqlStore

_TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}


class RunNotFound(Exception):
    """No run with the given id."""


class NotActionable(Exception):
    """The run exists but is in a state that doesn't allow this action."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


def cancel_run(store: SqlStore, run_id: str) -> str:
    """Mark a non-terminal run CANCELLED. Returns the new status."""
    with store.locked_run(run_id) as session:
        run = store.get_run(run_id, session=session)
        if run is None:
            raise RunNotFound
        if run.status in _TERMINAL:
            raise NotActionable(f"run is {run.status}; only non-terminal runs can be cancelled")
        store.update_run(run_id, session=session, status="CANCELLED")
    return "CANCELLED"


def retry_run(store: SqlStore, run_id: str) -> None:
    """Reset a FAILED run to PENDING so a worker can resume it.

    Drops failed steps (they reschedule on the next replay) and clears the error.
    The caller must enqueue a tick afterwards for the run to actually advance.
    """
    with store.locked_run(run_id) as session:
        run = store.get_run(run_id, session=session)
        if run is None:
            raise RunNotFound
        if run.status != "FAILED":
            raise NotActionable(f"run is {run.status}; only FAILED runs can be retried")
        for step in store.get_steps(run_id, session=session):
            if step.status == "FAILED":
                session.delete(step)
        store.update_run(run_id, session=session, status="PENDING", error=None)
