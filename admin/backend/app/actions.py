"""Run mutations for the admin, performed store-only (the admin has no engine).

These mirror ``duratiq.Engine.cancel`` / ``Engine.retry`` / ``Engine.signal`` but
run against the DB directly, using ``SqlStore.locked_run`` for the same per-run
serialisation. The re-tick a retry or signal needs is enqueued by the caller via
the broker.
"""

from __future__ import annotations

from typing import Any

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
    """Mark a non-terminal run CANCELLED, cascading to its running children.

    Matches ``Engine.cancel``'s downward cascade: any still-running child workflows
    (and theirs) are cancelled too, so cancelling a parent doesn't strand them. (The
    engine also fails a *directly* cancelled child's parent; that upward notification
    needs the registry, so it's left to the engine — see the admin README.) Returns
    the new status.
    """
    with store.locked_run(run_id) as session:
        run = store.get_run(run_id, session=session)
        if run is None:
            raise RunNotFound
        if run.status in _TERMINAL:
            raise NotActionable(f"run is {run.status}; only non-terminal runs can be cancelled")
        store.update_run(run_id, session=session, status="CANCELLED")
    _cancel_children(store, run_id)
    return "CANCELLED"


def _cancel_children(store: SqlStore, run_id: str) -> None:
    """Recursively cancel a run's non-terminal child workflows."""
    for child_id in store.find_active_children(run_id):
        with store.locked_run(child_id) as session:
            child = store.get_run(child_id, session=session)
            if child is None or child.status in _TERMINAL:
                continue
            store.update_run(child_id, session=session, status="CANCELLED")
        _cancel_children(store, child_id)


def terminate_run(store: SqlStore, run_id: str, reason: str | None = None) -> str:
    """Mark a non-terminal run FAILED with a ``WorkflowTerminated`` error, cascading.

    The hard counterpart to :func:`cancel_run`, mirroring ``Engine.terminate``: where
    cancel records ``CANCELLED``, terminate records ``FAILED`` with a
    ``WorkflowTerminated`` error dict, and its cascade terminates running children the
    same way. (As with cancel, the upward parent notification needs the registry and is
    left to the engine — see the admin README.) Returns the new status.
    """
    error = {"type": "WorkflowTerminated", "message": reason}
    with store.locked_run(run_id) as session:
        run = store.get_run(run_id, session=session)
        if run is None:
            raise RunNotFound
        if run.status in _TERMINAL:
            raise NotActionable(f"run is {run.status}; only non-terminal runs can be terminated")
        store.update_run(run_id, session=session, status="FAILED", error=error)
    _terminate_children(store, run_id, error)
    return "FAILED"


def _terminate_children(store: SqlStore, run_id: str, error: dict[str, Any]) -> None:
    """Recursively terminate a run's non-terminal child workflows."""
    for child_id in store.find_active_children(run_id):
        with store.locked_run(child_id) as session:
            child = store.get_run(child_id, session=session)
            if child is None or child.status in _TERMINAL:
                continue
            store.update_run(child_id, session=session, status="FAILED", error=error)
        _terminate_children(store, child_id, error)


def signal_run(store: SqlStore, run_id: str, name: str, payload: Any = None) -> str:
    """Deliver a signal to a non-terminal run; returns its status after delivery.

    Stores the signal and pairs it with any waiting ``ctx.wait_signal`` (FIFO by
    name), exactly like ``Engine.signal``. The caller must enqueue a tick afterwards
    for the run to consume a freshly-matched wait and advance.
    """
    with store.locked_run(run_id) as session:
        run = store.get_run(run_id, session=session)
        if run is None:
            raise RunNotFound
        if run.status in _TERMINAL:
            raise NotActionable(f"run is {run.status}; only non-terminal runs can be signalled")
        store.add_signal(run_id, name, payload, session=session)
        store.match_signals(run_id, session=session)
        return run.status


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
