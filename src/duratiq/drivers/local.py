"""Synchronous in-process driver — no broker required.

A simple FIFO work queue you pump explicitly. Ideal for dev, examples, and tests:
because *you* decide when to process each item, you can simulate a crash by simply
discarding a driver mid-run and resuming on a fresh one backed by the same store.
"""

from __future__ import annotations

from collections import deque

from ..engine import Engine


class LocalDriver:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        engine.driver = self
        self.queue: deque[tuple] = deque()

    # ---- Driver interface (called by the engine) --------------------------
    def request_tick(self, run_id: str) -> None:
        self.queue.append(("tick", run_id))

    def dispatch_activity(
        self, run_id: str, seq: int, name: str, args: list, kwargs: dict, max_retries: int
    ) -> None:
        self.queue.append(("activity", run_id, seq, name, args, kwargs))

    # ---- Pumping ----------------------------------------------------------
    def step(self) -> str | None:
        """Process exactly one queued item. Returns its kind, or ``None`` if idle."""
        if not self.queue:
            return None
        item = self.queue.popleft()
        if item[0] == "tick":
            self.engine.tick(item[1])
        else:
            _, run_id, seq, name, args, kwargs = item
            activity = self.engine.registry.get_activity(name)
            try:
                result = activity.fn(*args, **kwargs)
                self.engine.report_activity_result(run_id, seq, result, None)
            except Exception as exc:  # noqa: BLE001 - activity may raise anything
                self.engine.report_activity_result(run_id, seq, None, exc)
        return item[0]

    def run_until_idle(self) -> None:
        while self.queue:
            self.step()
