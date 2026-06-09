"""Dramatiq transport adapter.

Maps the engine's two primitives onto two Dramatiq actors:

* ``duratiq_tick``         — advances a run (one replay pass).
* ``duratiq_run_activity`` — runs one activity and reports its result.

This wires actors to *this* Engine instance, which is the simplest thing that
works inside one process (and is exactly what the StubBroker test exercises). For a
real multi-worker deployment you would instead declare module-level actors that
resolve a shared Engine built from the same registry + database config, so every
worker process can decode and run them.
"""

from __future__ import annotations

import dramatiq

from ..engine import Engine


class DramatiqDriver:
    def __init__(self, engine: Engine, *, broker: dramatiq.Broker | None = None, queue_name: str = "duratiq") -> None:
        self.engine = engine
        engine.driver = self
        self.broker = broker or dramatiq.get_broker()
        self.queue_name = queue_name

        self._tick_actor = dramatiq.actor(
            self._tick, actor_name="duratiq_tick", queue_name=queue_name, broker=self.broker, max_retries=3
        )
        self._activity_actor = dramatiq.actor(
            self._run_activity,
            actor_name="duratiq_run_activity",
            queue_name=queue_name,
            broker=self.broker,
            max_retries=0,  # the actor body never re-raises; failures are reported as FAILED steps
        )

    # ---- actor bodies -----------------------------------------------------
    def _tick(self, run_id: str) -> None:
        self.engine.tick(run_id)

    def _run_activity(self, run_id: str, seq: int, name: str, args: list, kwargs: dict) -> None:
        activity = self.engine.registry.get_activity(name)
        try:
            result = activity.fn(*args, **kwargs)
            self.engine.report_activity_result(run_id, seq, result, None)
        except Exception as exc:  # noqa: BLE001 - activity may raise anything
            self.engine.report_activity_result(run_id, seq, None, exc)

    # ---- Driver interface (called by the engine) --------------------------
    def request_tick(self, run_id: str) -> None:
        self._tick_actor.send(run_id)

    def dispatch_activity(
        self, run_id: str, seq: int, name: str, args: list, kwargs: dict, max_retries: int
    ) -> None:
        self._activity_actor.send_with_options(
            args=(run_id, seq, name, list(args), kwargs), max_retries=max_retries
        )
