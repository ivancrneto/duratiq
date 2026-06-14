"""The periodic scanner that makes a duratiq deployment self-driving.

A few engine scans have to run on a cadence for durable execution to actually make
progress without a request poking it:

* :meth:`Engine.fire_due_timers` — deliver elapsed ``ctx.sleep`` timers.
* :meth:`Engine.fire_due_schedules` — start runs for cron schedules that came due.
* :meth:`Engine.fire_due_activity_timeouts` — retry/fail activities past their deadline.
* :meth:`Engine.recover_stalled` — re-tick runs whose tick was lost to a crash.

:class:`Scanner` drives them on independent intervals from one thread. It's a
plain blocking loop (no APScheduler/periodiq dependency) — run it under whatever
process manager you already have (systemd, a Kubernetes Deployment, supervisord),
or embed it in a worker process with :meth:`run_forever` on a background thread.

    from duratiq.scanner import Scanner

    scanner = Scanner(engine)        # engine wired with its store + driver
    scanner.run_forever()            # blocks until SIGINT/SIGTERM or .stop()

For a standalone process, point the CLI at a factory that builds your engine:

    python -m duratiq.scanner myapp.workers:make_engine --timer-interval 1
"""

from __future__ import annotations

import argparse
import importlib
import logging
import signal
import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from .engine import Engine

log = logging.getLogger("duratiq.scanner")


class Scanner:
    """Runs the engine's three periodic scans on independent intervals.

    Each scan has its own cadence: timers want sub-second responsiveness, cron
    schedules only change once a minute, and recovery is a slower backstop. A scan
    that raises is logged and skipped — one transient DB error must not kill the
    loop — and the next scan runs on schedule.
    """

    def __init__(
        self,
        engine: Engine,
        *,
        timer_interval: float = 1.0,
        schedule_interval: float = 60.0,
        recovery_interval: float = 30.0,
        activity_timeout_interval: float = 5.0,
        recovery_older_than: float = 60.0,
        limit: int = 100,
    ) -> None:
        if min(timer_interval, schedule_interval, recovery_interval, activity_timeout_interval) <= 0:
            raise ValueError("scan intervals must be positive")
        self.engine = engine
        self.timer_interval = timer_interval
        self.schedule_interval = schedule_interval
        self.recovery_interval = recovery_interval
        self.activity_timeout_interval = activity_timeout_interval
        self.recovery_older_than = recovery_older_than
        self.limit = limit
        self._stop = threading.Event()

    # ------------------------------------------------------------------ scans
    def _scan_timers(self) -> int:
        return self.engine.fire_due_timers(limit=self.limit)

    def _scan_schedules(self) -> int:
        return self.engine.fire_due_schedules(limit=self.limit)

    def _scan_recovery(self) -> int:
        return self.engine.recover_stalled(older_than_seconds=self.recovery_older_than, limit=self.limit)

    def _scan_activity_timeouts(self) -> int:
        return self.engine.fire_due_activity_timeouts(limit=self.limit)

    def run_once(self, *, now: datetime | None = None) -> dict[str, int]:
        """Run every scan once and return how many runs each advanced.

        ``now`` is forwarded to every scan so tests can fast-forward the clock (note
        that a future ``now`` also makes ``recover_stalled`` treat runs as idle).
        Errors propagate here (unlike in :meth:`run_forever`), so a one-shot caller —
        e.g. a cron job invoking the scanner per minute — sees failures instead of
        swallowing them.
        """
        return {
            "timers": self.engine.fire_due_timers(now=now, limit=self.limit),
            "schedules": self.engine.fire_due_schedules(now=now, limit=self.limit),
            "activity_timeouts": self.engine.fire_due_activity_timeouts(now=now, limit=self.limit),
            "recovery": self.engine.recover_stalled(
                older_than_seconds=self.recovery_older_than, now=now, limit=self.limit
            ),
        }

    # ------------------------------------------------------------------- loop
    def run_forever(self) -> None:
        """Drive the scans until :meth:`stop` is called (blocking).

        Each scan fires on its own interval; the loop sleeps only until the nearest
        due scan, so timers stay responsive without busy-spinning. Interruptible —
        :meth:`stop` (or a signal handler that calls it) wakes the sleep at once.
        """
        self._stop.clear()
        scans: list[tuple[str, Callable[[], int], float, float]] = [
            ("timers", self._scan_timers, self.timer_interval, 0.0),
            ("schedules", self._scan_schedules, self.schedule_interval, 0.0),
            ("activity_timeouts", self._scan_activity_timeouts, self.activity_timeout_interval, 0.0),
            ("recovery", self._scan_recovery, self.recovery_interval, 0.0),
        ]
        # (name, fn, interval, next_deadline) using a monotonic clock.
        deadlines = {name: time.monotonic() for name, _, _, _ in scans}
        log.info("duratiq scanner started")
        while not self._stop.is_set():
            now = time.monotonic()
            for name, fn, interval, _ in scans:
                if now >= deadlines[name]:
                    self._run_scan(name, fn)
                    deadlines[name] = now + interval
            sleep_for = max(0.0, min(deadlines.values()) - time.monotonic())
            self._stop.wait(timeout=sleep_for)
        log.info("duratiq scanner stopped")

    def _run_scan(self, name: str, fn: Callable[[], int]) -> None:
        try:
            advanced = fn()
        except Exception:  # noqa: BLE001 - a transient scan error must not kill the loop
            log.exception("duratiq scan %s failed", name)
            return
        if advanced:
            log.info("duratiq scan %s advanced %d run(s)", name, advanced)

    def stop(self) -> None:
        """Signal :meth:`run_forever` to finish its current pass and return."""
        self._stop.set()


def _load_engine(ref: str) -> Engine:
    """Import ``module:factory`` and call it to build an :class:`Engine`.

    The scanner can't know your registry or broker, so a standalone process points
    at a zero-arg factory that wires and returns a fully-configured engine (store +
    driver), exactly like the entrypoint your workers already use.
    """
    if ":" not in ref:
        raise ValueError(f"engine factory must be 'module:callable', got {ref!r}")
    module_name, _, attr = ref.partition(":")
    factory = getattr(importlib.import_module(module_name), attr)
    return factory()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m duratiq.scanner",
        description="Run duratiq's periodic timer / schedule / activity-timeout / recovery scans.",
    )
    parser.add_argument("engine_factory", help="'module:callable' returning a configured Engine")
    parser.add_argument("--timer-interval", type=float, default=1.0, help="seconds between timer scans")
    parser.add_argument("--schedule-interval", type=float, default=60.0, help="seconds between schedule scans")
    parser.add_argument(
        "--activity-timeout-interval", type=float, default=5.0, help="seconds between activity-timeout scans"
    )
    parser.add_argument("--recovery-interval", type=float, default=30.0, help="seconds between recovery scans")
    parser.add_argument("--recovery-older-than", type=float, default=60.0, help="re-tick runs idle longer than this")
    parser.add_argument("--limit", type=int, default=100, help="max runs advanced per scan")
    parser.add_argument("--once", action="store_true", help="run each scan once and exit")
    parser.add_argument("--log-level", default="INFO", help="logging level (default: INFO)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    scanner = Scanner(
        _load_engine(args.engine_factory),
        timer_interval=args.timer_interval,
        schedule_interval=args.schedule_interval,
        recovery_interval=args.recovery_interval,
        activity_timeout_interval=args.activity_timeout_interval,
        recovery_older_than=args.recovery_older_than,
        limit=args.limit,
    )

    if args.once:
        counts = scanner.run_once()
        log.info("duratiq scan once: %s", counts)
        return 0

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: scanner.stop())
    scanner.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
