"""A small, dependency-free cron parser for recurring schedules.

Supports the standard 5-field format — ``minute hour day-of-month month
day-of-week`` — with ``*``, ``*/step``, ``a-b`` ranges, ``a-b/step`` stepped ranges,
and comma lists of those. Day-of-week is ``0-6`` with ``0`` = Sunday (``7`` is also
accepted as Sunday). It follows the classic Vixie-cron day rule: when *both*
day-of-month and day-of-week are restricted, a tick matches if *either* matches.

This is deliberately minimal — enough to drive recurring workflow starts from the
schedule scanner. ``next_after`` finds the next matching minute by stepping forward,
which is simple and correct at the engine's once-a-minute cadence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

_FIELD_BOUNDS = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]  # min,hour,dom,month,dow
# Stop the next_after search after this many minutes (~4 years) so an impossible
# spec (e.g. Feb 30) raises instead of looping forever.
_MAX_SCAN_MINUTES = 4 * 366 * 24 * 60


def _parse_field(spec: str, lo: int, hi: int) -> frozenset[int]:
    values: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            raise ValueError(f"empty cron field component in {spec!r}")
        step = 1
        if "/" in part:
            base, _, step_s = part.partition("/")
            step = int(step_s)
            if step <= 0:
                raise ValueError(f"cron step must be positive in {part!r}")
        else:
            base = part
        if base == "*":
            start, end = lo, hi
        elif "-" in base:
            start_s, _, end_s = base.partition("-")
            start, end = int(start_s), int(end_s)
        else:
            start = end = int(base)
        if start < lo or end > hi or start > end:
            raise ValueError(f"cron field component {part!r} out of range {lo}-{hi}")
        values.update(range(start, end + 1, step))
    return frozenset(values)


@dataclass(frozen=True)
class CronSpec:
    minute: frozenset[int]
    hour: frozenset[int]
    dom: frozenset[int]
    month: frozenset[int]
    dow: frozenset[int]
    dom_restricted: bool
    dow_restricted: bool

    def matches(self, dt: datetime) -> bool:
        if dt.minute not in self.minute or dt.hour not in self.hour or dt.month not in self.month:
            return False
        # Python weekday(): Mon=0..Sun=6. Cron: Sun=0..Sat=6. Convert.
        cron_dow = (dt.weekday() + 1) % 7
        dom_ok = dt.day in self.dom
        dow_ok = cron_dow in self.dow
        if self.dom_restricted and self.dow_restricted:
            return dom_ok or dow_ok  # Vixie rule: either matches
        return dom_ok and dow_ok

    def next_after(self, after: datetime) -> datetime:
        """Return the first matching minute strictly after ``after``."""
        # Truncate to the start of the next minute.
        candidate = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for _ in range(_MAX_SCAN_MINUTES):
            if self.matches(candidate):
                return candidate
            candidate += timedelta(minutes=1)
        raise ValueError("cron expression has no matching time within the scan horizon")


def parse_cron(expr: str) -> CronSpec:
    """Parse a 5-field cron expression into a :class:`CronSpec`."""
    fields = expr.split()
    if len(fields) != 5:
        raise ValueError(f"cron expression must have 5 fields, got {len(fields)}: {expr!r}")
    # Normalise day-of-week 7 -> 0 (both mean Sunday) before parsing.
    dow_field = fields[4].replace("7", "0")
    parsed = [
        _parse_field(fields[i], *_FIELD_BOUNDS[i]) if i != 4 else _parse_field(dow_field, *_FIELD_BOUNDS[4])
        for i in range(5)
    ]
    return CronSpec(
        minute=parsed[0],
        hour=parsed[1],
        dom=parsed[2],
        month=parsed[3],
        dow=parsed[4],
        dom_restricted=fields[2] != "*",
        dow_restricted=fields[4] != "*",
    )
