"""The minimal cron parser behind recurring schedules."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from duratiq.cron import parse_cron

UTC = timezone.utc


def test_every_minute_next_is_top_of_next_minute() -> None:
    spec = parse_cron("* * * * *")
    t = datetime(2026, 6, 15, 9, 30, 15, tzinfo=UTC)
    assert spec.next_after(t) == datetime(2026, 6, 15, 9, 31, 0, tzinfo=UTC)


def test_step_minutes() -> None:
    spec = parse_cron("*/15 * * * *")
    assert spec.matches(datetime(2026, 6, 15, 9, 30, tzinfo=UTC))
    assert not spec.matches(datetime(2026, 6, 15, 9, 31, tzinfo=UTC))
    assert spec.next_after(datetime(2026, 6, 15, 9, 31, tzinfo=UTC)) == datetime(2026, 6, 15, 9, 45, tzinfo=UTC)


def test_daily_at_hour() -> None:
    spec = parse_cron("0 9 * * *")
    # Strictly after 09:00 rolls to the next day.
    assert spec.next_after(datetime(2026, 6, 15, 9, 0, tzinfo=UTC)) == datetime(2026, 6, 16, 9, 0, tzinfo=UTC)
    # From just before, it's the same day.
    assert spec.next_after(datetime(2026, 6, 15, 8, 59, tzinfo=UTC)) == datetime(2026, 6, 15, 9, 0, tzinfo=UTC)


def test_day_of_month() -> None:
    spec = parse_cron("30 2 1 * *")
    assert spec.next_after(datetime(2026, 6, 15, 0, 0, tzinfo=UTC)) == datetime(2026, 7, 1, 2, 30, tzinfo=UTC)


def test_list_and_range() -> None:
    spec = parse_cron("0 9,17 * * 1-5")  # 9am and 5pm on weekdays
    nxt = spec.next_after(datetime(2026, 6, 1, 0, 0, tzinfo=UTC))
    assert nxt.hour in (9, 17) and nxt.minute == 0
    assert nxt.weekday() < 5  # Mon-Fri


def test_weekday_conversion() -> None:
    # cron Mon == 1; Python Monday == 0.
    spec = parse_cron("0 9 * * 1")
    nxt = spec.next_after(datetime(2026, 6, 1, 0, 0, tzinfo=UTC))
    assert nxt.weekday() == 0 and nxt.hour == 9 and nxt.minute == 0


def test_sunday_is_zero_or_seven() -> None:
    by_zero = parse_cron("0 0 * * 0")
    by_seven = parse_cron("0 0 * * 7")
    base = datetime(2026, 6, 1, 0, 0, tzinfo=UTC)
    assert by_zero.next_after(base) == by_seven.next_after(base)
    assert by_zero.next_after(base).weekday() == 6  # Python Sunday == 6


def test_vixie_either_day_matches_when_both_restricted() -> None:
    spec = parse_cron("0 0 13 * 1")  # 00:00 on the 13th OR on Mondays
    the_13th = datetime(2026, 5, 13, 0, 0, tzinfo=UTC)
    assert spec.matches(the_13th)  # via day-of-month, whatever weekday it is
    a_monday = parse_cron("0 0 * * 1").next_after(datetime(2026, 5, 1, 0, 0, tzinfo=UTC))
    assert a_monday.weekday() == 0
    assert spec.matches(a_monday)  # via day-of-week, whatever date it is
    neither = datetime(2026, 5, 6, 0, 0, tzinfo=UTC)  # May 6 2026
    if neither.day != 13 and neither.weekday() != 0:
        assert not spec.matches(neither)


@pytest.mark.parametrize(
    "bad", ["* * * *", "* * * * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *", "*/0 * * * *"]
)
def test_invalid_expressions_raise(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_cron(bad)
