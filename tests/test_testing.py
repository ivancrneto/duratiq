"""``duratiq.testing.WorkflowEnvironment``: the time-skipping test harness."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from duratiq import (
    TIMEOUT,
    UPDATE_PENDING,
    Registry,
    SqlStore,
    UpdateFailed,
    activity,
    default_registry,
    workflow,
)
from duratiq.testing import RunBlocked, RunFailed, WorkflowEnvironment

T0 = datetime(2030, 1, 7, 8, 0, tzinfo=timezone.utc)  # a Monday


@pytest.fixture
def reg() -> Registry:
    reg = Registry()

    @activity(name="send_reminder", registry=reg)
    def send_reminder(user_id: int) -> str:
        return f"reminded {user_id}"

    @activity(name="charge", registry=reg, max_retries=2)
    def charge(amount: int) -> str:
        return f"charged {amount}"

    @workflow(name="trial", registry=reg)
    def trial(ctx, user_id: int, wait: str = "P30D") -> str:
        ctx.sleep(wait)
        return ctx.activity(send_reminder, user_id)

    @workflow(name="two_naps", registry=reg)
    def two_naps(ctx, first_nap: str = "P30D", second_nap: str = "P1D") -> list[str]:
        ctx.sleep(first_nap)
        first = ctx.now().isoformat()
        ctx.sleep(second_nap)
        second = ctx.now().isoformat()
        return [first, second]

    @workflow(name="checkout", registry=reg)
    def checkout(ctx, amount: int) -> str:
        return ctx.activity(charge, amount)

    @workflow(name="approval", registry=reg)
    def approval(ctx, timeout: str = "P7D") -> object:
        decision = ctx.wait_signal("decision", timeout=timeout)
        return "expired" if decision is TIMEOUT else decision

    @workflow(name="await_signal", registry=reg)
    def await_signal(ctx) -> object:
        return ctx.wait_signal("go")

    return reg


@pytest.fixture
def env(reg: Registry):
    with WorkflowEnvironment(reg, start_time=T0) as env:
        yield env


# Every time-skipping scenario runs once with ISO-8601 durations and once with the
# developer shorthand, so neither style can regress unnoticed.
DURATION_STYLES = {
    "iso": {
        "30d": "P30D",
        "1d": "P1D",
        "6d": "P6D",
        "7d": "P7D",
        "40d": "P40D",
        "just_under_30d": "P29DT23H59M",
        "1m": "PT1M",
        "30m": "PT30M",
    },
    "shorthand": {
        "30d": "30d",
        "1d": "1d",
        "6d": "6d",
        "7d": "7d",
        "40d": "40d",
        "just_under_30d": "29d23h59m",
        "1m": "1m",
        "30m": "30m",
    },
}


@pytest.fixture(params=sorted(DURATION_STYLES))
def durations(request: pytest.FixtureRequest) -> dict[str, str]:
    return DURATION_STYLES[request.param]


# ---------------------------------------------------------------- time skipping
def test_execute_skips_a_30_day_sleep_in_one_line(env: WorkflowEnvironment, durations: dict[str, str]) -> None:
    assert env.execute("trial", user_id=7, wait=durations["30d"]) == "reminded 7"
    assert env.now() == T0 + timedelta(days=30)


def test_start_does_not_move_the_clock(env: WorkflowEnvironment) -> None:
    run_id = env.start("trial", user_id=7)
    assert env.status(run_id) == "SUSPENDED"
    assert env.now() == T0


def test_skip_is_precise(env: WorkflowEnvironment, durations: dict[str, str]) -> None:
    run_id = env.start("trial", user_id=7, wait=durations["30d"])
    env.skip(durations["just_under_30d"])
    assert env.status(run_id) == "SUSPENDED"
    env.skip(durations["1m"])
    assert env.status(run_id) == "COMPLETED"
    assert env.result(run_id) == "reminded 7"


def test_skip_accepts_timedelta_seconds_iso_and_shorthand(env: WorkflowEnvironment) -> None:
    env.skip(timedelta(hours=1))
    env.skip(1800)
    env.skip("PT30M")
    env.skip("30m")
    assert env.now() == T0 + timedelta(hours=2, minutes=30)


def test_skip_rejects_negative_durations(env: WorkflowEnvironment) -> None:
    with pytest.raises(ValueError):
        env.skip(-1)


def test_sequential_sleeps_measure_from_virtual_time(env: WorkflowEnvironment, durations: dict[str, str]) -> None:
    # Regression for the naive "just pass a future now=" approach: the second sleep
    # must be scheduled relative to the *virtual* clock, so a minute later it is
    # still pending rather than already overdue.
    run_id = env.start("two_naps", first_nap=durations["30d"], second_nap=durations["1d"])
    env.skip(durations["30d"])
    env.skip(durations["1m"])
    assert env.status(run_id) == "SUSPENDED"
    env.skip(durations["1d"])
    assert env.status(run_id) == "COMPLETED"


def test_one_long_skip_stops_at_every_deadline_in_between(env: WorkflowEnvironment, durations: dict[str, str]) -> None:
    run_id = env.start("two_naps", first_nap=durations["30d"], second_nap=durations["1d"])
    env.skip(durations["40d"])
    first, second = env.result(run_id)
    assert datetime.fromisoformat(first) == T0 + timedelta(days=30)
    assert datetime.fromisoformat(second) == T0 + timedelta(days=31)
    assert env.now() == T0 + timedelta(days=40)


def test_wait_signal_timeout_is_skipped_to(env: WorkflowEnvironment, durations: dict[str, str]) -> None:
    assert env.execute("approval", timeout=durations["7d"]) == "expired"
    assert env.now() == T0 + timedelta(days=7)


def test_signal_wins_before_the_timeout(env: WorkflowEnvironment, durations: dict[str, str]) -> None:
    run_id = env.start("approval", timeout=durations["7d"])
    env.skip(durations["6d"])
    env.signal(run_id, "decision", "approved")
    assert env.result(run_id) == "approved"


def test_parent_result_skips_through_a_sleeping_child(reg: Registry) -> None:
    @workflow(name="napping_child", registry=reg)
    def napping_child(ctx, n: int) -> int:
        ctx.sleep("PT12H")
        return n * 2

    @workflow(name="parent", registry=reg)
    def parent(ctx) -> int:
        return ctx.child_workflow(napping_child, n=21)

    with WorkflowEnvironment(reg, start_time=T0) as env:
        assert env.execute("parent") == 42
        assert env.now() == T0 + timedelta(hours=12)


def test_continue_as_new_loop_with_sleeps(reg: Registry) -> None:
    @workflow(name="poller", registry=reg)
    def poller(ctx, polls: int = 0) -> int:
        if polls == 3:
            return polls
        ctx.sleep("P1D")
        ctx.continue_as_new(polls=polls + 1)

    with WorkflowEnvironment(reg, start_time=T0) as env:
        assert env.execute("poller") == 3
        assert env.now() == T0 + timedelta(days=3)


def test_cron_schedule_fires_on_each_due_time_during_a_skip(reg: Registry) -> None:
    @workflow(name="standup", registry=reg)
    def standup(ctx) -> str:
        return ctx.now().strftime("%a")

    with WorkflowEnvironment(reg, start_time=T0) as env:
        env.engine.create_schedule("standup", "0 9 * * 1-5")  # 9am on weekdays
        env.skip("P7D")
        runs = env.engine.list_runs(name="standup", newest_first=False)
        assert [run.result["value"] for run in runs] == ["Mon", "Tue", "Wed", "Thu", "Fri"]


def test_lost_activity_is_retried_after_its_timeout(reg: Registry) -> None:
    calls = []

    @activity(name="flaky_network", registry=reg, start_to_close_ms=30_000, max_retries=1)
    def flaky_network() -> str:
        calls.append(1)
        return "delivered"

    @workflow(name="deliver", registry=reg)
    def deliver(ctx) -> str:
        return ctx.activity(flaky_network)

    with WorkflowEnvironment(reg, start_time=T0) as env:
        run_id = env.engine.start("deliver")
        env.driver.step()  # the tick schedules + dispatches the activity...
        env.driver.queue.clear()  # ...and the message is lost (worker crash)
        assert env.result(run_id) == "delivered"  # re-dispatched once the 30s deadline passes
        assert env.now() == T0 + timedelta(seconds=30)
        assert len(calls) == 1  # only the re-dispatched attempt ever ran


def test_execution_timeout_fails_a_run_waiting_forever(reg: Registry) -> None:
    with WorkflowEnvironment(reg, start_time=T0) as env:
        run_id = env.start("await_signal", execution_timeout=3600)
        with pytest.raises(RunFailed) as excinfo:
            env.result(run_id)
        assert excinfo.value.error["type"] == "ExecutionTimeout"
        assert env.now() == T0 + timedelta(hours=1)


def test_endless_sleep_loop_hits_max_skips(reg: Registry) -> None:
    @workflow(name="forever", registry=reg)
    def forever(ctx) -> None:
        while True:
            ctx.sleep(60)

    with WorkflowEnvironment(reg, max_skips=20) as env:
        with pytest.raises(RuntimeError, match="max_skips=20"):
            env.execute("forever")
        with pytest.raises(RuntimeError, match="max_skips=20"):
            env.skip("P1D")


# ------------------------------------------------------------- blocked / failed
def test_result_raises_run_blocked_naming_the_missing_signal(env: WorkflowEnvironment) -> None:
    run_id = env.start("await_signal")
    with pytest.raises(RunBlocked) as excinfo:
        env.result(run_id)
    assert excinfo.value.status == "SUSPENDED"
    assert excinfo.value.waiting_on == ["signal 'go' (seq 0)"]
    assert "signal 'go'" in str(excinfo.value)

    env.signal(run_id, "go", {"ok": True})
    assert env.result(run_id) == {"ok": True}


def test_result_without_skipping_time_reports_the_pending_timer(env: WorkflowEnvironment) -> None:
    run_id = env.start("trial", user_id=1)
    with pytest.raises(RunBlocked) as excinfo:
        env.result(run_id, skip_time=False)
    assert excinfo.value.waiting_on == [f"timer due {(T0 + timedelta(days=30)).isoformat()} (seq 0)"]
    assert env.now() == T0


def test_failed_run_raises_run_failed(env: WorkflowEnvironment) -> None:
    def declined(amount: int) -> str:
        raise RuntimeError("card declined")

    env.mock_activity("charge", declined)
    with pytest.raises(RunFailed) as excinfo:
        env.execute("checkout", amount=10)
    assert excinfo.value.status == "FAILED"
    assert excinfo.value.error["type"] == "ActivityFailed"
    assert "card declined" in str(excinfo.value)


def test_cancelled_run_raises_run_failed(env: WorkflowEnvironment) -> None:
    run_id = env.start("trial", user_id=1)
    assert env.cancel(run_id) is True
    with pytest.raises(RunFailed) as excinfo:
        env.result(run_id)
    assert excinfo.value.status == "CANCELLED"
    assert excinfo.value.error is None


def test_terminate(env: WorkflowEnvironment) -> None:
    run_id = env.start("trial", user_id=1)
    assert env.terminate(run_id, "ops said so") is True
    with pytest.raises(RunFailed, match="ops said so"):
        env.result(run_id)


def test_unknown_run_raises_key_error(env: WorkflowEnvironment) -> None:
    with pytest.raises(KeyError):
        env.result("nope")


# -------------------------------------------------------------------- mocking
def test_mock_activity_with_a_mock_object(env: WorkflowEnvironment) -> None:
    fake = Mock(return_value="charged by mock")
    env.mock_activity("charge", fake)
    assert env.execute("checkout", amount=5) == "charged by mock"
    fake.assert_called_once_with(5)


def test_mock_activity_as_decorator_keeps_retry_options(reg: Registry, env: WorkflowEnvironment) -> None:
    attempts = []

    @env.mock_activity(reg.get_activity("charge"))
    def flaky_charge(amount: int) -> str:
        attempts.append(amount)
        if len(attempts) < 3:
            raise ConnectionError("try again")
        return "charged on attempt 3"

    # The original declared max_retries=2, so three attempts are allowed.
    assert env.execute("checkout", amount=1) == "charged on attempt 3"
    assert len(attempts) == 3


def test_mock_activity_keeps_timeout_options(reg: Registry) -> None:
    # The activity-timeout scanner reads the deadline and retry budget from the
    # registry, so a mock must carry the original's options for a lost dispatch to be
    # retried (rather than failed as an activity with no timeout).
    @activity(name="upload", registry=reg, start_to_close_ms=10_000, max_retries=1)
    def upload() -> str:
        return "real upload"

    @workflow(name="uploader", registry=reg)
    def uploader(ctx) -> str:
        return ctx.activity(upload)

    with WorkflowEnvironment(reg, start_time=T0) as env:
        env.mock_activity(upload, lambda: "mocked upload")
        run_id = env.engine.start("uploader")
        env.driver.step()
        env.driver.queue.clear()  # lose the first dispatch
        assert env.result(run_id) == "mocked upload"
        assert env.now() == T0 + timedelta(seconds=10)


def test_mock_activity_does_not_leak_into_the_registry(reg: Registry) -> None:
    with WorkflowEnvironment(reg) as mocked:
        mocked.mock_activity("charge", lambda amount: "mocked")
        assert mocked.execute("checkout", amount=1) == "mocked"
    with WorkflowEnvironment(reg) as real:
        assert real.execute("checkout", amount=1) == "charged 1"
    assert reg.get_activity("charge").fn(1) == "charged 1"


def test_mock_unknown_activity_raises(env: WorkflowEnvironment) -> None:
    with pytest.raises(KeyError):
        env.mock_activity("nope", lambda: None)


# ------------------------------------------------------ mocking local activities
def _normalize(raw: str) -> str:
    return raw.strip().upper()


@pytest.fixture
def local_reg() -> Registry:
    reg = Registry()

    @activity(name="geocode", registry=reg, max_retries=0)
    def geocode(address: str) -> str:
        return f"real geocode of {address}"

    @workflow(name="enrich", registry=reg)
    def enrich(ctx, address: str) -> dict:
        return {
            "normalized": ctx.local_activity(_normalize, address),
            "local_geo": ctx.local_activity(geocode, address),
            "remote_geo": ctx.activity(geocode, address),
        }

    return reg


def test_mock_plain_local_activity_by_function(local_reg: Registry) -> None:
    with WorkflowEnvironment(local_reg) as env:
        env.mock_activity(_normalize, lambda raw: "MOCKED")
        result = env.execute("enrich", address=" main st ")
        assert result["normalized"] == "MOCKED"
        assert result["local_geo"] == "real geocode of  main st "
        # The step keeps the real function's name, so history reads the same as production.
        assert env.history(env.engine.list_runs()[0].id)[0].name == "_normalize"


def test_mock_plain_local_activity_as_decorator_with_mock_assertions(local_reg: Registry) -> None:
    with WorkflowEnvironment(local_reg) as env:
        fake = Mock(return_value="N")
        env.mock_activity(_normalize)(fake)
        env.execute("enrich", address="x")
        fake.assert_called_once_with("x")


def test_mock_registered_activity_covers_local_and_remote_calls(local_reg: Registry) -> None:
    with WorkflowEnvironment(local_reg) as env:
        calls = []

        @env.mock_activity("geocode")
        def fake_geocode(address: str) -> str:
            calls.append(address)
            return "0,0"

        result = env.execute("enrich", address="a")
        assert (result["local_geo"], result["remote_geo"]) == ("0,0", "0,0")
        assert calls == ["a", "a"]


def test_local_mock_keeps_the_requested_retries(local_reg: Registry) -> None:
    @workflow(name="retrying_local", registry=local_reg)
    def retrying_local(ctx) -> str:
        return ctx.local_activity(_normalize, "x", max_retries=1)

    attempts = []

    def flaky(raw: str) -> str:
        attempts.append(raw)
        if len(attempts) == 1:
            raise ConnectionError("blip")
        return "ok"

    with WorkflowEnvironment(local_reg) as env:
        env.mock_activity(_normalize, flaky)
        assert env.execute("retrying_local") == "ok"
        assert len(attempts) == 2


def test_local_mocks_do_not_leak_between_environments(local_reg: Registry) -> None:
    with WorkflowEnvironment(local_reg) as mocked:
        mocked.mock_activity(_normalize, lambda raw: "MOCKED")
        mocked.mock_activity("geocode", lambda address: "MOCKED")
        assert mocked.execute("enrich", address="b")["local_geo"] == "MOCKED"
    with WorkflowEnvironment(local_reg) as real:
        assert real.execute("enrich", address="b") == {
            "normalized": "B",
            "local_geo": "real geocode of b",
            "remote_geo": "real geocode of b",
        }


def test_mock_activity_object_used_only_locally(local_reg: Registry) -> None:
    # An @activity registered somewhere else (not in the environment's registry) can
    # still be run — and mocked — as a local activity.
    elsewhere = Registry()

    @activity(name="score", registry=elsewhere)
    def score(n: int) -> int:
        return n

    @workflow(name="scorer", registry=local_reg)
    def scorer(ctx) -> int:
        return ctx.local_activity(score, 1)

    with WorkflowEnvironment(local_reg) as env:
        env.mock_activity(score, lambda n: 100)
        assert env.execute("scorer") == 100
        with pytest.raises(KeyError):
            env.registry.get_activity("score")  # not smuggled into the registry


def test_unhashable_local_callable_still_runs_unmocked(local_reg: Registry) -> None:
    class Doubler:
        __hash__ = None  # e.g. a dataclass with eq=True

        def __call__(self, n: int) -> int:
            return n * 2

    @workflow(name="doubling", registry=local_reg)
    def doubling(ctx) -> int:
        return ctx.local_activity(Doubler(), 21)

    with WorkflowEnvironment(local_reg) as env:
        env.mock_activity(_normalize, lambda raw: raw)  # some other mock is installed
        assert env.execute("doubling") == 42


def test_mock_activity_rejects_non_callables(env: WorkflowEnvironment) -> None:
    with pytest.raises(TypeError, match="mock_activity expects"):
        env.mock_activity(42, lambda: None)


# ------------------------------------------------------------ updates/queries
def test_update_and_query_helpers(reg: Registry) -> None:
    @workflow(name="counter", registry=reg)
    def counter(ctx) -> int:
        total = {"n": 0}

        def add(k: int) -> int:
            total["n"] += k
            return total["n"]

        def reject_negative(k: int) -> None:
            if k < 0:
                raise ValueError("negative")

        def explode(k: int) -> int:
            raise RuntimeError("boom")

        ctx.set_query_handler("total", lambda: total["n"])
        ctx.set_update_handler("add", add)
        ctx.set_update_validator("add", reject_negative)
        ctx.set_update_handler("explode", explode)
        ctx.set_update_handler("stop", lambda: "stopping")
        while ctx.wait_update() != "stop":
            pass
        return total["n"]

    with WorkflowEnvironment(reg) as env:
        run_id = env.start("counter")
        assert env.update(run_id, "add", 2) == 2
        assert env.update(run_id, "add", 3) == 5
        assert env.query(run_id, "total") == 5
        with pytest.raises(ValueError, match="negative"):
            env.update(run_id, "add", -1)
        with pytest.raises(UpdateFailed, match="boom"):
            env.update(run_id, "explode", 1)
        assert env.pending(run_id) == ["an update (seq 3)"]  # a rejected update takes no seq
        assert env.update(run_id, "stop") == "stopping"
        assert env.result(run_id) == 5


def test_update_before_wait_is_pending(reg: Registry) -> None:
    @workflow(name="late_waiter", registry=reg)
    def late_waiter(ctx) -> str:
        ctx.set_update_handler("ping", lambda: "pong")
        ctx.wait_signal("ready")
        ctx.wait_update()
        return "done"

    with WorkflowEnvironment(reg) as env:
        run_id = env.start("late_waiter")
        assert env.update(run_id, "ping") is UPDATE_PENDING
        env.signal(run_id, "ready")
        assert env.result(run_id) == "done"


# --------------------------------------------------------------- inspection
def test_pending_describes_children_activities_and_handlers(reg: Registry) -> None:
    @workflow(name="idle_child", registry=reg)
    def idle_child(ctx) -> None:
        ctx.wait_signal("never")

    @workflow(name="busy_parent", registry=reg)
    def busy_parent(ctx) -> None:
        ctx.set_signal_handler("abort", lambda _: None)
        ctx.child_workflow(idle_child)

    with WorkflowEnvironment(reg) as env:
        run_id = env.start("busy_parent")
        child_id = env.store.find_child_run(run_id, 1).id
        assert env.pending(run_id) == [
            "signal handler 'abort' (seq 0)",
            f"child workflow 'idle_child' (seq 1, child run {child_id} is SUSPENDED)",
        ]

        # An activity whose message was lost shows up as an outstanding activity.
        lost = env.engine.start("checkout", amount=1)
        env.driver.step()
        env.driver.queue.clear()
        assert env.pending(lost) == ["activity 'charge' (seq 0)"]
        assert env.get(lost).status == "SUSPENDED"


def test_history_returns_recorded_steps(env: WorkflowEnvironment) -> None:
    run_id = env.start("trial", user_id=3)
    env.result(run_id)
    assert [(step.kind, step.status) for step in env.history(run_id)] == [
        ("TIMER", "COMPLETED"),
        ("ACTIVITY", "COMPLETED"),
    ]


# ----------------------------------------------------------------- lifecycle
def test_defaults_to_a_copy_of_the_default_registry() -> None:
    @workflow(name="testing_default_registry_probe")
    def probe(ctx) -> str:
        return "found"

    with WorkflowEnvironment() as env:
        assert env.execute("testing_default_registry_probe") == "found"
        assert env.registry is not default_registry


def test_naive_start_time_is_treated_as_utc(reg: Registry) -> None:
    with WorkflowEnvironment(reg, start_time=datetime(2030, 1, 1)) as env:  # noqa: DTZ001 - naive input under test
        assert env.now() == datetime(2030, 1, 1, tzinfo=timezone.utc)


def test_default_start_time_is_the_real_time_frozen(reg: Registry) -> None:
    before = datetime.now(timezone.utc)
    with WorkflowEnvironment(reg) as env:
        assert before <= env.now() <= datetime.now(timezone.utc)
        frozen = env.now()
        env.run_until_idle()
        assert env.now() == frozen


def test_bring_your_own_store_is_not_disposed(reg: Registry, tmp_path) -> None:
    store = SqlStore(f"sqlite:///{tmp_path / 'env.db'}")
    store.create_all()
    with WorkflowEnvironment(reg, store=store) as env:
        run_id = env.start("await_signal")
    # The caller's store is still usable after the environment closes.
    assert store.get_run(run_id).status == "SUSPENDED"
