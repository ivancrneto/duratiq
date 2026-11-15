"""Testing workflows with ``duratiq.testing``: a month-long workflow tested in milliseconds.

A free-trial workflow waits 30 days, then either converts the customer or — if they
asked to cancel in the meantime — sends a goodbye. Testing that against the real
clock is impossible; ``WorkflowEnvironment`` runs it on a **virtual clock** that
only moves when the test says so (or jumps straight to the next deadline).

Each block below is what you would write as a separate test function. It runs as a
plain script too, so you can see the output:

    cd duratiq && python -m examples.testing_workflows

In a pytest suite, the usual shape is a fixture::

    @pytest.fixture
    def env():
        with WorkflowEnvironment(reg) as env:
            yield env

    def test_trial_converts(env):
        assert env.execute("trial", customer="ada") == "converted ada"
"""

from __future__ import annotations

from unittest.mock import Mock

from duratiq import TIMEOUT, Registry, activity, workflow
from duratiq.testing import RunBlocked, RunFailed, WorkflowEnvironment

reg = Registry()


@activity(name="charge_card", registry=reg)
def charge_card(customer: str) -> str:
    raise RuntimeError("this would hit the real payment provider")


@activity(name="send_email", registry=reg)
def send_email(customer: str, template: str) -> str:
    return f"{template} {customer}"


def loyalty_tier(days_active: int) -> str:
    """A cheap, pure helper — run inline via ctx.local_activity."""
    return "gold" if days_active >= 30 else "standard"


@workflow(name="trial", registry=reg)
def trial(ctx, customer: str) -> str:
    # Durations: ISO-8601 ("P30D") and shorthand ("30d") are interchangeable.
    cancelled = ctx.wait_signal("cancel", timeout="30d")
    if cancelled is not TIMEOUT:
        return ctx.activity(send_email, customer, "goodbye")
    ctx.activity(charge_card, customer)
    tier = ctx.local_activity(loyalty_tier, 30)
    return ctx.activity(send_email, customer, f"converted ({tier})")


@workflow(name="onboarding", registry=reg)
def onboarding(ctx, customer: str) -> str:
    ctx.sleep("P1D")  # ISO-8601 works everywhere shorthand does
    ctx.activity(send_email, customer, "day-1 tips")
    ctx.wait_signal("profile_completed")  # time alone can't deliver this
    return "onboarded"


def main() -> None:
    # 1. The one-liner: start, skip 30 days of virtual time, return the result.
    with WorkflowEnvironment(reg) as env:
        env.mock_activity(charge_card, Mock(return_value="ch_test"))  # never touch the real provider
        start = env.now()
        result = env.execute("trial", customer="ada")
        print(f"1. execute -> {result!r} after {(env.now() - start).days} virtual days")
        assert result == "converted (gold) ada"

    # 2. Step-by-step: skip precise amounts of time and assert in between.
    with WorkflowEnvironment(reg) as env:
        env.mock_activity("charge_card", lambda customer: "ch_test")
        run_id = env.start("trial", customer="bob")
        env.skip("P29D")  # or "29d", a number of seconds, or a timedelta
        print(f"2. after 29 days: {env.status(run_id)}, waiting on {env.pending(run_id)}")
        assert env.status(run_id) == "SUSPENDED"
        env.signal(run_id, "cancel")
        print(f"   cancelled on day 29 -> {env.result(run_id)!r}")
        assert env.result(run_id) == "goodbye bob"

    # 3. Mock a local activity too: pass the very function the workflow uses.
    with WorkflowEnvironment(reg) as env:
        env.mock_activity(charge_card, lambda customer: "ch_test")
        env.mock_activity(loyalty_tier, lambda days_active: "platinum")
        result = env.execute("trial", customer="cy")
        print(f"3. with loyalty_tier mocked -> {result!r}")
        assert result == "converted (platinum) cy"

    # 4. Failures surface as RunFailed, carrying the recorded error.
    with WorkflowEnvironment(reg) as env:
        env.mock_activity(charge_card, Mock(side_effect=RuntimeError("card declined")))
        try:
            env.execute("trial", customer="dee")
        except RunFailed as exc:
            print(f"4. RunFailed: {exc.error['message']}")
            assert "card declined" in exc.error["message"]
        else:
            raise AssertionError("expected the run to fail")

    # 5. A run waiting on something time can't deliver raises RunBlocked — no hang.
    with WorkflowEnvironment(reg) as env:
        run_id = env.start("onboarding", customer="eve")
        try:
            env.result(run_id)
        except RunBlocked as exc:
            print(f"5. RunBlocked, waiting on: {exc.waiting_on}")
            assert exc.waiting_on == ["signal 'profile_completed' (seq 2)"]
        env.signal(run_id, "profile_completed")
        assert env.result(run_id) == "onboarded"

    print("\n✓ a 30-day workflow, tested five ways without waiting a single real day. ✅")


if __name__ == "__main__":
    main()
