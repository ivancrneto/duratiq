"""End-to-end: the scanner CLI as its own OS process, the way it is deployed.

``python -m duratiq.scanner module:factory`` is what users run under systemd or a
Kubernetes Deployment. These tests start it as a real subprocess against a shared
SQLite file, let it fire a workflow's timer in wall-clock time, and then stop it
with SIGTERM — checking the factory loading, the interval loop, the signal handling,
and that its work is visible to another process through the database.

The factory's engine uses a driver that processes work as soon as it is queued, so
the scanner process also acts as the worker: the activity's result carries the
scanner's pid, which proves the run was advanced *there* and not by the test.
"""

from __future__ import annotations

import importlib
import os
import pathlib
import signal
import subprocess
import sys

import pytest

from duratiq import Engine, SqlStore
from duratiq.drivers.local import LocalDriver

ROOT = pathlib.Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")

APP_MODULE = "duratiq_e2e_scanner_app"
APP_SOURCE = '''
import os

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@activity(name="stamp", registry=reg)
def stamp(label):
    return f"{label} stamped by pid {os.getpid()}"


@workflow(name="nap", registry=reg)
def nap(ctx, label, seconds):
    ctx.sleep(seconds)
    return ctx.activity(stamp, label)


class DrainingDriver(LocalDriver):
    """A LocalDriver that drains its queue as soon as work arrives."""

    def __init__(self, engine):
        super().__init__(engine)
        self._draining = False

    def request_tick(self, run_id):
        super().request_tick(run_id)
        self._drain()

    def dispatch_activity(self, *args):
        super().dispatch_activity(*args)
        self._drain()

    def _drain(self):
        if self._draining:
            return
        self._draining = True
        try:
            self.run_until_idle()
        finally:
            self._draining = False


def make_engine():
    engine = Engine(reg, SqlStore(os.environ["DURATIQ_E2E_DB_URL"]))
    DrainingDriver(engine)
    return engine
'''


@pytest.fixture
def app(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch):
    (tmp_path / f"{APP_MODULE}.py").write_text(APP_SOURCE)
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module(APP_MODULE)
    yield module
    sys.modules.pop(APP_MODULE, None)


@pytest.fixture
def db_url(tmp_path: pathlib.Path) -> str:
    url = f"sqlite:///{tmp_path / 'scanner.db'}"
    store = SqlStore(url)
    store.create_all()
    store.engine.dispose()
    return url


def _start_sleeping_run(app, db_url: str, seconds: float) -> tuple[SqlStore, str]:
    """Start a run from *this* process and leave it parked on its timer."""
    store = SqlStore(db_url)
    engine = Engine(app.reg, store)
    driver = LocalDriver(engine)
    run_id = engine.start("nap", label="job", seconds=seconds)
    driver.run_until_idle()
    assert store.get_run(run_id).status == "SUSPENDED"
    return store, run_id


def _scanner_cmd(*args: str) -> list[str]:
    return [sys.executable, "-m", "duratiq.scanner", f"{APP_MODULE}:make_engine", *args]


def _env(tmp_path: pathlib.Path, db_url: str) -> dict[str, str]:
    path = [str(tmp_path), str(ROOT / "src"), os.environ.get("PYTHONPATH", "")]
    return {**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, path)), "DURATIQ_E2E_DB_URL": db_url}


def test_scanner_process_fires_timers_and_stops_on_sigterm(app, db_url, tmp_path, wait_for) -> None:
    store, run_id = _start_sleeping_run(app, db_url, seconds=0.5)

    proc = subprocess.Popen(
        _scanner_cmd("--timer-interval", "0.05", "--log-level", "INFO"),
        env=_env(tmp_path, db_url),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for(
            lambda: proc.poll() is not None or store.get_run(run_id).status == "COMPLETED",
            what="the scanner process to complete the run",
        )
        assert proc.poll() is None, f"scanner exited early ({proc.returncode}): {proc.communicate()[1]}"

        proc.send_signal(signal.SIGTERM)
        _, stderr = proc.communicate(timeout=15)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate()

    assert proc.returncode == 0, stderr
    assert store.get_run(run_id).result["value"] == f"job stamped by pid {proc.pid}"
    assert "duratiq scanner started" in stderr
    assert "duratiq scan timers advanced 1 run(s)" in stderr
    assert "duratiq scanner stopped" in stderr
    store.engine.dispose()


def test_scanner_once_runs_every_scan_and_exits(app, db_url, tmp_path) -> None:
    store, run_id = _start_sleeping_run(app, db_url, seconds=0)  # due immediately

    proc = subprocess.run(
        _scanner_cmd("--once"), env=_env(tmp_path, db_url), capture_output=True, text=True, timeout=60
    )

    assert proc.returncode == 0, proc.stderr
    assert "duratiq scan once:" in proc.stderr and "'timers': 1" in proc.stderr
    run = store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"].startswith("job stamped by pid ")
    assert run.result["value"] != f"job stamped by pid {os.getpid()}"
    store.engine.dispose()


def test_scanner_rejects_a_malformed_factory_reference(tmp_path, db_url) -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "duratiq.scanner", "no_colon_here", "--once"],
        env=_env(tmp_path, db_url),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode != 0
    assert "engine factory must be 'module:callable'" in proc.stderr
