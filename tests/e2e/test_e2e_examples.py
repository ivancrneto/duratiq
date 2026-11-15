"""Every runnable example still runs, exactly as the README tells users to run it.

The examples are the library's front door. Most also assert their own outcome (a
run COMPLETED, a card charged once, ...), so a non-zero exit here catches both API
drift that breaks user code and behavioural regressions. Each runs in a fresh
interpreter (``python -m examples.<name>`` from the repo root) because several
install process-global state — a payload codec, an OpenTelemetry provider.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]

# Needs a live PostgreSQL *and* Redis; the Postgres path is covered by the e2e
# Postgres tests instead.
NEEDS_INFRA = {"postgres_dramatiq"}

EXAMPLES = sorted(path.stem for path in (ROOT / "examples").glob("*.py") if path.stem not in NEEDS_INFRA)


def test_examples_are_discovered() -> None:
    assert len(EXAMPLES) >= 20, EXAMPLES


@pytest.mark.parametrize("name", EXAMPLES)
def test_example_runs(name: str) -> None:
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, [str(ROOT / "src"), os.environ.get("PYTHONPATH")]))}
    proc = subprocess.run(
        [sys.executable, "-m", f"examples.{name}"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, f"examples/{name}.py exited {proc.returncode}\n{proc.stdout}\n{proc.stderr}"
