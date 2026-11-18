"""Migration checks run by CI (.github/workflows/migrations.yml); also runnable locally.

    python scripts/check_migrations.py history
    DURATIQ_DATABASE_URL=... python scripts/check_migrations.py stairway

``history`` needs no database. It fails, naming the files involved, if:

* two migration files declare the same revision ID. Alembic only *warns* about
  this and keeps one of them, so `upgrade head` then fails with a misleading
  "multiple heads" error;
* a migration's ``down_revision`` points at a revision that doesn't exist;
* there is more than one head — typically two PRs that each added a migration
  on top of the same parent.

``stairway`` is DESTRUCTIVE — it downgrades to base, dropping every table — so it
only runs against an explicitly set ``DURATIQ_DATABASE_URL`` (use a throwaway
database). It upgrades to head, walks down to base one revision at a time, then for
each revision in order runs upgrade -> downgrade -> upgrade, and finally ``check``s
the result against the models. That catches a ``downgrade()`` that fails, one that
leaves something behind (the re-upgrade collides with it), and one that removes
something its upgrade doesn't recreate. A plain round trip to base misses the
"left behind" case: dropping the tables at the bottom hides the leftover.
"""

from __future__ import annotations

import ast
import os
import sys
import traceback
import warnings
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import NoReturn

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "src" / "duratiq" / "migrations"
VERSIONS = MIGRATIONS / "versions"


def fail(message: str) -> NoReturn:
    # `::error::` renders as an annotation on GitHub; plain text elsewhere.
    prefix = "::error title=migrations::" if os.getenv("GITHUB_ACTIONS") == "true" else "ERROR: "
    print(prefix + message.replace("\n", "%0A" if prefix.startswith("::") else "\n"), file=sys.stderr)
    sys.exit(1)


def config() -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    # alembic.ini's paths are relative to the repo root; make them cwd-independent.
    cfg.set_main_option("script_location", str(MIGRATIONS))
    return cfg


def _declared(path: Path) -> dict[str, object]:
    """Module-level ``revision`` / ``down_revision`` literals, read without importing."""
    try:
        tree = ast.parse(path.read_text(), filename=str(path))
    except SyntaxError as exc:
        fail(f"{path.name}: cannot parse migration: {exc}")
    found: dict[str, object] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target, value = node.targets[0], node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            target, value = node.target, node.value
        else:
            continue
        if isinstance(target, ast.Name) and target.id in ("revision", "down_revision"):
            try:
                found[target.id] = ast.literal_eval(value)
            except ValueError:
                fail(f"{path.name}: `{target.id}` must be a literal")
    return found


def history() -> None:
    files = sorted(VERSIONS.glob("*.py"))
    by_revision: dict[str, list[str]] = defaultdict(list)
    parents: dict[str, object] = {}
    for path in files:
        declared = _declared(path)
        revision = declared.get("revision")
        if not isinstance(revision, str):
            fail(f'{path.name}: no `revision = "..."` declared')
        by_revision[revision].append(path.name)
        parents[path.name] = declared.get("down_revision")

    duplicates = {rev: names for rev, names in by_revision.items() if len(names) > 1}
    if duplicates:
        lines = [f"  {rev!r}: {', '.join(names)}" for rev, names in sorted(duplicates.items())]
        fail(
            "duplicate revision IDs (often two PRs that each picked the next number):\n"
            + "\n".join(lines)
            + "\nGive yours the next free ID after the current head and set its down_revision to that head."
        )

    for name, parent in sorted(parents.items()):
        for p in parent if isinstance(parent, (tuple, list)) else [parent]:
            if p is not None and p not in by_revision:
                fail(f"{name}: down_revision {p!r} does not exist (renamed or removed upstream?)")

    with warnings.catch_warnings():
        warnings.simplefilter("error")  # anything alembic would only warn about is a failure here
        script = ScriptDirectory.from_config(config())
        heads = script.get_heads()
    if len(heads) != 1:
        detail = []
        for head in sorted(heads):
            rev = script.get_revision(head)
            assert rev is not None
            detail.append(f"  {head} ({Path(rev.path).name}, down_revision={rev.down_revision!r})")
        fail(
            f"{len(heads)} migration heads, expected 1:\n"
            + "\n".join(detail)
            + "\nThese branch from the same parent — usually two PRs that each added a migration. Rebase onto"
            " main and set your migration's down_revision to the latest head (or `alembic merge heads`)."
        )
    print(f"migration history OK: {len(files)} revisions, single head {heads[0]}")


def _step(label: str, hint: str, action: Callable[[], None]) -> None:
    print(f"--- {label}", flush=True)
    try:
        action()
    except Exception as exc:  # noqa: BLE001 - report any failure with context, then exit
        traceback.print_exc()
        fail(f"{label} failed: {type(exc).__name__}: {exc}\n{hint}")


def stairway() -> None:
    url = os.getenv("DURATIQ_DATABASE_URL")
    if not url:
        fail("stairway drops every table: set DURATIQ_DATABASE_URL to a throwaway database")
    print(f"stairway against {make_url(url).render_as_string(hide_password=True)}")
    cfg = config()
    revisions = [s.revision for s in reversed(list(ScriptDirectory.from_config(cfg).walk_revisions()))]

    # Walk down from head one revision at a time, so a failure names the migration.
    _step("upgrade to head", "Upgrading to head fails.", lambda: command.upgrade(cfg, "head"))
    for rev in reversed(revisions):
        _step(
            f"{rev}: downgrade (walking down from head)",
            f"{rev}'s downgrade() fails on a fully migrated database — or a later revision's downgrade() "
            "already removed what this one drops.",
            lambda: command.downgrade(cfg, "-1"),
        )
    for rev in revisions:
        _step(
            f"{rev}: upgrade",
            f"Upgrading to {rev} fails from a clean previous revision.",
            lambda rev=rev: command.upgrade(cfg, rev),
        )
        _step(f"{rev}: downgrade", f"{rev}'s downgrade() fails.", lambda: command.downgrade(cfg, "-1"))
        _step(
            f"{rev}: upgrade again",
            f"{rev}'s downgrade() left something behind that its upgrade() recreates.",
            lambda rev=rev: command.upgrade(cfg, rev),
        )
    _step(
        "check against the models",
        "After stepping down and up, the schema differs from the models: a downgrade() removed something "
        "that its own upgrade() doesn't recreate. (If `alembic check` fails right after a fresh "
        "`upgrade head` too, it's plain model drift instead.)",
        lambda: command.check(cfg),
    )
    print(f"stairway OK: {len(revisions)} revisions each upgraded, downgraded and upgraded again")


COMMANDS = {"history": history, "stairway": stairway}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in COMMANDS:
        sys.exit(f"usage: {sys.argv[0]} {{{'|'.join(COMMANDS)}}}")
    COMMANDS[sys.argv[1]]()
