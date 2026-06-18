"""schedule overlap policy

Adds ``overlap_policy`` to ``workflow_schedules``. Controls what happens when the
scanner fires a schedule while the previous run from that schedule is still active:

* ``ALLOW`` (default) — always start a new run regardless.
* ``SKIP`` — skip this iteration; advance ``next_fire_at`` without starting a run.
* ``REPLACE`` — cancel the still-running run then start a new one.
* ``TERMINATE`` — terminate (fail) the still-running run then start a new one.

Revision ID: 0008
Revises: 0007
Create Date: 2026-06-16
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("workflow_schedules", schema=None) as batch_op:
        batch_op.add_column(sa.Column("overlap_policy", sa.String(length=20), nullable=True, server_default="ALLOW"))


def downgrade() -> None:
    with op.batch_alter_table("workflow_schedules", schema=None) as batch_op:
        batch_op.drop_column("overlap_policy")
