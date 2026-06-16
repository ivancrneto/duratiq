"""workflow-level timeouts and attempt counter

Adds three columns to ``workflow_runs``:

* ``execution_timeout_at`` — the absolute deadline for the entire workflow lifetime
  (including all ``continue_as_new`` iterations). Indexed for the scanner.
* ``run_timeout_at`` — the deadline for the current run only; reset to a fresh
  deadline when ``continue_as_new`` fires. Indexed for the scanner.
* ``attempt`` — monotonically increasing retry/reset counter, incremented by
  ``engine.retry()`` and ``engine.reset_to_step()``. Starts at 1.

Revision ID: 0006
Revises: 0005
Create Date: 2026-06-16
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("workflow_runs", schema=None) as batch_op:
        batch_op.add_column(sa.Column("execution_timeout_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("run_timeout_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("attempt", sa.Integer(), nullable=True, server_default="1"))
        batch_op.create_index(
            batch_op.f("ix_workflow_runs_execution_timeout_at"), ["execution_timeout_at"], unique=False
        )
        batch_op.create_index(
            batch_op.f("ix_workflow_runs_run_timeout_at"), ["run_timeout_at"], unique=False
        )


def downgrade() -> None:
    with op.batch_alter_table("workflow_runs", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_workflow_runs_run_timeout_at"))
        batch_op.drop_index(batch_op.f("ix_workflow_runs_execution_timeout_at"))
        batch_op.drop_column("attempt")
        batch_op.drop_column("run_timeout_at")
        batch_op.drop_column("execution_timeout_at")
