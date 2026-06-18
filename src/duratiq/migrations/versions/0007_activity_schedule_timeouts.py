"""activity schedule-to-start and schedule-to-close timeouts

Adds two columns to ``workflow_steps``:

* ``schedule_to_start_at`` — the deadline before which a dispatched ACTIVITY step
  must be dequeued and started by a worker. If the activity sits in the queue past
  this deadline (i.e. workers are down or overloaded), the step is failed with a
  ``ScheduleToStartTimeout`` error. Indexed for the scanner.
* ``schedule_to_close_at`` — the total budget from dispatch to final completion,
  across all retries. Even if the per-attempt ``start_to_close_ms`` would allow
  another retry, the step is failed once this deadline elapses. Indexed for the
  scanner.

Both are ``None`` when the corresponding timeout was not configured (existing rows
are unaffected).

Revision ID: 0007
Revises: 0006
Create Date: 2026-06-16
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("workflow_steps", schema=None) as batch_op:
        batch_op.add_column(sa.Column("schedule_to_start_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("schedule_to_close_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.create_index(
            batch_op.f("ix_workflow_steps_schedule_to_start_at"), ["schedule_to_start_at"], unique=False
        )
        batch_op.create_index(
            batch_op.f("ix_workflow_steps_schedule_to_close_at"), ["schedule_to_close_at"], unique=False
        )


def downgrade() -> None:
    with op.batch_alter_table("workflow_steps", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_workflow_steps_schedule_to_close_at"))
        batch_op.drop_index(batch_op.f("ix_workflow_steps_schedule_to_start_at"))
        batch_op.drop_column("schedule_to_close_at")
        batch_op.drop_column("schedule_to_start_at")
