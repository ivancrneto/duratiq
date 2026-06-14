"""activity start-to-close timeout

Adds ``workflow_steps.timeout_at`` — the per-attempt deadline for an outstanding
ACTIVITY step. The activity-timeout scanner uses it (indexed) to find activities
that were dispatched but never reported back, so a hung or lost activity can't wedge
its run forever.

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-14
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("workflow_steps", schema=None) as batch_op:
        batch_op.add_column(sa.Column("timeout_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.create_index(batch_op.f("ix_workflow_steps_timeout_at"), ["timeout_at"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("workflow_steps", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_workflow_steps_timeout_at"))
        batch_op.drop_column("timeout_at")
