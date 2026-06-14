"""activity heartbeat

Adds ``workflow_steps.heartbeat`` — the latest progress a long-running activity
reported via ``heartbeat(details)``. A retry of the same step reads it back to resume
from where the previous attempt left off. JSON at the DDL level (the codec only
transforms values in Python).

Revision ID: 0004
Revises: 0003
Create Date: 2026-06-14
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("workflow_steps", schema=None) as batch_op:
        batch_op.add_column(sa.Column("heartbeat", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("workflow_steps", schema=None) as batch_op:
        batch_op.drop_column("heartbeat")
