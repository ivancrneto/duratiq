"""workflow updates

Adds ``workflow_updates`` — the inbox for ``engine.update``: synchronous, mutating
requests the workflow consumes at a ``ctx.wait_update`` point, with the handler's
result/error recorded for the caller to read back.

The codec-backed columns (``args``/``result``/``error``) are plain ``JSON`` at the
DDL level — the codec only transforms values in Python.

Revision ID: 0003
Revises: 0002
Create Date: 2026-06-14
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workflow_updates",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("args", sa.JSON(), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error", sa.JSON(), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_seq", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["run_id"], ["workflow_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_workflow_updates_run_id", "workflow_updates", ["run_id"], unique=False)
    op.create_index("ix_workflow_updates_status", "workflow_updates", ["status"], unique=False)


def downgrade() -> None:
    op.drop_table("workflow_updates")
