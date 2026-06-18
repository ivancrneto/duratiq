"""workflow_id column

Adds ``workflow_id`` (VARCHAR 255, nullable, indexed) to ``workflow_runs``. This is a
user-supplied business identifier distinct from the internal UUID ``id`` and the
``idempotency_key``. Combined with ``workflow_id_reuse_policy`` on ``engine.start()``,
it enables Temporal-style "start-or-reuse" semantics for named entities.

Revision ID: 0010
Revises: 0009
Create Date: 2026-06-16
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("workflow_runs", schema=None) as batch_op:
        batch_op.add_column(sa.Column("workflow_id", sa.String(255), nullable=True))
        batch_op.create_index("ix_workflow_runs_workflow_id", ["workflow_id"])


def downgrade() -> None:
    with op.batch_alter_table("workflow_runs", schema=None) as batch_op:
        batch_op.drop_index("ix_workflow_runs_workflow_id")
        batch_op.drop_column("workflow_id")
