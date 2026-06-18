"""workflow memo field

Adds ``memo`` (JSON, nullable) to ``workflow_runs``. Memo is immutable, unindexed
metadata set at ``engine.start()`` time and readable by ``engine.get_memo()`` and
``ctx.info().memo``. Unlike ``search_attributes`` (indexed, filterable, mutable),
memo is for arbitrary context that does not need to be queried — e.g. a human-readable
description, correlation IDs, or tags that are display-only.

Revision ID: 0009
Revises: 0008
Create Date: 2026-06-16
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("workflow_runs", schema=None) as batch_op:
        batch_op.add_column(sa.Column("memo", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("workflow_runs", schema=None) as batch_op:
        batch_op.drop_column("memo")
