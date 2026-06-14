"""search attributes

Adds ``workflow_search_attributes`` — typed, indexed key/value metadata on a run,
set at start or via ``ctx.upsert_search_attributes``. ``engine.list_runs`` filters on
them. ``value`` is the typed value (plain JSON at the DDL level); ``value_index`` is
its canonical string form, indexed with ``key`` for efficient equality filtering.

Revision ID: 0005
Revises: 0004
Create Date: 2026-06-14
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workflow_search_attributes",
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("key", sa.String(length=255), nullable=False),
        sa.Column("value", sa.JSON(), nullable=True),
        sa.Column("value_index", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["workflow_runs.id"]),
        sa.PrimaryKeyConstraint("run_id", "key"),
    )
    op.create_index(
        "ix_workflow_search_attributes_key_value",
        "workflow_search_attributes",
        ["key", "value_index"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("workflow_search_attributes")
