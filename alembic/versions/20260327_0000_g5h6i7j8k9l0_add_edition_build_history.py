"""Add edition_build_history table.

Logs every build that an edition has pointed to, enabling rollback
and orphan detection.

Revision ID: g5h6i7j8k9l0
Revises: f4a5b6c7d8e9
Create Date: 2026-03-27 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "g5h6i7j8k9l0"
down_revision: str | None = "f4a5b6c7d8e9"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "edition_build_history",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "edition_id",
            sa.Integer,
            sa.ForeignKey("editions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "build_id",
            sa.Integer,
            sa.ForeignKey("builds.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("position", sa.Integer, nullable=False),
        sa.Column(
            "date_created",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "idx_ebh_edition_id", "edition_build_history", ["edition_id"]
    )
    op.create_index(
        "idx_ebh_edition_position",
        "edition_build_history",
        ["edition_id", "position"],
    )


def downgrade() -> None:
    op.drop_table("edition_build_history")
