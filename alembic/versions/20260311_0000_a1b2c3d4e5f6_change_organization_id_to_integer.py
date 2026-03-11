"""Change organization id from UUID to auto-incrementing integer.

Revision ID: a1b2c3d4e5f6
Revises: cf3b3f99916b
Create Date: 2026-03-11 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: str | None = "cf3b3f99916b"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # Early development — no production data to migrate.
    # Drop the UUID column and recreate as auto-incrementing integer.
    op.drop_constraint("organizations_pkey", "organizations", type_="primary")
    op.drop_column("organizations", "id")
    op.add_column(
        "organizations",
        sa.Column(
            "id",
            sa.Integer(),
            sa.Identity(always=False),
            nullable=False,
        ),
    )
    op.create_primary_key("organizations_pkey", "organizations", ["id"])


def downgrade() -> None:
    op.drop_constraint("organizations_pkey", "organizations", type_="primary")
    op.drop_column("organizations", "id")
    op.add_column(
        "organizations",
        sa.Column("id", sa.Uuid(), nullable=False),
    )
    op.create_primary_key("organizations_pkey", "organizations", ["id"])
