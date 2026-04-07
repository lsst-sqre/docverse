"""Add storage_prefix column to builds.

Stores the object store prefix for build artifacts, e.g.
``{project_slug}/__builds/{base32_id}/``.

Revision ID: a1b2c3d4e5f7
Revises: h6i7j8k9l0m1
Create Date: 2026-04-07 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f7"
down_revision: str | None = "h6i7j8k9l0m1"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "builds",
        sa.Column("storage_prefix", sa.String(512), nullable=False),
    )


def downgrade() -> None:
    op.drop_column("builds", "storage_prefix")
