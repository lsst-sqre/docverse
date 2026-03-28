"""Add default_edition_config column to organizations.

Stores the organization-level default tracking configuration for
the __main edition created on new projects.

Revision ID: h6i7j8k9l0m1
Revises: g5h6i7j8k9l0
Create Date: 2026-03-27 00:01:00.000000+00:00
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "h6i7j8k9l0m1"
down_revision: str | None = "g5h6i7j8k9l0"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "organizations",
        sa.Column("default_edition_config", JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("organizations", "default_edition_config")
