"""Add editions.alternate_name.

Adds a nullable ``alternate_name VARCHAR(128)`` column to the
``editions`` table mirroring the existing column on ``builds``. The
column is populated by ``EditionTrackingService`` for deployment-scoped
(``alternate_git_ref``) tracking rules and surfaced into the dashboard
``EditionContext`` so templates can filter drafts per deployment target
(SQR-112). Existing rows remain ``NULL``; no backfill is required.

Revision ID: j8k9l0m1n2o3
Revises: i7j8k9l0m1n2
Create Date: 2026-04-17 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "j8k9l0m1n2o3"
down_revision: str | None = "i7j8k9l0m1n2"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "editions",
        sa.Column("alternate_name", sa.String(128), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("editions", "alternate_name")
