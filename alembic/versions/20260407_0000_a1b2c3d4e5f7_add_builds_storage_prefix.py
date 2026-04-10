"""Add storage_prefix column to builds.

Stores the object store prefix for build artifacts, e.g.
``{project_slug}/__builds/{base32_id}/``.

Revision ID: a1b2c3d4e5f7
Revises: h6i7j8k9l0m1
Create Date: 2026-04-07 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op
from docverse.domain.base32id import serialize_base32_id

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f7"
down_revision: str | None = "h6i7j8k9l0m1"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # Step 1: Add column as nullable
    op.add_column(
        "builds",
        sa.Column("storage_prefix", sa.String(512), nullable=True),
    )

    # Step 2: Backfill existing rows
    conn = op.get_bind()
    rows = conn.execute(
        sa.text(
            "SELECT builds.id, builds.public_id, projects.slug "
            "FROM builds JOIN projects ON builds.project_id = projects.id"
        )
    ).fetchall()
    for build_id, public_id, project_slug in rows:
        base32_id = serialize_base32_id(public_id)
        storage_prefix = f"{project_slug}/__builds/{base32_id}/"
        conn.execute(
            sa.text(
                "UPDATE builds SET storage_prefix = :prefix WHERE id = :id"
            ),
            {"prefix": storage_prefix, "id": build_id},
        )

    # Step 3: Set column to NOT NULL
    op.alter_column("builds", "storage_prefix", nullable=False)


def downgrade() -> None:
    op.drop_column("builds", "storage_prefix")
