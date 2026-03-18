"""Add pg_trgm extension and trigram indexes on projects.

Revision ID: d3e4f5a6b7c8
Revises: c2d3e4f5a6b7
Create Date: 2026-03-17 00:00:00.000000+00:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d3e4f5a6b7c8"
down_revision: str | None = "c2d3e4f5a6b7"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.create_index(
        "idx_projects_slug_trgm",
        "projects",
        ["slug"],
        postgresql_using="gin",
        postgresql_ops={"slug": "gin_trgm_ops"},
    )
    op.create_index(
        "idx_projects_title_trgm",
        "projects",
        ["title"],
        postgresql_using="gin",
        postgresql_ops={"title": "gin_trgm_ops"},
    )


def downgrade() -> None:
    op.drop_index("idx_projects_title_trgm", table_name="projects")
    op.drop_index("idx_projects_slug_trgm", table_name="projects")
