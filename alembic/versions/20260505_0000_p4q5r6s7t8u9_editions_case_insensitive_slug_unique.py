"""Replace editions UNIQUE(project_id, slug) with case-insensitive index.

Drops the case-sensitive ``uq_editions_project_slug`` UNIQUE constraint
and replaces it with a functional unique index on
``(project_id, lower(slug))`` so a project cannot hold two editions
whose slugs differ only by case (e.g. ``DM-54112`` and ``dm-54112``).

PR #295 widened ``EditionCreate.slug`` to permit uppercase ticket-style
slugs without adjusting the table-level uniqueness rule, which would
otherwise let case-only duplicates land as siblings — confusing or
shadowing each other under the per-project ``/v/{slug}/`` URL segment.

Revision ID: p4q5r6s7t8u9
Revises: o3p4q5r6s7t8
Create Date: 2026-05-05 00:00:00.000000+00:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "p4q5r6s7t8u9"
down_revision: str | None = "o3p4q5r6s7t8"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.drop_constraint(
        "uq_editions_project_slug",
        "editions",
        type_="unique",
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_editions_project_lower_slug "
        "ON editions (project_id, lower(slug))"
    )


def downgrade() -> None:
    op.drop_index("uq_editions_project_lower_slug", table_name="editions")
    op.create_unique_constraint(
        "uq_editions_project_slug",
        "editions",
        ["project_id", "slug"],
    )
