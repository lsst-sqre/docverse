"""Add composite ``(project_id, content_hash)`` index on ``builds``.

The keeper-sync engine's dual-upload convergence lookup
(``BuildStore.get_completed_by_content_hash``) runs on every inbound LTD
``sync_build`` call, filtering ``builds`` on
``(project_id, content_hash, status, date_deleted)``. The convergence
query has to execute even when no match exists — that is how the sync
decides whether to copy. Without a leading ``(project_id, content_hash)``
index, this is a project-scoped scan whose cost grows with both project
size and sync frequency. A composite index on the leading filter columns
serves the lookup directly; the trailing ``status`` and ``date_deleted``
predicates are cheap to apply against the small candidate set the index
returns.

Revision ID: s7t8u9v0w1x2
Revises: r6s7t8u9v0w1
Create Date: 2026-05-07 00:01:00.000000+00:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "s7t8u9v0w1x2"
down_revision: str | None = "r6s7t8u9v0w1"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_index(
        "idx_builds_project_id_content_hash",
        "builds",
        ["project_id", "content_hash"],
    )


def downgrade() -> None:
    op.drop_index("idx_builds_project_id_content_hash", table_name="builds")
