"""Replace ``idx_builds_project_id`` with composite ``(project_id, git_ref)``.

The stale-build guard in ``build_processing`` runs
``SELECT max(id) FROM builds WHERE project_id = :p AND git_ref = :r`` at
every job entry. The existing single-column indexes on ``project_id`` and
``git_ref`` alone force a table scan of the project's build history. A
composite ``(project_id, git_ref)`` index serves that query directly and
also covers any remaining ``project_id``-only filters via its leading
column, so the old ``idx_builds_project_id`` is dropped. The
``idx_builds_git_ref`` single-column index is kept — the composite's
leading column is ``project_id``, so it cannot serve queries that filter
only by ``git_ref``.

Revision ID: l0m1n2o3p4q5
Revises: k9l0m1n2o3p4
Create Date: 2026-04-22 00:00:00.000000+00:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "l0m1n2o3p4q5"
down_revision: str | None = "k9l0m1n2o3p4"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_index(
        "idx_builds_project_id_git_ref",
        "builds",
        ["project_id", "git_ref"],
    )
    op.drop_index("idx_builds_project_id", table_name="builds")


def downgrade() -> None:
    op.create_index("idx_builds_project_id", "builds", ["project_id"])
    op.drop_index("idx_builds_project_id_git_ref", table_name="builds")
