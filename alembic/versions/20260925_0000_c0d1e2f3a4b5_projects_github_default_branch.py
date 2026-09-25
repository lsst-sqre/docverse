"""Add ``projects.github_default_branch``.

First slice of the default-branch rename work (PRD #721 / DM-56241).
A project's ``__main`` edition stores a literal branch name, and nothing
recorded the repository's actual default branch, so a ``master`` →
``main`` rename left ``__main`` silently stuck on the old name. This
column is where Docverse remembers what GitHub reports as the default
branch; the ``project_github_resolve`` worker seeds it from the
``GET /repos/{owner}/{repo}`` response it already fetches.

Nullable with no data step: ``NULL`` means "not yet learned", and every
consumer falls back to ``"main"`` for it. Existing projects are filled
by their next resolve and by the daily ``git_ref_audit`` pass rather
than by this migration, which has no GitHub access.

Revision ID: c0d1e2f3a4b5
Revises: b9c0d1e2f3a4
Create Date: 2026-09-25 00:00:00.000000+00:00
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c0d1e2f3a4b5"
down_revision: str | None = "b9c0d1e2f3a4"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "projects",
        sa.Column(
            "github_default_branch", sa.String(length=255), nullable=True
        ),
    )


def downgrade() -> None:
    op.drop_column("projects", "github_default_branch")
