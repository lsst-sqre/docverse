"""Add dashboard GitHub template binding/template/template-file tables.

Adds the persistence layer for the GitHub-backed dashboard-template
system. Three tables land in one revision:

- ``dashboard_github_template_bindings`` records which GitHub source
  (``owner``/``repo``/``ref``/``root_path``) an org or project should
  use. ``project_id`` is nullable; ``NULL`` denotes the org default.
  PostgreSQL treats ``NULL`` as distinct in standard unique
  constraints, so the org default uniqueness is enforced via a partial
  unique index (``project_id IS NULL``) alongside the regular unique
  constraint on ``(org_id, project_id)``.

- ``dashboard_github_templates`` holds the synced bytes of one
  template tree, keyed by ``(github_owner, github_repo, github_ref,
  root_path)`` so multiple bindings pointing at the same source share
  one cached copy.

- ``dashboard_github_template_files`` holds one row per file in a
  template tree, unique on ``(github_template_id, relative_path)``.

Revision ID: m1n2o3p4q5r6
Revises: l0m1n2o3p4q5
Create Date: 2026-04-22 00:01:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "m1n2o3p4q5r6"
down_revision: str | None = "l0m1n2o3p4q5"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "dashboard_github_templates",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("github_owner", sa.String(256), nullable=False),
        sa.Column("github_repo", sa.String(256), nullable=False),
        sa.Column("github_ref", sa.String(256), nullable=False),
        sa.Column("root_path", sa.String(512), nullable=False),
        sa.Column("commit_sha", sa.String(64), nullable=False),
        sa.Column("etag", sa.String(256), nullable=False),
        sa.Column("template_toml", sa.LargeBinary, nullable=False),
        sa.Column(
            "date_synced",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "github_owner",
            "github_repo",
            "github_ref",
            "root_path",
            name="uq_dashboard_github_templates_source_key",
        ),
    )

    op.create_table(
        "dashboard_github_template_files",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "github_template_id",
            sa.Integer,
            sa.ForeignKey("dashboard_github_templates.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("relative_path", sa.String(512), nullable=False),
        sa.Column("is_text", sa.Boolean, nullable=False),
        sa.Column("data", sa.LargeBinary, nullable=False),
        sa.Column("size_bytes", sa.BigInteger, nullable=False),
        sa.UniqueConstraint(
            "github_template_id",
            "relative_path",
            name="uq_dashboard_github_template_files_template_path",
        ),
    )

    op.create_table(
        "dashboard_github_template_bindings",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "org_id",
            sa.Integer,
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "project_id",
            sa.Integer,
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("github_owner", sa.String(256), nullable=False),
        sa.Column("github_repo", sa.String(256), nullable=False),
        sa.Column("github_ref", sa.String(256), nullable=False),
        sa.Column(
            "root_path",
            sa.String(512),
            nullable=False,
            server_default="/",
        ),
        sa.Column(
            "github_template_id",
            sa.Integer,
            sa.ForeignKey(
                "dashboard_github_templates.id", ondelete="SET NULL"
            ),
            nullable=True,
        ),
        sa.Column(
            "last_sync_status",
            sa.String(32),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("last_sync_error", sa.Text, nullable=True),
        sa.Column(
            "date_created",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "date_updated",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "org_id",
            "project_id",
            name="uq_dashboard_github_template_bindings_org_project",
        ),
    )

    op.create_index(
        "uq_dashboard_github_template_bindings_org_default",
        "dashboard_github_template_bindings",
        ["org_id"],
        unique=True,
        postgresql_where=sa.text("project_id IS NULL"),
    )
    op.create_index(
        "idx_dashboard_github_template_bindings_org_id",
        "dashboard_github_template_bindings",
        ["org_id"],
    )
    op.create_index(
        "idx_dashboard_github_template_bindings_github_template_id",
        "dashboard_github_template_bindings",
        ["github_template_id"],
    )
    op.create_index(
        "idx_dashboard_github_template_bindings_repo_ref",
        "dashboard_github_template_bindings",
        ["github_owner", "github_repo", "github_ref"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_dashboard_github_template_bindings_repo_ref",
        table_name="dashboard_github_template_bindings",
    )
    op.drop_index(
        "idx_dashboard_github_template_bindings_github_template_id",
        table_name="dashboard_github_template_bindings",
    )
    op.drop_index(
        "idx_dashboard_github_template_bindings_org_id",
        table_name="dashboard_github_template_bindings",
    )
    op.drop_index(
        "uq_dashboard_github_template_bindings_org_default",
        table_name="dashboard_github_template_bindings",
    )
    op.drop_table("dashboard_github_template_bindings")
    op.drop_table("dashboard_github_template_files")
    op.drop_table("dashboard_github_templates")
