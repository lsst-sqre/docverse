"""Add projects, builds, editions, and org_memberships tables.

Revision ID: b1c2d3e4f5a6
Revises: a1b2c3d4e5f6
Create Date: 2026-03-13 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b1c2d3e4f5a6"
down_revision: str | None = "a1b2c3d4e5f6"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # --- projects ---
    op.create_table(
        "projects",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("slug", sa.String(length=128), nullable=False),
        sa.Column("title", sa.String(length=256), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("doc_repo", sa.String(length=512), nullable=False),
        sa.Column(
            "slug_rewrite_rules",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "lifecycle_rules",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "date_created",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "date_updated",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("date_deleted", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "slug", name="uq_projects_org_slug"),
    )
    op.create_index("idx_projects_org_id", "projects", ["org_id"])

    # --- builds ---
    op.create_table(
        "builds",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "public_id", sa.BigInteger(), autoincrement=False, nullable=False
        ),
        sa.Column("project_id", sa.Integer(), nullable=False),
        sa.Column("git_ref", sa.String(length=256), nullable=False),
        sa.Column("alternate_name", sa.String(length=128), nullable=True),
        sa.Column("content_hash", sa.String(length=128), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "uploading",
                "processing",
                "completed",
                "failed",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("staging_key", sa.String(length=512), nullable=False),
        sa.Column("object_count", sa.Integer(), nullable=True),
        sa.Column("total_size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("uploader", sa.String(length=256), nullable=False),
        sa.Column(
            "annotations",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "date_created",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("date_uploaded", sa.DateTime(timezone=True), nullable=True),
        sa.Column("date_completed", sa.DateTime(timezone=True), nullable=True),
        sa.Column("date_deleted", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id"),
    )
    op.create_index("idx_builds_project_id", "builds", ["project_id"])
    op.create_index("idx_builds_status", "builds", ["status"])
    op.create_index("idx_builds_git_ref", "builds", ["git_ref"])

    # --- editions ---
    op.create_table(
        "editions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("slug", sa.String(length=128), nullable=False),
        sa.Column("title", sa.String(length=256), nullable=False),
        sa.Column("project_id", sa.Integer(), nullable=False),
        sa.Column(
            "kind",
            sa.Enum(
                "main",
                "release",
                "draft",
                "major",
                "minor",
                "alternate",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column(
            "tracking_mode",
            sa.Enum(
                "git_ref",
                "lsst_doc",
                "eups_major_release",
                "eups_weekly_release",
                "eups_daily_release",
                "semver_release",
                "semver_major",
                "semver_minor",
                "alternate_git_ref",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column(
            "tracking_params",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("current_build_id", sa.Integer(), nullable=True),
        sa.Column(
            "lifecycle_exempt",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "date_created",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "date_updated",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("date_deleted", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "project_id", "slug", name="uq_editions_project_slug"
        ),
    )
    op.create_index("idx_editions_project_id", "editions", ["project_id"])

    # --- org_memberships ---
    op.create_table(
        "org_memberships",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("principal", sa.String(length=256), nullable=False),
        sa.Column(
            "principal_type",
            sa.Enum("user", "group", native_enum=False, length=32),
            nullable=False,
        ),
        sa.Column(
            "role",
            sa.Enum(
                "reader",
                "uploader",
                "admin",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id",
            "principal_type",
            "principal",
            name="uq_org_memberships_org_type_principal",
        ),
    )
    op.create_index(
        "idx_org_memberships_org_id", "org_memberships", ["org_id"]
    )

    # --- FK constraints on queue_jobs ---
    op.create_foreign_key(
        "fk_queue_jobs_project_id",
        "queue_jobs",
        "projects",
        ["project_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_queue_jobs_build_id",
        "queue_jobs",
        "builds",
        ["build_id"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_queue_jobs_build_id", "queue_jobs", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_queue_jobs_project_id", "queue_jobs", type_="foreignkey"
    )
    op.drop_index("idx_org_memberships_org_id", table_name="org_memberships")
    op.drop_table("org_memberships")
    op.drop_index("idx_editions_project_id", table_name="editions")
    op.drop_table("editions")
    op.drop_index("idx_builds_git_ref", table_name="builds")
    op.drop_index("idx_builds_status", table_name="builds")
    op.drop_index("idx_builds_project_id", table_name="builds")
    op.drop_table("builds")
    op.drop_index("idx_projects_org_id", table_name="projects")
    op.drop_table("projects")
