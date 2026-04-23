"""SQLAlchemy ORM model for ``dashboard_github_template_bindings``."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from .base import Base


class SqlDashboardGitHubTemplateBinding(Base):
    """ORM model for the ``dashboard_github_template_bindings`` table.

    A binding records the GitHub source (``owner``/``repo``/``ref``/
    ``root_path``) that an organization or a project should use for its
    dashboard template. ``project_id`` is nullable; ``NULL`` denotes the
    organization-wide default. A regular ``UniqueConstraint`` enforces
    one row per ``(org_id, project_id)`` for project-specific overrides;
    a partial unique index (``project_id IS NULL``) enforces a single
    org default because PostgreSQL treats ``NULL`` as distinct in
    standard unique constraints.
    """

    __tablename__ = "dashboard_github_template_bindings"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    org_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )

    project_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=True,
    )

    github_owner: Mapped[str] = mapped_column(String(256), nullable=False)
    github_repo: Mapped[str] = mapped_column(String(256), nullable=False)
    github_ref: Mapped[str] = mapped_column(String(256), nullable=False)
    root_path: Mapped[str] = mapped_column(
        String(512), nullable=False, default="/", server_default="/"
    )

    github_template_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("dashboard_github_templates.id", ondelete="SET NULL"),
        nullable=True,
    )

    last_sync_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="pending",
        server_default="pending",
    )

    last_sync_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    date_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    date_updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "project_id",
            name="uq_dashboard_github_template_bindings_org_project",
        ),
        Index(
            "uq_dashboard_github_template_bindings_org_default",
            "org_id",
            unique=True,
            postgresql_where=text("project_id IS NULL"),
        ),
        Index(
            "idx_dashboard_github_template_bindings_org_id",
            "org_id",
        ),
        Index(
            "idx_dashboard_github_template_bindings_github_template_id",
            "github_template_id",
        ),
        Index(
            "idx_dashboard_github_template_bindings_repo_ref",
            "github_owner",
            "github_repo",
            "github_ref",
        ),
    )
