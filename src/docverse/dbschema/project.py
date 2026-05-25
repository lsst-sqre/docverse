"""SQLAlchemy ORM model for the ``projects`` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class SqlProject(Base):
    """ORM model for the ``projects`` table."""

    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    slug: Mapped[str] = mapped_column(String(128), nullable=False)

    title: Mapped[str] = mapped_column(String(256), nullable=False)

    org_id: Mapped[int] = mapped_column(Integer, nullable=False)

    # Free-form URL to the documentation source repository. Nullable so
    # projects can be created GitHub-binding-only (with ``github_owner``
    # / ``github_repo`` populated) without a redundant URL string.
    source_url: Mapped[str | None] = mapped_column(String(512), nullable=True)

    # Structured GitHub coordinates for the documentation source repo.
    # ``github_owner`` / ``github_repo`` are the operator-supplied source
    # of truth; the ``_id`` and ``installation_id`` columns are captured
    # opportunistically and may stay NULL forever for projects whose
    # GitHub App is not installed. The check constraint enforces that
    # ``github_owner`` and ``github_repo`` are populated together.
    github_owner: Mapped[str | None] = mapped_column(String(39), nullable=True)
    github_repo: Mapped[str | None] = mapped_column(String(100), nullable=True)
    github_owner_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    github_repo_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    github_installation_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )

    slug_rewrite_rules: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

    lifecycle_rules: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

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

    date_deleted: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("org_id", "slug", name="uq_projects_org_slug"),
        CheckConstraint(
            "(github_owner IS NULL) = (github_repo IS NULL)",
            name="ck_projects_github_owner_repo_both_or_neither",
        ),
        Index("idx_projects_org_id", "org_id"),
        Index(
            "idx_projects_slug_trgm",
            "slug",
            postgresql_using="gin",
            postgresql_ops={"slug": "gin_trgm_ops"},
        ),
        Index(
            "idx_projects_title_trgm",
            "title",
            postgresql_using="gin",
            postgresql_ops={"title": "gin_trgm_ops"},
        ),
        Index(
            "idx_projects_github_owner_repo",
            text("lower(github_owner)"),
            text("lower(github_repo)"),
        ),
        Index(
            "idx_projects_github_repo_id",
            "github_repo_id",
        ),
    )
