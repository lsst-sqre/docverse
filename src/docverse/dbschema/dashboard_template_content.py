"""SQLAlchemy ORM model for the ``dashboard_template_contents`` table."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, LargeBinary, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from .base import Base


class SqlDashboardTemplateContent(Base):
    """ORM model for the ``dashboard_template_contents`` table.

    Holds the synced bytes of a template tree. One row per unique
    ``(github_owner, github_repo, github_ref, root_path)`` so multiple
    bindings pointing at the same source share a single cached copy.
    """

    __tablename__ = "dashboard_template_contents"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    github_owner: Mapped[str] = mapped_column(String(256), nullable=False)
    github_repo: Mapped[str] = mapped_column(String(256), nullable=False)
    github_ref: Mapped[str] = mapped_column(String(256), nullable=False)
    root_path: Mapped[str] = mapped_column(String(512), nullable=False)

    commit_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    etag: Mapped[str] = mapped_column(String(256), nullable=False)

    template_toml: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)

    date_synced: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "github_owner",
            "github_repo",
            "github_ref",
            "root_path",
            name="uq_dashboard_template_contents_source_key",
        ),
    )
