"""SQLAlchemy ORM model for the ``dashboard_template_content_files`` table."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class SqlDashboardTemplateContentFile(Base):
    """ORM model for the ``dashboard_template_content_files`` table.

    One row per file in a synced template tree, keyed by
    ``(content_id, relative_path)``. ``is_text`` distinguishes Jinja /
    TOML / CSS / JS sources that are read as decoded text from binary
    assets (images, fonts, etc.) returned as raw bytes.
    """

    __tablename__ = "dashboard_template_content_files"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    content_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("dashboard_template_contents.id", ondelete="CASCADE"),
        nullable=False,
    )

    relative_path: Mapped[str] = mapped_column(String(512), nullable=False)

    is_text: Mapped[bool] = mapped_column(Boolean, nullable=False)

    data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)

    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "content_id",
            "relative_path",
            name="uq_dashboard_template_content_files_content_path",
        ),
    )
