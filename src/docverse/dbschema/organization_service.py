"""SQLAlchemy ORM model for the ``organization_services`` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from .base import Base


class SqlOrganizationService(Base):
    """ORM model for the ``organization_services`` table."""

    __tablename__ = "organization_services"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    organization_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )

    label: Mapped[str] = mapped_column(String(128), nullable=False)

    category: Mapped[str] = mapped_column(String(32), nullable=False)

    provider: Mapped[str] = mapped_column(String(32), nullable=False)

    config: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )

    credential_label: Mapped[str] = mapped_column(String(128), nullable=False)

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
            "organization_id", "label", name="uq_org_service_label"
        ),
    )
