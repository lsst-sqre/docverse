"""SQLAlchemy ORM model for the ``organization_credentials`` table."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from .base import Base


class SqlOrganizationCredential(Base):
    """ORM model for the ``organization_credentials`` table."""

    __tablename__ = "organization_credentials"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    organization_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )

    label: Mapped[str] = mapped_column(String(128), nullable=False)

    provider: Mapped[str] = mapped_column(String(32), nullable=False)

    encrypted_credentials: Mapped[bytes] = mapped_column(
        LargeBinary, nullable=False
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

    __table_args__ = (
        UniqueConstraint(
            "organization_id", "label", name="uq_org_credential_label"
        ),
    )
