"""SQLAlchemy ORM model for the ``org_memberships`` table."""

from __future__ import annotations

from sqlalchemy import Enum, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from docverse.client.models import OrgRole, PrincipalType

from .base import Base


class SqlOrgMembership(Base):
    """ORM model for the ``org_memberships`` table."""

    __tablename__ = "org_memberships"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    org_id: Mapped[int] = mapped_column(Integer, nullable=False)

    principal: Mapped[str] = mapped_column(String(256), nullable=False)

    principal_type: Mapped[PrincipalType] = mapped_column(
        Enum(PrincipalType, native_enum=False, length=32),
        nullable=False,
    )

    role: Mapped[OrgRole] = mapped_column(
        Enum(OrgRole, native_enum=False, length=32),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "principal_type",
            "principal",
            name="uq_org_memberships_org_type_principal",
        ),
        Index("idx_org_memberships_org_id", "org_id"),
    )
