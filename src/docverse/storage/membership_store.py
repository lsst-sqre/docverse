"""Database operations for the org_memberships table."""

from __future__ import annotations

import structlog
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrgMembershipCreate, OrgRole, PrincipalType
from docverse.dbschema.membership import SqlOrgMembership
from docverse.domain.membership import ROLE_RANK, OrgMembership


class OrgMembershipStore:
    """Direct database operations for organization memberships."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(
        self, *, org_id: int, data: OrgMembershipCreate
    ) -> OrgMembership:
        """Insert a new membership row."""
        row = SqlOrgMembership(
            org_id=org_id,
            principal=data.principal,
            principal_type=data.principal_type,
            role=data.role,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return OrgMembership.model_validate(row)

    async def get_by_principal(
        self,
        *,
        org_id: int,
        principal_type: PrincipalType,
        principal: str,
    ) -> OrgMembership | None:
        """Fetch a membership by org, type, and principal."""
        result = await self._session.execute(
            select(SqlOrgMembership).where(
                SqlOrgMembership.org_id == org_id,
                SqlOrgMembership.principal_type == principal_type,
                SqlOrgMembership.principal == principal,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return OrgMembership.model_validate(row)

    async def update_role(
        self,
        *,
        org_id: int,
        principal_type: PrincipalType,
        principal: str,
        role: OrgRole,
    ) -> OrgMembership | None:
        """Update a membership's role in place.

        Returns
        -------
        OrgMembership or None
            The updated membership, or None if no matching membership
            exists.
        """
        result = await self._session.execute(
            select(SqlOrgMembership).where(
                SqlOrgMembership.org_id == org_id,
                SqlOrgMembership.principal_type == principal_type,
                SqlOrgMembership.principal == principal,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        row.role = role
        await self._session.flush()
        await self._session.refresh(row)
        return OrgMembership.model_validate(row)

    async def list_by_org(self, org_id: int) -> list[OrgMembership]:
        """List all memberships for an organization."""
        result = await self._session.execute(
            select(SqlOrgMembership)
            .where(SqlOrgMembership.org_id == org_id)
            .order_by(
                SqlOrgMembership.principal_type,
                SqlOrgMembership.principal,
            )
        )
        rows = result.scalars().all()
        return [OrgMembership.model_validate(r) for r in rows]

    async def delete(
        self,
        *,
        org_id: int,
        principal_type: PrincipalType,
        principal: str,
    ) -> bool:
        """Delete a membership.

        Returns
        -------
        bool
            True if deleted, False if not found.
        """
        result = await self._session.execute(
            select(SqlOrgMembership).where(
                SqlOrgMembership.org_id == org_id,
                SqlOrgMembership.principal_type == principal_type,
                SqlOrgMembership.principal == principal,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return False
        await self._session.delete(row)
        return True

    async def list_effective_roles(
        self,
        *,
        username: str,
        groups: list[str],
    ) -> dict[int, OrgRole]:
        """Resolve the caller's effective role in every org they belong to.

        Queries all memberships matching the user (by username) or any of
        the caller's groups, across every organization, and collapses them
        to the single highest-ranked role per org.

        Parameters
        ----------
        username
            The authenticated username.
        groups
            The caller's group names.

        Returns
        -------
        dict
            Mapping of ``org_id`` to the caller's effective
            :class:`~docverse.client.models.OrgRole` for that org. Orgs in
            which the caller holds no membership are absent.
        """
        conditions = [
            # Direct user membership
            (
                (SqlOrgMembership.principal_type == PrincipalType.user)
                & (SqlOrgMembership.principal == username)
            ),
        ]
        if groups:
            # Group memberships
            conditions.append(
                (SqlOrgMembership.principal_type == PrincipalType.group)
                & (SqlOrgMembership.principal.in_(groups))
            )

        result = await self._session.execute(
            select(SqlOrgMembership).where(or_(*conditions))
        )
        rows = result.scalars().all()

        best: dict[int, OrgRole] = {}
        for row in rows:
            role = OrgRole(row.role)
            current = best.get(row.org_id)
            if current is None or ROLE_RANK[role] > ROLE_RANK[current]:
                best[row.org_id] = role
        return best

    async def resolve_role(
        self,
        *,
        org_id: int,
        username: str,
        groups: list[str],
    ) -> tuple[OrgRole, PrincipalType, str | None] | None:
        """Resolve the effective role for a user in an organization.

        Queries all matching memberships (user by username OR group by
        any group name) and returns the highest role along with how it
        was determined.

        Returns
        -------
        tuple or None
            ``(role, principal_type, group_name)`` for the winning
            membership, or None if no matching memberships.
            ``group_name`` is the principal value when the winning
            membership is a group, otherwise None.
        """
        conditions = [
            # Direct user membership
            (
                (SqlOrgMembership.principal_type == PrincipalType.user)
                & (SqlOrgMembership.principal == username)
            ),
        ]
        if groups:
            # Group memberships
            conditions.append(
                (SqlOrgMembership.principal_type == PrincipalType.group)
                & (SqlOrgMembership.principal.in_(groups))
            )

        result = await self._session.execute(
            select(SqlOrgMembership).where(
                SqlOrgMembership.org_id == org_id,
                or_(*conditions),
            )
        )
        rows = result.scalars().all()
        if not rows:
            return None

        best = max(rows, key=lambda r: ROLE_RANK[OrgRole(r.role)])
        return (
            OrgRole(best.role),
            PrincipalType(best.principal_type),
            best.principal
            if best.principal_type == PrincipalType.group
            else None,
        )
