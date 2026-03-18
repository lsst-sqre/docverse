"""Tests for AuthorizationService."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import (
    OrganizationCreate,
    OrgMembershipCreate,
    OrgRole,
    PrincipalType,
)
from docverse.exceptions import PermissionDeniedError
from docverse.services.authorization import AuthorizationService
from docverse.storage.membership_store import OrgMembershipStore
from docverse.storage.organization_store import OrganizationStore


async def _setup(
    db_session: async_scoped_session[AsyncSession],
) -> tuple[int, OrgMembershipStore]:
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug="auth-org",
            title="Auth Org",
            base_domain="auth.example.com",
        )
    )
    store = OrgMembershipStore(session=db_session, logger=logger)
    return org.id, store


@pytest.mark.asyncio
async def test_require_role_sufficient(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        org_id, store = await _setup(db_session)
        await store.create(
            org_id=org_id,
            data=OrgMembershipCreate(
                principal="alice",
                principal_type=PrincipalType.user,
                role=OrgRole.admin,
            ),
        )
        service = AuthorizationService(membership_store=store, logger=logger)
        role = await service.require_role(
            org_id=org_id,
            username="alice",
            groups=[],
            minimum_role=OrgRole.reader,
        )
        await db_session.commit()
    assert role == OrgRole.admin


@pytest.mark.asyncio
async def test_require_role_insufficient(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        org_id, store = await _setup(db_session)
        await store.create(
            org_id=org_id,
            data=OrgMembershipCreate(
                principal="bob",
                principal_type=PrincipalType.user,
                role=OrgRole.reader,
            ),
        )
        service = AuthorizationService(membership_store=store, logger=logger)
        with pytest.raises(PermissionDeniedError):
            await service.require_role(
                org_id=org_id,
                username="bob",
                groups=[],
                minimum_role=OrgRole.admin,
            )
        await db_session.commit()


@pytest.mark.asyncio
async def test_require_role_no_membership(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        org_id, store = await _setup(db_session)
        service = AuthorizationService(membership_store=store, logger=logger)
        with pytest.raises(PermissionDeniedError):
            await service.require_role(
                org_id=org_id,
                username="nobody",
                groups=[],
                minimum_role=OrgRole.reader,
            )
        await db_session.commit()
