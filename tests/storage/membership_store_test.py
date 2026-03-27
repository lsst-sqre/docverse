"""Tests for OrgMembershipStore."""

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
from docverse.storage.membership_store import OrgMembershipStore
from docverse.storage.organization_store import OrganizationStore


@pytest.fixture
def membership_store(
    db_session: async_scoped_session[AsyncSession],
) -> OrgMembershipStore:
    logger = structlog.get_logger("docverse")
    return OrgMembershipStore(session=db_session, logger=logger)


async def _create_org(
    db_session: async_scoped_session[AsyncSession],
) -> int:
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug="mem-org",
            title="Mem Org",
            base_domain="mem.example.com",
        )
    )
    return org.id


@pytest.mark.asyncio
async def test_create_membership(
    db_session: async_scoped_session[AsyncSession],
    membership_store: OrgMembershipStore,
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        member = await membership_store.create(
            org_id=org_id,
            data=OrgMembershipCreate(
                principal="jdoe",
                principal_type=PrincipalType.user,
                role=OrgRole.admin,
            ),
        )
        await db_session.commit()
    assert member.principal == "jdoe"
    assert member.principal_type == PrincipalType.user
    assert member.role == OrgRole.admin


@pytest.mark.asyncio
async def test_get_by_principal(
    db_session: async_scoped_session[AsyncSession],
    membership_store: OrgMembershipStore,
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        await membership_store.create(
            org_id=org_id,
            data=OrgMembershipCreate(
                principal="jdoe",
                principal_type=PrincipalType.user,
                role=OrgRole.reader,
            ),
        )
        found = await membership_store.get_by_principal(
            org_id=org_id,
            principal_type=PrincipalType.user,
            principal="jdoe",
        )
        await db_session.commit()
    assert found is not None
    assert found.principal == "jdoe"


@pytest.mark.asyncio
async def test_list_by_org(
    db_session: async_scoped_session[AsyncSession],
    membership_store: OrgMembershipStore,
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        await membership_store.create(
            org_id=org_id,
            data=OrgMembershipCreate(
                principal="alice",
                principal_type=PrincipalType.user,
                role=OrgRole.admin,
            ),
        )
        await membership_store.create(
            org_id=org_id,
            data=OrgMembershipCreate(
                principal="g_team",
                principal_type=PrincipalType.group,
                role=OrgRole.reader,
            ),
        )
        members = await membership_store.list_by_org(org_id)
        await db_session.commit()
    assert len(members) == 2


@pytest.mark.asyncio
async def test_delete_membership(
    db_session: async_scoped_session[AsyncSession],
    membership_store: OrgMembershipStore,
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        await membership_store.create(
            org_id=org_id,
            data=OrgMembershipCreate(
                principal="bob",
                principal_type=PrincipalType.user,
                role=OrgRole.uploader,
            ),
        )
        deleted = await membership_store.delete(
            org_id=org_id,
            principal_type=PrincipalType.user,
            principal="bob",
        )
        assert deleted is True
        found = await membership_store.get_by_principal(
            org_id=org_id,
            principal_type=PrincipalType.user,
            principal="bob",
        )
        await db_session.commit()
    assert found is None


@pytest.mark.asyncio
async def test_resolve_role_user(
    db_session: async_scoped_session[AsyncSession],
    membership_store: OrgMembershipStore,
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        await membership_store.create(
            org_id=org_id,
            data=OrgMembershipCreate(
                principal="alice",
                principal_type=PrincipalType.user,
                role=OrgRole.uploader,
            ),
        )
        result = await membership_store.resolve_role(
            org_id=org_id, username="alice", groups=[]
        )
        await db_session.commit()
    assert result is not None
    role, principal_type, group_name = result
    assert role == OrgRole.uploader
    assert principal_type == PrincipalType.user
    assert group_name is None


@pytest.mark.asyncio
async def test_resolve_role_highest_wins(
    db_session: async_scoped_session[AsyncSession],
    membership_store: OrgMembershipStore,
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        # User has reader directly
        await membership_store.create(
            org_id=org_id,
            data=OrgMembershipCreate(
                principal="alice",
                principal_type=PrincipalType.user,
                role=OrgRole.reader,
            ),
        )
        # Group gives admin
        await membership_store.create(
            org_id=org_id,
            data=OrgMembershipCreate(
                principal="g_admins",
                principal_type=PrincipalType.group,
                role=OrgRole.admin,
            ),
        )
        result = await membership_store.resolve_role(
            org_id=org_id, username="alice", groups=["g_admins"]
        )
        await db_session.commit()
    assert result is not None
    role, principal_type, group_name = result
    assert role == OrgRole.admin
    assert principal_type == PrincipalType.group
    assert group_name == "g_admins"


@pytest.mark.asyncio
async def test_resolve_role_no_membership(
    db_session: async_scoped_session[AsyncSession],
    membership_store: OrgMembershipStore,
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        result = await membership_store.resolve_role(
            org_id=org_id, username="nobody", groups=[]
        )
        await db_session.commit()
    assert result is None
