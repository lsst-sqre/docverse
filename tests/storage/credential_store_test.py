"""Tests for the OrganizationCredentialStore."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import OrganizationCreate
from docverse.storage.organization_credential_store import (
    OrganizationCredentialStore,
)
from docverse.storage.organization_store import OrganizationStore


async def _create_org(
    session: async_scoped_session[AsyncSession],
) -> int:
    logger = structlog.get_logger("test")
    org_store = OrganizationStore(session=session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug="cred-test-org",
            title="Cred Test Org",
            base_domain="cred.example.com",
        )
    )
    return org.id


@pytest.mark.asyncio
async def test_create_and_get(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        logger = structlog.get_logger("test")
        store = OrganizationCredentialStore(session=db_session, logger=logger)
        cred = await store.create(
            organization_id=org_id,
            label="my-cred",
            provider="aws",
            encrypted_credentials=b"encrypted-data",
        )
        assert cred.label == "my-cred"
        assert cred.provider == "aws"

        result = await store.get_by_label(
            organization_id=org_id, label="my-cred"
        )
        assert result is not None
        fetched_cred, encrypted = result
        assert fetched_cred.label == "my-cred"
        assert encrypted == b"encrypted-data"
        await db_session.rollback()


@pytest.mark.asyncio
async def test_list_by_org(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        logger = structlog.get_logger("test")
        store = OrganizationCredentialStore(session=db_session, logger=logger)
        await store.create(
            organization_id=org_id,
            label="alpha",
            provider="aws",
            encrypted_credentials=b"a",
        )
        await store.create(
            organization_id=org_id,
            label="beta",
            provider="cloudflare",
            encrypted_credentials=b"b",
        )
        creds = await store.list_by_org(org_id)
        assert len(creds) == 2
        assert creds[0].label == "alpha"
        assert creds[1].label == "beta"
        await db_session.rollback()


@pytest.mark.asyncio
async def test_delete(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        logger = structlog.get_logger("test")
        store = OrganizationCredentialStore(session=db_session, logger=logger)
        await store.create(
            organization_id=org_id,
            label="to-delete",
            provider="aws",
            encrypted_credentials=b"x",
        )
        deleted = await store.delete(organization_id=org_id, label="to-delete")
        assert deleted is True

        result = await store.get_by_label(
            organization_id=org_id, label="to-delete"
        )
        assert result is None
        await db_session.rollback()


@pytest.mark.asyncio
async def test_delete_nonexistent(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        logger = structlog.get_logger("test")
        store = OrganizationCredentialStore(session=db_session, logger=logger)
        deleted = await store.delete(organization_id=org_id, label="nope")
        assert deleted is False
        await db_session.rollback()
