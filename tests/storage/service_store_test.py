"""Tests for the OrganizationServiceStore."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import OrganizationCreate
from docverse.storage.organization_service_store import (
    OrganizationServiceStore,
)
from docverse.storage.organization_store import OrganizationStore


async def _create_org(
    session: async_scoped_session[AsyncSession],
) -> int:
    logger = structlog.get_logger("test")
    org_store = OrganizationStore(session=session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug="svc-test-org",
            title="Service Test Org",
            base_domain="svc.example.com",
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
        store = OrganizationServiceStore(session=db_session, logger=logger)
        svc = await store.create(
            organization_id=org_id,
            label="my-s3",
            category="object_storage",
            provider="aws_s3",
            config={"bucket": "my-bucket", "region": "us-east-1"},
            credential_label="primary-aws",
        )
        assert svc.label == "my-s3"
        assert svc.provider == "aws_s3"
        assert svc.category == "object_storage"
        assert svc.config == {"bucket": "my-bucket", "region": "us-east-1"}
        assert svc.credential_label == "primary-aws"

        result = await store.get_by_label(
            organization_id=org_id, label="my-s3"
        )
        assert result is not None
        assert result.label == "my-s3"
        await db_session.rollback()


@pytest.mark.asyncio
async def test_list_by_org(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        logger = structlog.get_logger("test")
        store = OrganizationServiceStore(session=db_session, logger=logger)
        await store.create(
            organization_id=org_id,
            label="alpha-store",
            category="object_storage",
            provider="aws_s3",
            config={},
            credential_label="aws-cred",
        )
        await store.create(
            organization_id=org_id,
            label="beta-cdn",
            category="cdn",
            provider="fastly",
            config={},
            credential_label="fastly-cred",
        )
        services = await store.list_by_org(org_id)
        assert len(services) == 2
        labels = [s.label for s in services]
        assert "alpha-store" in labels
        assert "beta-cdn" in labels
        await db_session.rollback()


@pytest.mark.asyncio
async def test_list_by_category(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        logger = structlog.get_logger("test")
        store = OrganizationServiceStore(session=db_session, logger=logger)
        await store.create(
            organization_id=org_id,
            label="s3-store",
            category="object_storage",
            provider="aws_s3",
            config={},
            credential_label="aws-cred",
        )
        await store.create(
            organization_id=org_id,
            label="my-cdn",
            category="cdn",
            provider="fastly",
            config={},
            credential_label="fastly-cred",
        )
        stores = await store.list_by_category(
            organization_id=org_id, category="object_storage"
        )
        assert len(stores) == 1
        assert stores[0].label == "s3-store"
        await db_session.rollback()


@pytest.mark.asyncio
async def test_delete(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    async with db_session.begin():
        org_id = await _create_org(db_session)
        logger = structlog.get_logger("test")
        store = OrganizationServiceStore(session=db_session, logger=logger)
        await store.create(
            organization_id=org_id,
            label="to-delete",
            category="object_storage",
            provider="aws_s3",
            config={},
            credential_label="aws-cred",
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
        store = OrganizationServiceStore(session=db_session, logger=logger)
        deleted = await store.delete(organization_id=org_id, label="nope")
        assert deleted is False
        await db_session.rollback()
