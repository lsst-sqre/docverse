"""Tests for the ``OrganizationStore`` storage layer."""

from __future__ import annotations

import pytest
import structlog
from pydantic import HttpUrl
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import KeeperSyncConfig, OrganizationCreate
from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.storage.organization_store import OrganizationStore


async def _seed_org(session: AsyncSession, *, slug: str = "ks-org") -> int:
    logger = structlog.get_logger("test")
    org_store = OrganizationStore(session=session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title="KS Org",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id


@pytest.mark.asyncio
async def test_get_by_slug_returns_typed_keeper_sync_config(
    db_session: AsyncSession,
) -> None:
    """``get_by_slug`` round-trips a typed ``KeeperSyncConfig`` instance."""
    logger = structlog.get_logger("test")
    org_store = OrganizationStore(session=db_session, logger=logger)
    config = KeeperSyncConfig(
        enabled=True,
        ltd_base_url=HttpUrl("https://keeper.lsst.codes/"),
        project_slugs=["dmtn-001", "sqr-112"],
    )

    async with db_session.begin():
        await _seed_org(db_session)
        await org_store.update_keeper_sync_config(slug="ks-org", config=config)

    async with db_session.begin():
        org = await org_store.get_by_slug("ks-org")

    assert org is not None
    assert isinstance(org.keeper_sync_config, KeeperSyncConfig)
    assert org.keeper_sync_config.enabled is True
    assert org.keeper_sync_config.ltd_base_url == HttpUrl(
        "https://keeper.lsst.codes/"
    )
    assert org.keeper_sync_config.project_slugs == ["dmtn-001", "sqr-112"]


@pytest.mark.asyncio
async def test_get_by_slug_ignores_unknown_keeper_sync_config_keys(
    db_session: AsyncSession,
) -> None:
    """A stored config carrying an unknown key still loads, key dropped.

    This is the rollback case: a newer server writes a field this code
    does not know, then the deployment is rolled back and this code has
    to read the row it left behind. ``KeeperSyncConfig`` ignores the
    extra key rather than failing validation, so the org still loads and
    the known fields come back intact.
    """
    logger = structlog.get_logger("test")
    org_store = OrganizationStore(session=db_session, logger=logger)

    async with db_session.begin():
        await _seed_org(db_session, slug="ks-future")
        # Write the blob directly: the model would drop the unknown key
        # before it ever reached the column.
        await db_session.execute(
            update(SqlOrganization)
            .where(SqlOrganization.slug == "ks-future")
            .values(
                keeper_sync_config={
                    "enabled": True,
                    "ltd_base_url": "https://keeper.lsst.codes/",
                    "project_slugs": ["sqr-112"],
                    "future_scope_field": ["not-yet-a-thing"],
                }
            )
        )
        await db_session.commit()

    async with db_session.begin():
        org = await org_store.get_by_slug("ks-future")

    assert org is not None
    assert org.keeper_sync_config is not None
    assert org.keeper_sync_config.enabled is True
    assert org.keeper_sync_config.project_slugs == ["sqr-112"]
    assert "future_scope_field" not in org.keeper_sync_config.model_dump()


@pytest.mark.asyncio
async def test_create_mints_time_ordered_public_id(
    db_session: AsyncSession,
) -> None:
    """Orgs created in succession sort by ``public_id`` in that order."""
    logger = structlog.get_logger("test")
    org_store = OrganizationStore(session=db_session, logger=logger)

    async with db_session.begin():
        first = await org_store.create(
            OrganizationCreate(
                slug="pid-first",
                title="First",
                base_domain="pid-first.example.com",
            )
        )
        second = await org_store.create(
            OrganizationCreate(
                slug="pid-second",
                title="Second",
                base_domain="pid-second.example.com",
            )
        )
        await db_session.commit()

    assert first.public_id > 0
    assert second.public_id > first.public_id
