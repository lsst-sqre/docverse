"""Tests for the ``OrganizationStore`` storage layer."""

from __future__ import annotations

import pytest
import structlog
from pydantic import HttpUrl
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import KeeperSyncConfig, OrganizationCreate
from docverse.storage.organization_store import OrganizationStore


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
