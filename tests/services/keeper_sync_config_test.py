"""Tests for :class:`KeeperSyncConfigService`."""

from __future__ import annotations

from collections.abc import MutableMapping, Sequence
from typing import Any

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.models import (
    KeeperSyncConfig,
    KeeperSyncConfigUpdate,
    OrganizationCreate,
)
from docverse_server.services.keeper_sync_config import KeeperSyncConfigService
from docverse_server.storage.organization_store import OrganizationStore

_ORG = "ks-config-org"


async def _seed_org(session: AsyncSession, *, slug: str = _ORG) -> None:
    logger = structlog.get_logger("test")
    store = OrganizationStore(session=session, logger=logger)
    await store.create(
        OrganizationCreate(
            slug=slug,
            title="KS Config Org",
            base_domain=f"{slug}.example.com",
        )
    )


def _build_service(session: AsyncSession) -> KeeperSyncConfigService:
    logger = structlog.get_logger("test")
    return KeeperSyncConfigService(
        org_store=OrganizationStore(session=session, logger=logger),
        logger=logger,
    )


def _updated_event(
    captured: Sequence[MutableMapping[str, Any]],
) -> MutableMapping[str, Any]:
    events = [
        entry
        for entry in captured
        if entry["event"] == "Updated keeper_sync_config"
    ]
    assert len(events) == 1
    return events[0]


@pytest.mark.asyncio
async def test_put_logs_scope_field_sizes(db_session: AsyncSession) -> None:
    """The update event records the size of each of the four scope fields."""
    async with db_session.begin():
        await _seed_org(db_session)
    service = _build_service(db_session)
    config = KeeperSyncConfig(
        enabled=True,
        project_slugs=["sqr-112", "dmtn-001"],
        project_slug_patterns=[r"sqr-\d+"],
        exclude_project_slugs=["www", "test", "legacy"],
        exclude_project_slug_patterns=[r"test-.*", r"tmp-.*"],
    )

    with capture_logs() as captured:
        async with db_session.begin():
            await service.put(org_slug=_ORG, config=config)

    event = _updated_event(captured)
    assert event["project_slugs_count"] == 2
    assert event["project_slug_patterns_count"] == 1
    assert event["exclude_project_slugs_count"] == 3
    assert event["exclude_project_slug_patterns_count"] == 2


@pytest.mark.asyncio
async def test_put_logs_wildcard_project_slugs_as_none(
    db_session: AsyncSession,
) -> None:
    """``project_slugs="*"`` has no size; the event logs ``None``."""
    async with db_session.begin():
        await _seed_org(db_session)
    service = _build_service(db_session)

    with capture_logs() as captured:
        async with db_session.begin():
            await service.put(
                org_slug=_ORG,
                config=KeeperSyncConfig(enabled=True, project_slugs="*"),
            )

    event = _updated_event(captured)
    assert event["project_slugs_count"] is None


@pytest.mark.asyncio
async def test_patch_carries_new_scope_fields(
    db_session: AsyncSession,
) -> None:
    """A merge patch of one scope field leaves the other three alone."""
    async with db_session.begin():
        await _seed_org(db_session)
    service = _build_service(db_session)
    async with db_session.begin():
        await service.put(
            org_slug=_ORG,
            config=KeeperSyncConfig(
                enabled=True,
                project_slugs=["sqr-112"],
                project_slug_patterns=[r"sqr-\d+"],
                exclude_project_slugs=["www"],
                exclude_project_slug_patterns=[r"test-.*"],
            ),
        )

    async with db_session.begin():
        merged = await service.patch(
            org_slug=_ORG,
            update=KeeperSyncConfigUpdate(
                exclude_project_slugs=["www", "legacy"]
            ),
        )

    assert merged.exclude_project_slugs == ["www", "legacy"]
    assert merged.project_slugs == ["sqr-112"]
    assert merged.project_slug_patterns == [r"sqr-\d+"]
    assert merged.exclude_project_slug_patterns == [r"test-.*"]
