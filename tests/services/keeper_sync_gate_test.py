"""Tests for the shared keeper-sync eligibility gate.

The gate is the org lookup → ``enabled`` check → scope check that every
per-project keeper-sync endpoint runs before it touches anything else.
It used to be copy-pasted into each service; these tests pin the one
copy, including the exact 404 messages the docs treat as a contract.
"""

from __future__ import annotations

from typing import Literal

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import KeeperSyncConfig, OrganizationCreate
from docverse_server.exceptions import ConflictError, NotFoundError
from docverse_server.services.keeper_sync_gate import (
    require_org,
    require_sync_eligible,
    require_sync_enabled,
)
from docverse_server.storage.organization_store import OrganizationStore

_ORG = "ks-org"


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(
    db_session: AsyncSession,
    *,
    enabled: bool = True,
    project_slugs: list[str] | Literal["*"] = "*",
    exclude_project_slugs: list[str] | None = None,
) -> OrganizationStore:
    """Create ``ks-org`` with a keeper-sync config and return its store."""
    store = OrganizationStore(session=db_session, logger=_logger())
    org = await store.create(
        OrganizationCreate(
            slug=_ORG,
            title="KS Org",
            base_domain="ks.example.com",
        )
    )
    await store.update_keeper_sync_config(
        slug=org.slug,
        config=KeeperSyncConfig(
            enabled=enabled,
            project_slugs=project_slugs,
            exclude_project_slugs=exclude_project_slugs or [],
        ),
    )
    return store


@pytest.mark.asyncio
async def test_require_sync_eligible_returns_org_and_config(
    app: None, db_session: AsyncSession
) -> None:
    """The happy path hands back both the org row and its config."""
    async with db_session.begin():
        store = await _seed_org(db_session, project_slugs=["pipelines"])
        org, config = await require_sync_eligible(
            store, org_slug=_ORG, ltd_slug="pipelines"
        )
    assert org.slug == _ORG
    assert config.enabled is True


@pytest.mark.asyncio
async def test_require_sync_eligible_404s_for_missing_org(
    app: None, db_session: AsyncSession
) -> None:
    """An org that does not exist is a 404 naming the slug."""
    async with db_session.begin():
        store = OrganizationStore(session=db_session, logger=_logger())
        with pytest.raises(NotFoundError) as excinfo:
            await require_sync_eligible(
                store, org_slug="nope", ltd_slug="pipelines"
            )
    assert str(excinfo.value) == "Organization 'nope' not found"


@pytest.mark.asyncio
async def test_require_sync_eligible_404s_when_sync_disabled(
    app: None, db_session: AsyncSession
) -> None:
    """Sync disabled on the org is a 404, not a 409."""
    async with db_session.begin():
        store = await _seed_org(db_session, enabled=False)
        with pytest.raises(NotFoundError) as excinfo:
            await require_sync_eligible(
                store, org_slug=_ORG, ltd_slug="pipelines"
            )
    assert str(excinfo.value) == (
        f"LTD Keeper sync is not enabled for organization {_ORG!r}"
    )


@pytest.mark.asyncio
async def test_require_sync_eligible_404s_for_out_of_scope_slug(
    app: None, db_session: AsyncSession
) -> None:
    """An out-of-scope slug carries the message the docs quote.

    ``docs/keeper-sync-scope.md`` promises operators that a project
    which has fallen out of scope answers 404 with a message reading
    ``is not in the keeper-sync scope``, so the wording is part of the
    contract, not an implementation detail.
    """
    async with db_session.begin():
        store = await _seed_org(db_session, exclude_project_slugs=["retired"])
        with pytest.raises(NotFoundError) as excinfo:
            await require_sync_eligible(
                store, org_slug=_ORG, ltd_slug="retired"
            )
    assert str(excinfo.value) == (
        f"LTD slug 'retired' is not in the keeper-sync scope"
        f" for organization {_ORG!r}"
    )


@pytest.mark.asyncio
async def test_require_sync_enabled_ignores_the_scope(
    app: None, db_session: AsyncSession
) -> None:
    """The two-clause gate admits a slug-less caller whatever the scope.

    ``list_project_statuses`` and ``start_run`` address the whole org,
    so they check only that the org exists and that sync is on — an
    empty scope is not their concern.
    """
    async with db_session.begin():
        store = await _seed_org(db_session, project_slugs=[])
        org, config = await require_sync_enabled(store, org_slug=_ORG)
    assert org.slug == _ORG
    assert config.project_slugs == []


@pytest.mark.asyncio
async def test_require_sync_enabled_can_raise_conflict_when_disabled(
    app: None, db_session: AsyncSession
) -> None:
    """``start_run`` keeps its 409 for the disabled case.

    Launching a backfill against a disabled org is a conflict with the
    org's state rather than a missing resource, so that one caller
    passes its own exception type into the shared gate.
    """
    async with db_session.begin():
        store = await _seed_org(db_session, enabled=False)
        with pytest.raises(ConflictError) as excinfo:
            await require_sync_enabled(
                store, org_slug=_ORG, disabled_error=ConflictError
            )
    assert str(excinfo.value) == (
        f"LTD Keeper sync is not enabled for organization {_ORG!r}"
    )


@pytest.mark.asyncio
async def test_require_org_404s_for_missing_org(
    app: None, db_session: AsyncSession
) -> None:
    """The org-only gate is the same lookup without the config checks."""
    async with db_session.begin():
        store = await _seed_org(db_session, enabled=False)
        org = await require_org(store, org_slug=_ORG)
        assert org.slug == _ORG
        with pytest.raises(NotFoundError) as excinfo:
            await require_org(store, org_slug="nope")
    assert str(excinfo.value) == "Organization 'nope' not found"
