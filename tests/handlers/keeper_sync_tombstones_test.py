"""Tests for ``GET`` / ``DELETE /orgs/{org}/keeper-sync/tombstones``.

Backs PRD #332's admin API for tombstone visibility and recovery.
"""

from __future__ import annotations

import pytest
import structlog
from docverse.client.models import (
    EditionCreate,
    EditionKind,
    OrgRole,
    ProjectCreate,
    TrackingMode,
)
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency

from docverse.services.keeper_sync_tombstone import KeeperSyncTombstoneService
from docverse.storage.edition_store import EditionStore
from docverse.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from tests.conftest import seed_member, seed_org_with_admin

_ADMIN = "admin-user"
_ORG = "ks-tomb-handler"


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


async def _setup(client: AsyncClient) -> None:
    await seed_org_with_admin(client, _ORG, _ADMIN)


async def _get_org_id() -> int:
    async for session in db_session_dependency():
        store = OrganizationStore(session=session, logger=_logger())
        org = await store.get_by_slug(_ORG)
        assert org is not None
        return org.id
    msg = "no session"
    raise AssertionError(msg)


async def _record_tombstone(
    *,
    org_id: int,
    resource_type: ResourceType,
    reason: TombstoneReason,
    ltd_id: int | None = None,
    ltd_slug: str | None = None,
    note: str | None = None,
) -> int:
    """Record a tombstone via the service and return the state row id."""
    async for session in db_session_dependency():
        async with session.begin():
            state_store = KeeperSyncStateStore(
                session=session, logger=_logger()
            )
            service = KeeperSyncTombstoneService(
                session=session,
                state_store=state_store,
                logger=_logger(),
            )
            state = await service.record(
                org_id=org_id,
                resource_type=resource_type,
                reason=reason,
                ltd_id=ltd_id,
                ltd_slug=ltd_slug,
                note=note,
            )
            await session.commit()
        return state.id
    msg = "no session"
    raise AssertionError(msg)


async def _seed_imported_edition(
    *,
    org_id: int,
    project_slug: str,
    edition_slug: str,
    ltd_id: int,
) -> tuple[int, int]:
    """Create a Docverse project + edition + matching state row."""
    async for session in db_session_dependency():
        async with session.begin():
            proj_store = ProjectStore(session=session, logger=_logger())
            edition_store = EditionStore(session=session, logger=_logger())
            state_store = KeeperSyncStateStore(
                session=session, logger=_logger()
            )
            project = await proj_store.create(
                org_id=org_id,
                data=ProjectCreate(
                    slug=project_slug,
                    title=project_slug.title(),
                    source_url=f"https://example.com/x/{project_slug}",
                ),
            )
            edition = await edition_store.create(
                project_id=project.id,
                data=EditionCreate(
                    slug=edition_slug,
                    title=edition_slug,
                    kind=EditionKind.draft,
                    tracking_mode=TrackingMode.git_ref,
                ),
            )
            await state_store.upsert(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_id,
                ltd_slug=edition_slug,
                docverse_id=edition.id,
            )
            await session.commit()
        return project.id, edition.id
    msg = "no session"
    raise AssertionError(msg)


async def _soft_delete_edition(
    *,
    org_id: int,
    project_id: int,
    slug: str,
) -> None:
    """Soft-delete an edition through the centralized chokepoint."""
    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            await edition_store.soft_delete(
                org_id=org_id,
                project_id=project_id,
                slug=slug,
                reason=TombstoneReason.manual_delete,
            )
            await session.commit()
        return
    msg = "no session"
    raise AssertionError(msg)


# ---------------------------------------------------------------------------
# Auth gating
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_tombstones_403_for_non_admin(client: AsyncClient) -> None:
    await _setup(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_tombstones_403_without_auth(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones",
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_tombstones_404_for_unknown_org(
    client: AsyncClient,
) -> None:
    response = await client.get(
        "/docverse/orgs/no-such-org/keeper-sync/tombstones",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_tombstone_403_for_non_admin(
    client: AsyncClient,
) -> None:
    await _setup(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.delete(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones/1",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# GET — listing & filtering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_tombstones_returns_empty_initially(
    client: AsyncClient,
) -> None:
    """An org with no tombstones returns ``[]`` with ``X-Total-Count: 0``."""
    await _setup(client)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    assert response.json() == []
    assert response.headers["X-Total-Count"] == "0"


@pytest.mark.asyncio
async def test_list_tombstones_returns_recorded_rows(
    client: AsyncClient,
) -> None:
    """A recorded tombstone appears in the listing with all expected fields."""
    await _setup(client)
    org_id = await _get_org_id()
    state_id = await _record_tombstone(
        org_id=org_id,
        resource_type=ResourceType.edition,
        reason=TombstoneReason.lifecycle_delete,
        ltd_id=4242,
        ltd_slug="aged-out",
        note="auto-aged",
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    entry = body[0]
    assert entry["state_id"] == state_id
    assert entry["resource_type"] == "edition"
    assert entry["ltd_id"] == 4242
    assert entry["ltd_slug"] == "aged-out"
    assert entry["tombstone_reason"] == "lifecycle_delete"
    assert entry["tombstone_note"] == "auto-aged"
    assert entry["date_tombstoned"] is not None
    assert entry["display_path"] == "aged-out"
    assert entry["self_url"].endswith(
        f"/orgs/{_ORG}/keeper-sync/tombstones/{state_id}"
    )


@pytest.mark.asyncio
async def test_list_tombstones_filters_by_resource_type(
    client: AsyncClient,
) -> None:
    """``?resource_type=edition`` excludes project rows."""
    await _setup(client)
    org_id = await _get_org_id()
    await _record_tombstone(
        org_id=org_id,
        resource_type=ResourceType.project,
        reason=TombstoneReason.manual_delete,
        ltd_slug="a-proj",
    )
    await _record_tombstone(
        org_id=org_id,
        resource_type=ResourceType.edition,
        reason=TombstoneReason.lifecycle_delete,
        ltd_id=99,
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones?resource_type=edition",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert [entry["resource_type"] for entry in body] == ["edition"]


@pytest.mark.asyncio
async def test_list_tombstones_filters_by_reason(
    client: AsyncClient,
) -> None:
    """``?tombstone_reason=manual_delete`` excludes other reasons."""
    await _setup(client)
    org_id = await _get_org_id()
    await _record_tombstone(
        org_id=org_id,
        resource_type=ResourceType.edition,
        reason=TombstoneReason.manual_delete,
        ltd_id=1,
    )
    await _record_tombstone(
        org_id=org_id,
        resource_type=ResourceType.edition,
        reason=TombstoneReason.lifecycle_delete,
        ltd_id=2,
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones"
        "?tombstone_reason=manual_delete",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert [entry["tombstone_reason"] for entry in body] == ["manual_delete"]
    assert [entry["ltd_id"] for entry in body] == [1]


@pytest.mark.asyncio
async def test_list_tombstones_pagination_link_header(
    client: AsyncClient,
) -> None:
    """``?limit=1`` page includes a Link header for the next page."""
    await _setup(client)
    org_id = await _get_org_id()
    for ltd_id in (10, 11, 12):
        await _record_tombstone(
            org_id=org_id,
            resource_type=ResourceType.edition,
            reason=TombstoneReason.lifecycle_delete,
            ltd_id=ltd_id,
        )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones?limit=1",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.headers["X-Total-Count"] == "3"
    assert 'rel="next"' in response.headers["Link"]


# ---------------------------------------------------------------------------
# DELETE — clear (with revive-on-clear)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_tombstone_returns_204(client: AsyncClient) -> None:
    """A successful clear returns 204 with no body."""
    await _setup(client)
    org_id = await _get_org_id()
    state_id = await _record_tombstone(
        org_id=org_id,
        resource_type=ResourceType.edition,
        reason=TombstoneReason.manual_delete,
        ltd_id=51,
    )

    response = await client.delete(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones/{state_id}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 204
    assert response.content == b""


@pytest.mark.asyncio
async def test_delete_tombstone_clears_state_row(client: AsyncClient) -> None:
    """After DELETE, the state row's tombstone fields are NULL."""
    await _setup(client)
    org_id = await _get_org_id()
    state_id = await _record_tombstone(
        org_id=org_id,
        resource_type=ResourceType.edition,
        reason=TombstoneReason.manual_delete,
        ltd_id=53,
        note="recover me",
    )

    response = await client.delete(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones/{state_id}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 204

    async for session in db_session_dependency():
        async with session.begin():
            state_store = KeeperSyncStateStore(
                session=session, logger=_logger()
            )
            state = await state_store.get_by_id_for_org(
                state_id=state_id, org_id=org_id
            )
    assert state is not None
    assert state.date_tombstoned is None
    assert state.tombstone_reason is None
    assert state.tombstone_note is None


@pytest.mark.asyncio
async def test_delete_tombstone_revives_soft_deleted_edition(
    client: AsyncClient,
) -> None:
    """A soft-deleted edition is revived after DELETE."""
    await _setup(client)
    org_id = await _get_org_id()
    project_id, edition_id = await _seed_imported_edition(
        org_id=org_id,
        project_slug="rev-proj",
        edition_slug="rev-ed",
        ltd_id=2000,
    )
    await _soft_delete_edition(
        org_id=org_id, project_id=project_id, slug="rev-ed"
    )
    # The chokepoint stamped the tombstone — find its state_id.
    async for session in db_session_dependency():
        async with session.begin():
            state_store = KeeperSyncStateStore(
                session=session, logger=_logger()
            )
            state = await state_store.get(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=2000,
                include_tombstoned=True,
            )
    assert state is not None
    assert state.date_tombstoned is not None
    state_id = state.id

    response = await client.delete(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones/{state_id}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 204

    # Edition's date_deleted is NULL — the slug-clash crash for a
    # subsequent ``sync_edition`` is no longer reachable on this row.
    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            revived = await edition_store.get_by_slug(
                project_id=project_id, slug="rev-ed"
            )
    assert revived is not None
    assert revived.id == edition_id


@pytest.mark.asyncio
async def test_delete_tombstone_404_for_unknown_state_id(
    client: AsyncClient,
) -> None:
    """A non-existent state_id returns 404."""
    await _setup(client)
    response = await client.delete(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones/999999",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_tombstone_404_for_other_org(
    client: AsyncClient,
) -> None:
    """A state_id from another org returns 404 (org-scoped, not silent)."""
    await _setup(client)
    other_org = "ks-tomb-other"
    await seed_org_with_admin(client, other_org, _ADMIN)

    async for session in db_session_dependency():
        store = OrganizationStore(session=session, logger=_logger())
        other = await store.get_by_slug(other_org)
        assert other is not None
        other_id = other.id
        break

    state_id = await _record_tombstone(
        org_id=other_id,
        resource_type=ResourceType.edition,
        reason=TombstoneReason.manual_delete,
        ltd_id=88,
    )

    response = await client.delete(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones/{state_id}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_tombstone_404_when_not_tombstoned(
    client: AsyncClient,
) -> None:
    """An untombstoned state row returns 404 from the DELETE endpoint."""
    await _setup(client)
    org_id = await _get_org_id()
    async for session in db_session_dependency():
        async with session.begin():
            store = KeeperSyncStateStore(session=session, logger=_logger())
            state = await store.upsert(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=400,
                ltd_slug="alive",
            )
            await session.commit()
        break

    response = await client.delete(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones/{state.id}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404
