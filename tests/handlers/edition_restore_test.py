"""Tests for ``POST .../editions/{edition}/restore`` (PRD #830).

The admin endpoint that brings a soft-deleted edition back in place,
whatever its ``delete_reason`` — the only way back for an edition an
operator deleted, since neither keeper-sync nor a native build revives
a ``manual_delete``.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
import structlog
from httpx import AsyncClient
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency
from safir.metrics import MockEventPublisher
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    BuildCreate,
    EditionCreate,
    EditionKind,
    OrgRole,
    TrackingMode,
)
from docverse.models.queue_enums import JobKind
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.dependencies.context import context_dependency
from docverse_server.metrics import LifecycleAction, MetricsEditionKind
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from tests.conftest import seed_member, seed_org_with_admin
from tests.support.arq_testing import get_jobs_by_name

_ORG = "rs-org"
_PROJECT = "rs-proj"
_ADMIN = "testuser"
_SLUG = "DM-1"
_LTD_ID = 4242

_RESTORE_URL = (
    f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/editions/{{slug}}/restore"
)

_DELETE_REASONS = pytest.mark.parametrize(
    "reason",
    [TombstoneReason.lifecycle_delete, TombstoneReason.manual_delete],
)


@dataclass(frozen=True)
class _Seeded:
    """Ids of the soft-deleted edition a test restores."""

    org_id: int
    edition_id: int
    build_id: int | None
    history_id: int | None


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _setup(client: AsyncClient) -> None:
    """Create the org (with an admin) and the project."""
    await seed_org_with_admin(client, _ORG, _ADMIN)
    response = await client.post(
        f"/docverse/orgs/{_ORG}/projects",
        json={
            "slug": _PROJECT,
            "title": "Restore Project",
            "source_url": "https://example.com/example/rs",
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201


async def _seed_deleted_edition(
    db_session: AsyncSession,
    *,
    reason: TombstoneReason,
    with_build: bool = True,
    synced: bool = False,
) -> _Seeded:
    """Seed a draft edition serving a build, then soft-delete it.

    With ``synced`` the edition also gets a keeper-sync state row first,
    so the soft-delete stamps a tombstone on it as it would for an
    edition imported from LTD.
    """
    logger = _logger()
    async with db_session.begin():
        org = await OrganizationStore(
            session=db_session, logger=logger
        ).get_by_slug(_ORG)
        assert org is not None
        project = await ProjectStore(
            session=db_session, logger=logger
        ).get_by_slug(org_id=org.id, slug=_PROJECT)
        assert project is not None
        edition_store = EditionStore(session=db_session, logger=logger)
        edition = await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug=_SLUG,
                title="DM-1 draft",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "tickets/DM-1"},
            ),
        )
        build_id: int | None = None
        history_id: int | None = None
        if with_build:
            build = await BuildStore(session=db_session, logger=logger).create(
                project_id=project.id,
                data=BuildCreate(
                    git_ref="tickets/DM-1", content_hash="sha256:" + "a" * 64
                ),
                uploader=_ADMIN,
                project_slug=_PROJECT,
            )
            await edition_store.set_current_build(
                edition_id=edition.id,
                build_id=build.id,
                skip_date_guard=True,
            )
            history = await EditionBuildHistoryStore(
                session=db_session, logger=logger
            ).record(edition_id=edition.id, build_id=build.id)
            build_id = build.id
            history_id = history.id
        if synced:
            await KeeperSyncStateStore(
                session=db_session, logger=logger
            ).upsert(
                org_id=org.id,
                resource_type=ResourceType.edition,
                ltd_id=_LTD_ID,
                ltd_slug=_SLUG.lower(),
                docverse_id=edition.id,
            )
        assert await edition_store.soft_delete(
            org_id=org.id,
            project_id=project.id,
            slug=_SLUG,
            reason=reason,
        )
        await db_session.commit()
    return _Seeded(
        org_id=org.id,
        edition_id=edition.id,
        build_id=build_id,
        history_id=history_id,
    )


async def _read_edition_row(
    db_session: AsyncSession, edition_id: int
) -> SqlEdition:
    async with db_session.begin():
        row = (
            await db_session.execute(
                select(SqlEdition)
                .where(SqlEdition.id == edition_id)
                .execution_options(populate_existing=True)
            )
        ).scalar_one()
        await db_session.commit()
    return row


async def _settle_dashboard_build_jobs(db_session: AsyncSession) -> None:
    """Drive every ``dashboard_build`` row to ``completed``.

    Creating the project enqueued one; left in flight it would make the
    restore's own enqueue a dedup skip.
    """
    store = QueueJobStore(session=db_session, logger=_logger())
    async with db_session.begin():
        result = await db_session.execute(
            select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.dashboard_build.value
            )
        )
        for row in result.scalars().all():
            await store.start_if_queued(row.id)
            await store.complete(row.id)
        await db_session.commit()


def _mock_arq() -> MockArqQueue:
    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    return mock_arq


def _restore_events() -> list[object]:
    events = context_dependency._events
    assert events is not None
    publisher = events.edition_lifecycle
    assert isinstance(publisher, MockEventPublisher)
    return [
        e for e in publisher.published if e.action == LifecycleAction.restore
    ]


@pytest.mark.asyncio
@_DELETE_REASONS
async def test_restore_returns_the_live_edition(
    client: AsyncClient,
    db_session: AsyncSession,
    reason: TombstoneReason,
) -> None:
    """The edition comes back in place, with no deletion provenance.

    Same row, same slug; ``date_deleted`` and ``delete_reason`` are
    cleared, so the ordinary read finds it again.
    """
    await _setup(client)
    seeded = await _seed_deleted_edition(db_session, reason=reason)

    response = await client.post(
        _RESTORE_URL.format(slug=_SLUG),
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["slug"] == _SLUG
    assert body["kind"] == "draft"
    assert body["build_url"] is not None
    assert body["publish_status"] == "pending"
    row = await _read_edition_row(db_session, seeded.edition_id)
    assert row.date_deleted is None
    assert row.delete_reason is None
    fetched = await client.get(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/editions/{_SLUG}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert fetched.status_code == 200


@pytest.mark.asyncio
@_DELETE_REASONS
async def test_restore_republishes_the_current_build(
    client: AsyncClient,
    db_session: AsyncSession,
    reason: TombstoneReason,
) -> None:
    """The delete took the edition off the CDN; the restore puts it back.

    The ``publish_edition`` job names the build the edition still points
    at and the pair's existing history row: a restore is not a repoint,
    so it records no new history entry.
    """
    await _setup(client)
    seeded = await _seed_deleted_edition(db_session, reason=reason)

    response = await client.post(
        _RESTORE_URL.format(slug=_SLUG),
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 200
    publish_jobs = get_jobs_by_name(_mock_arq(), "publish_edition")
    assert len(publish_jobs) == 1
    payload = publish_jobs[0].kwargs["payload"]
    assert payload["edition_id"] == seeded.edition_id
    assert payload["edition_slug"] == _SLUG
    assert payload["build_id"] == seeded.build_id
    assert payload["history_id"] == seeded.history_id
    async with db_session.begin():
        history = await EditionBuildHistoryStore(
            session=db_session, logger=_logger()
        ).list_by_edition(seeded.edition_id)
        await db_session.commit()
    assert [entry.id for entry in history] == [seeded.history_id]


@pytest.mark.asyncio
@_DELETE_REASONS
async def test_restore_enqueues_a_dashboard_rebuild(
    client: AsyncClient,
    db_session: AsyncSession,
    reason: TombstoneReason,
) -> None:
    """The dashboard lists live editions, so it owes a rebuild."""
    await _setup(client)
    await _seed_deleted_edition(db_session, reason=reason)
    await _settle_dashboard_build_jobs(db_session)
    before = len(get_jobs_by_name(_mock_arq(), "dashboard_build"))

    response = await client.post(
        _RESTORE_URL.format(slug=_SLUG),
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 200
    after = len(get_jobs_by_name(_mock_arq(), "dashboard_build"))
    assert after == before + 1


@pytest.mark.asyncio
@_DELETE_REASONS
async def test_restore_publishes_a_restore_event(
    client: AsyncClient,
    db_session: AsyncSession,
    reason: TombstoneReason,
) -> None:
    """One ``edition_lifecycle`` ``restore`` event, tagged with the kind."""
    await _setup(client)
    await _seed_deleted_edition(db_session, reason=reason)

    response = await client.post(
        _RESTORE_URL.format(slug=_SLUG),
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 200
    events = context_dependency._events
    assert events is not None
    publisher = events.edition_lifecycle
    assert isinstance(publisher, MockEventPublisher)
    restores = [
        e for e in publisher.published if e.action == LifecycleAction.restore
    ]
    assert len(restores) == 1
    assert restores[0].organization == _ORG
    assert restores[0].project == _PROJECT
    assert restores[0].edition_kind == MetricsEditionKind.draft


@pytest.mark.asyncio
async def test_restore_clears_the_keeper_sync_tombstone(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """A once-synced edition leaves the tombstones listing.

    Left in place, the tombstone would make keeper-sync skip an edition
    that is live again.
    """
    await _setup(client)
    seeded = await _seed_deleted_edition(
        db_session, reason=TombstoneReason.manual_delete, synced=True
    )
    listed = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert len(listed.json()) == 1

    response = await client.post(
        _RESTORE_URL.format(slug=_SLUG),
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 200
    listed = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/tombstones",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert listed.status_code == 200
    assert listed.json() == []
    async with db_session.begin():
        state = await KeeperSyncStateStore(
            session=db_session, logger=_logger()
        ).get(
            org_id=seeded.org_id,
            resource_type=ResourceType.edition,
            ltd_id=_LTD_ID,
            include_tombstoned=True,
        )
        await db_session.commit()
    assert state is not None
    assert state.date_tombstoned is None
    assert state.tombstone_reason is None
    assert state.docverse_id == seeded.edition_id


@pytest.mark.asyncio
async def test_restore_unknown_edition_is_not_found(
    client: AsyncClient,
) -> None:
    await _setup(client)

    response = await client.post(
        _RESTORE_URL.format(slug="no-such-edition"),
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_restore_live_edition_is_not_found(
    client: AsyncClient,
) -> None:
    """The URL addresses only deleted editions, like a tombstone clear.

    Answering 200 would report a restore that never happened, with a
    publish and dashboard rebuild nobody owed.
    """
    await _setup(client)
    created = await client.post(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/editions",
        json={
            "slug": "live-ed",
            "title": "Live",
            "kind": "draft",
            "tracking_mode": "git_ref",
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert created.status_code == 201

    response = await client.post(
        _RESTORE_URL.format(slug="live-ed"),
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 404
    assert get_jobs_by_name(_mock_arq(), "publish_edition") == []
    assert _restore_events() == []


@pytest.mark.asyncio
async def test_restore_main_is_not_found(client: AsyncClient) -> None:
    """``__main`` cannot be deleted, so there is nothing to restore."""
    await _setup(client)

    response = await client.post(
        _RESTORE_URL.format(slug="__main"),
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_restore_requires_admin(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Admin-only, like the DELETE it undoes."""
    await _setup(client)
    seeded = await _seed_deleted_edition(
        db_session, reason=TombstoneReason.manual_delete
    )
    await seed_member(_ORG, "upload-user", OrgRole.uploader)

    response = await client.post(
        _RESTORE_URL.format(slug=_SLUG),
        headers={"X-Auth-Request-User": "upload-user"},
    )

    assert response.status_code == 403
    row = await _read_edition_row(db_session, seeded.edition_id)
    assert row.date_deleted is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("purged", [False, True])
async def test_restore_does_not_republish_a_deleted_build(
    client: AsyncClient,
    db_session: AsyncSession,
    *,
    purged: bool,
) -> None:
    """A current build that is gone revives the edition, unpublished.

    A soft-deleted build is not served and a purged one has no content
    left, so publishing either would point the CDN at nothing.
    """
    await _setup(client)
    seeded = await _seed_deleted_edition(
        db_session, reason=TombstoneReason.lifecycle_delete
    )
    assert seeded.build_id is not None
    async with db_session.begin():
        build_store = BuildStore(session=db_session, logger=_logger())
        assert await build_store.soft_delete(build_id=seeded.build_id)
        if purged:
            assert (
                await build_store.mark_purged(build_id=seeded.build_id)
                is not None
            )
        await db_session.commit()

    response = await client.post(
        _RESTORE_URL.format(slug=_SLUG),
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 200
    assert response.json()["publish_status"] != "pending"
    assert get_jobs_by_name(_mock_arq(), "publish_edition") == []
    row = await _read_edition_row(db_session, seeded.edition_id)
    assert row.date_deleted is None
    assert len(_restore_events()) == 1


@pytest.mark.asyncio
async def test_restore_without_a_build_does_not_publish(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """An edition that never served a build has nothing to publish."""
    await _setup(client)
    seeded = await _seed_deleted_edition(
        db_session, reason=TombstoneReason.manual_delete, with_build=False
    )

    response = await client.post(
        _RESTORE_URL.format(slug=_SLUG),
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 200
    assert response.json()["build_url"] is None
    assert get_jobs_by_name(_mock_arq(), "publish_edition") == []
    row = await _read_edition_row(db_session, seeded.edition_id)
    assert row.date_deleted is None
