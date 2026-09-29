"""Tests for :class:`~docverse_server.services.edition.EditionService`.

Focused on the lock order the edition PATCH shares with the rest of the
tree (PRD #634, review of PR #643): a PATCH that carries both an edition
metadata field *and* ``build`` writes two tables, and it has to reach
them in the one order ``ProjectStore.soft_delete`` cascades in —
projects, then editions, then builds.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

import pytest
import structlog
from fastapi import FastAPI
from safir.arq import MockArqQueue
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from docverse.models import (
    BuildCreate,
    EditionCreate,
    EditionKind,
    EditionUpdate,
    JobKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.models.queue_enums import PublishStatus
from docverse_server.config import Configuration
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.build import Build
from docverse_server.domain.edition import DEFAULT_EDITION_SLUG
from docverse_server.factory import Factory
from docverse_server.services.edition import EditionService
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.keeper_sync import TombstoneReason
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from tests.support.rowlocks import (
    LOCK_WAIT_TIMEOUT,
    backend_pid,
    record_statements,
    wait_until_blocked_or_finished,
)

_config = Configuration()


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _edition_service(db_session: AsyncSession) -> EditionService:
    factory = Factory(
        session=db_session,
        logger=_logger(),
        arq_queue=MockArqQueue(),
        default_queue_name=_config.arq_queue_name,
    )
    return factory.create_edition_service()


class _Seeded(NamedTuple):
    """What :func:`_seed` put in the database."""

    org_id: int
    project_id: int
    edition_id: int
    builds: list[Build]


async def _seed(db_session: AsyncSession, *, n_builds: int = 1) -> _Seeded:
    """Insert an org, a project, its ``__main`` edition, and builds.

    The builds are returned oldest first, so a test that wants a
    "current" build and something to move it to can unpack two.
    """
    logger = _logger()
    org = await OrganizationStore(session=db_session, logger=logger).create(
        OrganizationCreate(
            slug="es-org",
            title="ES Org",
            base_domain="es-org.example.com",
        )
    )
    project = await ProjectStore(session=db_session, logger=logger).create(
        org_id=org.id,
        data=ProjectCreate(
            slug="es-proj",
            title="ES Project",
            source_url="https://example.com/example/es",
        ),
    )
    edition = await EditionStore(
        session=db_session, logger=logger
    ).create_internal(
        project_id=project.id,
        slug=DEFAULT_EDITION_SLUG,
        title="Main",
        kind=EditionKind.main,
        tracking_mode=TrackingMode.git_ref,
        tracking_params={"git_ref": "main"},
    )
    build_store = BuildStore(session=db_session, logger=logger)
    builds = [
        await build_store.create(
            project_id=project.id,
            project_slug=project.slug,
            data=BuildCreate(
                git_ref="main", content_hash=f"sha256:{index:064x}"
            ),
            uploader="testuser",
        )
        for index in range(n_builds)
    ]
    return _Seeded(
        org_id=org.id,
        project_id=project.id,
        edition_id=edition.id,
        builds=builds,
    )


@pytest.mark.asyncio
async def test_build_override_off_default_takes_no_project_lock(
    app: FastAPI,
    db_session: AsyncSession,
) -> None:
    """A ``build`` override off ``__main`` reaches ``projects`` not at all.

    The project clock follows its default edition, so an override that
    lands anywhere else leaves the row alone — and now does not lock it
    either. The service resolved the edition to find its slug, so it
    can tell the store which case this is instead of leaving it to
    re-ask the database on every repoint. Pinned here rather than only
    in the store, because the saving is only real if the caller
    actually answers.
    """
    async with db_session.begin():
        seeded = await _seed(db_session)
        await EditionStore(session=db_session, logger=_logger()).create(
            project_id=seeded.project_id,
            data=EditionCreate(
                slug="v1",
                title="v1",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        await db_session.commit()
    (build,) = seeded.builds

    service = _edition_service(db_session)
    async with db_session.begin():
        with record_statements(db_session) as statements:
            write = await service.update(
                org_slug="es-org",
                project_slug="es-proj",
                slug="v1",
                data=EditionUpdate(build=serialize_base32_id(build.public_id)),
            )
        await db_session.commit()

    assert write.changed
    assert write.edition.current_build_id == build.id
    project_locks = [
        s
        for s in statements
        if "FROM projects" in s and "FOR NO KEY UPDATE" in s
    ]
    assert not project_locks, statements


@pytest.mark.asyncio
async def test_update_with_metadata_and_build_does_not_deadlock(
    app: FastAPI,
    db_session: AsyncSession,
    db_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PATCH carrying metadata *and* ``build`` never deadlocks a DELETE.

    ``EditionService.update`` writes two rows in two different tables,
    and a build-only PATCH emits no edition ``UPDATE`` at all — which is
    why every other test of this path passes whichever order the two are
    applied in. With the metadata update flushed first, the PATCH held
    the edition row and then asked for the project row, while a
    concurrent ``DELETE /orgs/{org}/projects/{project}`` held the
    project row and was cascading toward that same edition. PostgreSQL
    aborts one of them, and neither the DELETE handler nor the PATCH has
    anywhere to retry.

    Applying the build override first puts the project row ahead of the
    edition row on this path too, so the DELETE simply waits.
    """
    async with db_session.begin():
        seeded = await _seed(db_session)
        await db_session.commit()
    org_id = seeded.org_id
    edition_id = seeded.edition_id
    build_public_id = serialize_base32_id(seeded.builds[0].public_id)

    # Park the PATCH immediately after its metadata write, the point at
    # which the old order was already holding the edition row.
    at_metadata = asyncio.Event()
    release_metadata = asyncio.Event()
    update_edition = EditionStore.update

    async def paused_update(
        self: EditionStore, **kwargs: object
    ) -> object | None:
        result = await update_edition(self, **kwargs)  # type: ignore[arg-type]
        at_metadata.set()
        await release_metadata.wait()
        return result

    monkeypatch.setattr(EditionStore, "update", paused_update)

    async with (
        db_session_factory() as patch_session,
        db_session_factory() as delete_session,
        db_session_factory() as probe,
    ):
        delete_pid = await backend_pid(delete_session)

        async def run_patch() -> None:
            service = _edition_service(patch_session)
            await service.update(
                org_slug="es-org",
                project_slug="es-proj",
                slug=DEFAULT_EDITION_SLUG,
                data=EditionUpdate(title="Renamed", build=build_public_id),
            )
            await patch_session.commit()

        async def run_delete() -> None:
            store = ProjectStore(session=delete_session, logger=_logger())
            await store.soft_delete(
                org_id=org_id,
                slug="es-proj",
                reason=TombstoneReason.manual_delete,
            )
            await delete_session.commit()

        patching = asyncio.ensure_future(run_patch())
        deleting: asyncio.Task[None] | None = None
        parked = False
        try:
            await asyncio.wait_for(
                at_metadata.wait(), timeout=LOCK_WAIT_TIMEOUT
            )
            deleting = asyncio.ensure_future(run_delete())
            parked = await wait_until_blocked_or_finished(
                probe, pid=delete_pid, task=deleting
            )
            release_metadata.set()
            await patching
            await deleting
        finally:
            release_metadata.set()
            for task in (patching, deleting):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            await patch_session.rollback()
            await delete_session.rollback()

    # The DELETE waited on the project row the PATCH took first.
    assert parked

    async with db_session_factory() as reader:
        row = (
            await reader.execute(
                select(SqlEdition).where(SqlEdition.id == edition_id)
            )
        ).scalar_one()
        # Both sides landed: the PATCH's title and build, then the
        # DELETE's cascade over the top.
        assert row.title == "Renamed"
        assert row.current_build_id is not None
        assert row.date_deleted is not None


@pytest.mark.asyncio
async def test_same_build_override_decides_under_the_row_lock(
    app: FastAPI,
    db_session: AsyncSession,
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A same-build override answers on the state it holds a lock over.

    "The edition already serves this build" is only true for as long as
    nothing else repoints it, and the service's own read of the edition
    happens outside the row lock the repoint takes. Two operators
    reacting to the same incident — one rolling back to A, one naming
    the B the edition is serving — could therefore both be told their
    postcondition held, while the edition ended up on A and the B
    caller's 200 named a build it no longer served.

    Here the override arrives while a repoint onto A is still in
    flight. It has to park on the rows that repoint holds and answer on
    what it finds afterwards: B is no longer current, so this is a real
    repoint, with the history row and the publish that go with one.
    """
    async with db_session.begin():
        seeded = await _seed(db_session, n_builds=2)
        await db_session.commit()
    build_a, build_b = seeded.builds
    edition_id = seeded.edition_id

    # The edition starts out serving B, the build the override names.
    async with db_session.begin():
        await EditionStore(
            session=db_session, logger=_logger()
        ).set_current_build(
            edition_id=edition_id, build_id=build_b.id, skip_date_guard=True
        )
        await db_session.commit()

    async with (
        db_session_factory() as repoint_session,
        db_session_factory() as patch_session,
        db_session_factory() as probe,
    ):
        patch_pid = await backend_pid(patch_session)

        # The other operator's rollback onto A, uncommitted: it holds
        # the project and edition rows, and READ COMMITTED still shows
        # everyone else the edition on B.
        await EditionStore(
            session=repoint_session, logger=_logger()
        ).set_current_build(
            edition_id=edition_id, build_id=build_a.id, skip_date_guard=True
        )

        async def run_patch() -> None:
            service = _edition_service(patch_session)
            await service.update(
                org_slug="es-org",
                project_slug="es-proj",
                slug=DEFAULT_EDITION_SLUG,
                data=EditionUpdate(
                    build=serialize_base32_id(build_b.public_id)
                ),
            )
            await patch_session.commit()

        patching = asyncio.ensure_future(run_patch())
        parked = False
        try:
            parked = await wait_until_blocked_or_finished(
                probe, pid=patch_pid, task=patching
            )
            await repoint_session.commit()
            await patching
        finally:
            if not patching.done():
                patching.cancel()
                with suppress(asyncio.CancelledError):
                    await patching
            await repoint_session.rollback()
            await patch_session.rollback()

    # The override waited rather than deciding on its own stale read.
    assert parked

    async with db_session_factory() as reader:
        logger = _logger()
        edition = await EditionStore(session=reader, logger=logger).get_by_id(
            edition_id
        )
        assert edition is not None
        # Having waited, it found A in place and really did repoint.
        assert edition.current_build_id == build_b.id
        history = await EditionBuildHistoryStore(
            session=reader, logger=logger
        ).list_by_edition(edition_id)
        assert [entry.build_id for entry in history] == [build_b.id]


@pytest.mark.asyncio
async def test_build_only_patch_skips_the_metadata_write(
    app: FastAPI,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PATCH carrying only ``build`` never reaches the metadata write.

    There is nothing for that write to write — the payload's only field
    was consumed by the override — but it is not free: it re-selects
    the edition, flushes, refreshes, and re-reads it through the
    current-build join, all while the transaction holds the project,
    edition, and build rows. The override already returned the row this
    request produced, so the caller gets that instead.
    """
    async with db_session.begin():
        seeded = await _seed(db_session)
        await db_session.commit()
    build = seeded.builds[0]

    calls: list[object] = []
    store_update = EditionStore.update

    async def counted_update(
        self: EditionStore, **kwargs: object
    ) -> object | None:
        calls.append(kwargs)
        return await store_update(self, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(EditionStore, "update", counted_update)

    async with db_session.begin():
        service = _edition_service(db_session)
        written = await service.update(
            org_slug="es-org",
            project_slug="es-proj",
            slug=DEFAULT_EDITION_SLUG,
            data=EditionUpdate(build=serialize_base32_id(build.public_id)),
        )
        await db_session.commit()

    assert calls == []
    assert written.edition.current_build_id == build.id


@pytest.mark.asyncio
async def test_patch_with_build_and_metadata_applies_both(
    app: FastAPI,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A payload carrying both still gets both, in that order.

    The metadata write is skipped only when the payload has nothing
    left for it, so a ``build`` alongside a metadata field runs both
    arms — the override first, which is what keeps this path's locks in
    the projects → editions order.
    """
    async with db_session.begin():
        seeded = await _seed(db_session)
        await db_session.commit()
    build = seeded.builds[0]

    calls: list[object] = []
    store_update = EditionStore.update

    async def counted_update(
        self: EditionStore, **kwargs: object
    ) -> object | None:
        calls.append(kwargs)
        return await store_update(self, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(EditionStore, "update", counted_update)

    async with db_session.begin():
        service = _edition_service(db_session)
        written = await service.update(
            org_slug="es-org",
            project_slug="es-proj",
            slug=DEFAULT_EDITION_SLUG,
            data=EditionUpdate(
                title="Renamed", build=serialize_base32_id(build.public_id)
            ),
        )
        await db_session.commit()

    assert len(calls) == 1
    assert written.edition.title == "Renamed"
    assert written.edition.current_build_id == build.id


async def _date_builds(db_session: AsyncSession, builds: list[Build]) -> None:
    """Space ``builds`` a day apart, oldest first, as their order says.

    Builds seeded in one transaction share its ``now()``, and the
    stale-build guard refuses a build no newer than the one served.
    """
    base = datetime(2026, 1, 1, tzinfo=UTC)
    for index, build in enumerate(builds):
        await db_session.execute(
            update(SqlBuild)
            .where(SqlBuild.id == build.id)
            .values(date_created=base + timedelta(days=index))
        )


async def _seed_serving_oldest(db_session: AsyncSession) -> _Seeded:
    """Seed two dated builds with ``__main`` serving the older one."""
    async with db_session.begin():
        seeded = await _seed(db_session, n_builds=2)
        await _date_builds(db_session, seeded.builds)
        await EditionStore(
            session=db_session, logger=_logger()
        ).set_current_build(
            edition_id=seeded.edition_id, build_id=seeded.builds[0].id
        )
        await db_session.commit()
    return seeded


@pytest.mark.asyncio
async def test_advance_to_build_repoints_and_queues_the_publish(
    app: FastAPI,
    db_session: AsyncSession,
) -> None:
    """A newer build is served, recorded, marked pending, and published.

    The same sequence an operator's override runs — history row,
    ``publish_status`` flip, ``publish_edition`` job naming that row —
    but reached through the stale-build guard rather than around it.
    """
    seeded = await _seed_serving_oldest(db_session)
    newer = seeded.builds[1]
    store = EditionStore(session=db_session, logger=_logger())

    factory = Factory(
        session=db_session,
        logger=_logger(),
        arq_queue=MockArqQueue(),
        default_queue_name=_config.arq_queue_name,
    )
    async with db_session.begin():
        edition = await store.get_by_id(seeded.edition_id)
        assert edition is not None
        job = await factory.create_edition_service().advance_to_build(
            org_id=seeded.org_id,
            project_slug="es-proj",
            edition=edition,
            build=newer,
        )
        await db_session.commit()

    assert job is not None
    assert job.kind is JobKind.publish_edition
    assert job.edition_id == seeded.edition_id
    assert job.build_id == newer.id
    async with db_session.begin():
        current = await store.get_by_id(seeded.edition_id)
        assert current is not None
        assert current.current_build_id == newer.id
        assert current.publish_status is PublishStatus.pending
        history = await EditionBuildHistoryStore(
            session=db_session, logger=_logger()
        ).list_by_edition(seeded.edition_id)
    assert history[0].build_id == newer.id
    assert history[0].publish_status is PublishStatus.pending
    [pending] = factory.queue_dispatcher.pending
    assert pending.job_type == "publish_edition"
    assert pending.payload["build_id"] == newer.id
    assert pending.payload["history_id"] == history[0].id
    assert "trigger" not in pending.payload


@pytest.mark.asyncio
async def test_advance_to_build_refuses_an_older_build(
    app: FastAPI,
    db_session: AsyncSession,
) -> None:
    """A build no newer than the one served is refused, and nothing moves.

    Unlike the override, this path keeps the stale-build guard, so its
    answer to an older build is ``None`` with no history row, no
    ``publish_status`` flip, and no job.
    """
    seeded = await _seed_serving_oldest(db_session)
    older, newer = seeded.builds
    store = EditionStore(session=db_session, logger=_logger())
    async with db_session.begin():
        await store.set_current_build(
            edition_id=seeded.edition_id, build_id=newer.id
        )
        await db_session.commit()

    factory = Factory(
        session=db_session,
        logger=_logger(),
        arq_queue=MockArqQueue(),
        default_queue_name=_config.arq_queue_name,
    )
    async with db_session.begin():
        edition = await store.get_by_id(seeded.edition_id)
        assert edition is not None
        job = await factory.create_edition_service().advance_to_build(
            org_id=seeded.org_id,
            project_slug="es-proj",
            edition=edition,
            build=older,
        )
        await db_session.commit()

    assert job is None
    assert factory.queue_dispatcher.pending == ()
    async with db_session.begin():
        current = await store.get_by_id(seeded.edition_id)
        assert current is not None
        assert current.current_build_id == newer.id
        assert current.publish_status is None
        history = await EditionBuildHistoryStore(
            session=db_session, logger=_logger()
        ).list_by_edition(seeded.edition_id)
    assert history == []
