"""Tests for :class:`~docverse_server.services.build.BuildService`.

Focused on the terminal transitions introduced by the stranded-build
work (PRD #577 / DM-56012): ``supersede``, which the ``build_processing``
stale-skip path uses to stop leaving skipped builds stranded in
``processing``; ``cancel``, which the DELETE handler and the worker's
deleted-self guard both call — so it has to be safe to call twice; and
``soft_delete``, which cancels a build that had not finished and leaves
an already-terminal one with the status it earned.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress

import pytest
import structlog
from safir.arq import MockArqQueue
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from docverse.models import (
    BuildCreate,
    BuildStatus,
    OrganizationCreate,
    ProjectCreate,
)
from docverse_server.config import Configuration
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.build import Build
from docverse_server.exceptions import InvalidBuildStateError
from docverse_server.factory import Factory
from docverse_server.services.build import BuildService
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from tests.support.rowlocks import backend_pid, wait_until_blocked_on_lock

_HASH = "sha256:" + "b" * 64
_config = Configuration()


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _build_service(db_session: AsyncSession) -> BuildService:
    factory = Factory(
        session=db_session,
        logger=_logger(),
        arq_queue=MockArqQueue(),
        default_queue_name=_config.arq_queue_name,
    )
    return factory.create_build_service()


async def _seed_build(
    db_session: AsyncSession, *, status: BuildStatus
) -> Build:
    """Insert an org/project/build, stepping the build to ``status``."""
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    project_store = ProjectStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)

    org = await org_store.create(
        OrganizationCreate(
            slug="bs-org",
            title="BS Org",
            base_domain="bs-org.example.com",
        )
    )
    project = await project_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug="bs-proj",
            title="BS Project",
            source_url="https://example.com/example/bs",
        ),
    )
    build = await build_store.create(
        project_id=project.id,
        project_slug=project.slug,
        data=BuildCreate(git_ref="main", content_hash=_HASH),
        uploader="testuser",
    )
    if status is not BuildStatus.pending:
        build = await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
    if status not in (BuildStatus.pending, BuildStatus.processing):
        build = await build_store.transition_status(
            build_id=build.id, new_status=status
        )
    return build


@pytest.mark.asyncio
async def test_supersede_marks_processing_build_superseded(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A build a newer one took over lands on ``superseded``."""
    async with db_session.begin():
        build = await _seed_build(db_session, status=BuildStatus.processing)
        service = _build_service(db_session)
        superseded = await service.supersede(
            build_id=build.id, org_slug="bs-org", project_slug="bs-proj"
        )
        await db_session.commit()

    assert superseded.status == BuildStatus.superseded
    assert superseded.date_completed is not None


@pytest.mark.asyncio
async def test_cancel_marks_pending_build_cancelled(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A build deleted before upload lands on ``cancelled``."""
    async with db_session.begin():
        build = await _seed_build(db_session, status=BuildStatus.pending)
        service = _build_service(db_session)
        cancelled = await service.cancel(
            build_id=build.id, org_slug="bs-org", project_slug="bs-proj"
        )
        await db_session.commit()

    assert cancelled.status == BuildStatus.cancelled
    assert cancelled.date_completed is not None


@pytest.mark.asyncio
async def test_cancel_is_idempotent(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Cancelling an already-cancelled build returns the row unchanged.

    Two independent paths cancel a build — the DELETE handler and the
    worker's deleted-self guard — and either may run second. The second
    caller must not raise ``InvalidBuildStateError`` for a row that is
    already in exactly the state it was asking for.
    """
    async with db_session.begin():
        build = await _seed_build(db_session, status=BuildStatus.pending)
        service = _build_service(db_session)
        first = await service.cancel(build_id=build.id)
        second = await service.cancel(build_id=build.id)
        await db_session.commit()

    assert second.status == BuildStatus.cancelled
    assert second.date_completed == first.date_completed


@pytest.mark.asyncio
async def test_cancel_rejects_other_terminal_statuses(
    app: None,
    db_session: AsyncSession,
) -> None:
    """The no-op is scoped to ``cancelled``, not to terminality.

    A ``completed`` build keeps its status when it is deleted, and the
    store's transition table is what enforces that — the service must
    not swallow the error and pretend the build was cancelled.
    """
    async with db_session.begin():
        build = await _seed_build(db_session, status=BuildStatus.completed)
        service = _build_service(db_session)
        with pytest.raises(InvalidBuildStateError):
            await service.cancel(build_id=build.id)


@pytest.mark.parametrize(
    "status", [BuildStatus.pending, BuildStatus.processing]
)
@pytest.mark.asyncio
async def test_soft_delete_cancels_non_terminal_build(
    app: None,
    db_session: AsyncSession,
    status: BuildStatus,
) -> None:
    """Deleting an unfinished build also cancels it.

    ``processing`` has to keep meaning "a worker is on it", so a build
    deleted before it finished must not be left claiming to be in
    flight. The cancel and the soft-delete land in the handler's single
    transaction, so a reader never sees a deleted row still ``pending``
    or ``processing``.
    """
    async with db_session.begin():
        build = await _seed_build(db_session, status=status)
        service = _build_service(db_session)
        await service.soft_delete(
            org_slug="bs-org",
            project_slug="bs-proj",
            build_id=serialize_base32_id(build.public_id),
        )
        await db_session.commit()

    async with db_session.begin():
        store = BuildStore(session=db_session, logger=_logger())
        deleted = await store.get_by_id(build.id)
        assert deleted is not None
        assert deleted.status == BuildStatus.cancelled
        assert deleted.date_deleted is not None
        assert deleted.date_completed is not None


@pytest.mark.parametrize(
    "status",
    [BuildStatus.completed, BuildStatus.failed, BuildStatus.superseded],
)
@pytest.mark.asyncio
async def test_soft_delete_keeps_terminal_status(
    app: None,
    db_session: AsyncSession,
    status: BuildStatus,
) -> None:
    """A finished build keeps the status it earned when it is deleted."""
    async with db_session.begin():
        build = await _seed_build(db_session, status=status)
        service = _build_service(db_session)
        await service.soft_delete(
            org_slug="bs-org",
            project_slug="bs-proj",
            build_id=serialize_base32_id(build.public_id),
        )
        await db_session.commit()

    async with db_session.begin():
        store = BuildStore(session=db_session, logger=_logger())
        deleted = await store.get_by_id(build.id)
        assert deleted is not None
        assert deleted.status == status
        assert deleted.date_deleted is not None


@pytest.mark.parametrize(
    "status", [BuildStatus.pending, BuildStatus.processing]
)
@pytest.mark.asyncio
async def test_cancel_if_unfinished_cancels_live_build(
    app: None,
    db_session: AsyncSession,
    status: BuildStatus,
) -> None:
    """An unfinished build is cancelled, whichever live status it is in."""
    async with db_session.begin():
        build = await _seed_build(db_session, status=status)
        service = _build_service(db_session)
        cancelled = await service.cancel_if_unfinished(build_id=build.id)
        await db_session.commit()

    assert cancelled is not None
    assert cancelled.status == BuildStatus.cancelled
    assert cancelled.date_completed is not None


@pytest.mark.parametrize(
    "status",
    [
        BuildStatus.completed,
        BuildStatus.failed,
        BuildStatus.superseded,
        BuildStatus.cancelled,
    ],
)
@pytest.mark.asyncio
async def test_cancel_if_unfinished_leaves_terminal_build(
    app: None,
    db_session: AsyncSession,
    status: BuildStatus,
) -> None:
    """A finished build is left alone rather than raising.

    The lifecycle reaper calls this against rows it loaded earlier in
    its run, so the status it believes a build has may already be out of
    date. Unlike :meth:`BuildService.cancel`, which is the DELETE
    handler's deliberate "this build must end up cancelled", this is a
    best-effort retirement and reports the no-op with ``None``.
    """
    async with db_session.begin():
        build = await _seed_build(db_session, status=status)
        service = _build_service(db_session)
        assert await service.cancel_if_unfinished(build_id=build.id) is None

    async with db_session.begin():
        store = BuildStore(session=db_session, logger=_logger())
        row = await store.get_by_id(build.id)
        assert row is not None
        assert row.status == status


@pytest.mark.asyncio
async def test_fail_if_unfinished_fails_live_build(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A build a worker gave up on still lands on ``failed``."""
    async with db_session.begin():
        build = await _seed_build(db_session, status=BuildStatus.processing)
        service = _build_service(db_session)
        failed = await service.fail_if_unfinished(build_id=build.id)
        await db_session.commit()

    assert failed is not None
    assert failed.status == BuildStatus.failed


@pytest.mark.asyncio
async def test_fail_if_unfinished_leaves_cancelled_build(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A build cancelled out from under the worker keeps that status.

    The worker's error path runs in the same transaction that has to
    mark the queue job failed. Raising ``InvalidBuildStateError`` over a
    row a DELETE already cancelled would abort that transaction and
    strand the job ``in_progress`` — the failure mode PRD #577 exists to
    remove — so the retirement is skipped instead.
    """
    async with db_session.begin():
        build = await _seed_build(db_session, status=BuildStatus.processing)
        service = _build_service(db_session)
        await service.cancel(build_id=build.id)
        assert await service.fail_if_unfinished(build_id=build.id) is None
        await db_session.commit()

    async with db_session.begin():
        store = BuildStore(session=db_session, logger=_logger())
        row = await store.get_by_id(build.id)
        assert row is not None
        assert row.status == BuildStatus.cancelled


@pytest.mark.asyncio
async def test_supersede_if_unfinished_supersedes_processing_build(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A live build a newer one took over still lands on ``superseded``."""
    async with db_session.begin():
        build = await _seed_build(db_session, status=BuildStatus.processing)
        service = _build_service(db_session)
        superseded = await service.supersede_if_unfinished(build_id=build.id)
        await db_session.commit()

    assert superseded is not None
    assert superseded.status == BuildStatus.superseded
    assert superseded.date_completed is not None


@pytest.mark.parametrize(
    "status",
    [
        BuildStatus.completed,
        BuildStatus.failed,
        BuildStatus.superseded,
        BuildStatus.cancelled,
    ],
)
@pytest.mark.asyncio
async def test_supersede_if_unfinished_leaves_terminal_build(
    app: None,
    db_session: AsyncSession,
    status: BuildStatus,
) -> None:
    """A finished build keeps its status instead of raising.

    The ``build_processing`` stale guard reads the latest live build id
    for the ref outside the transaction that skips this one, so a DELETE
    or a lifecycle reap can retire the row in between. ``supersede``
    would then ask for an edge out of a terminal status, and
    :exc:`InvalidBuildStateError` would escape the worker (#590).
    """
    async with db_session.begin():
        build = await _seed_build(db_session, status=status)
        service = _build_service(db_session)
        assert await service.supersede_if_unfinished(build_id=build.id) is None

    async with db_session.begin():
        store = BuildStore(session=db_session, logger=_logger())
        row = await store.get_by_id(build.id)
        assert row is not None
        assert row.status == status


async def _seed_processing_build(db_session: AsyncSession) -> Build:
    """Commit one ``processing`` build for the racing tests to fight over."""
    async with db_session.begin():
        build = await _seed_build(db_session, status=BuildStatus.processing)
        await db_session.commit()
    return build


@pytest.mark.asyncio
async def test_soft_delete_stands_down_for_a_concurrent_completion(
    app: None,
    db_session: AsyncSession,
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A DELETE that loses the race sees ``completed`` and only deletes.

    ``cancel_if_unfinished`` used to decide from an unlocked snapshot, so
    a DELETE reaching a ``processing`` build while a worker was
    committing its completion still believed the build was unfinished.
    It then asked for ``processing -> cancelled`` against a row that had
    become ``completed``, and the store — which does hold the row lock —
    raised :exc:`InvalidBuildStateError` straight out of the DELETE
    handler, failing a request that has a perfectly good answer (review
    of PR #583, finding f1).

    Reading the row under the same lock the write needs makes the
    decision and the write one step: the DELETE blocks, sees the status
    the worker committed, leaves it alone, and stamps ``date_deleted``.
    """
    build = await _seed_processing_build(db_session)

    async with (
        db_session_factory() as worker_session,
        db_session_factory() as delete_session,
        db_session_factory() as probe,
    ):
        delete_pid = await backend_pid(delete_session)
        worker_store = BuildStore(session=worker_session, logger=_logger())
        # The worker completes but has not committed, so it holds the row.
        await worker_store.transition_status(
            build_id=build.id, new_status=BuildStatus.completed
        )

        async def run_delete() -> None:
            service = _build_service(delete_session)
            await service.soft_delete(
                org_slug="bs-org",
                project_slug="bs-proj",
                build_id=serialize_base32_id(build.public_id),
            )
            await delete_session.commit()

        deleting = asyncio.ensure_future(run_delete())
        try:
            await wait_until_blocked_on_lock(probe, pid=delete_pid)
            await worker_session.commit()
            await deleting
        finally:
            if not deleting.done():
                deleting.cancel()
                with suppress(asyncio.CancelledError):
                    await deleting
            await delete_session.rollback()

    async with db_session_factory() as reader:
        row = await BuildStore(session=reader, logger=_logger()).get_by_id(
            build.id
        )
        assert row is not None
        # The worker's terminal status stands; the DELETE only deleted.
        assert row.status == BuildStatus.completed
        assert row.date_deleted is not None


@pytest.mark.asyncio
async def test_cancel_if_unfinished_holds_the_row_against_a_completion(
    app: None,
    db_session: AsyncSession,
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The other ordering: the DELETE wins and the worker stands down.

    Here the DELETE's ``cancel_if_unfinished`` gets the row first and
    holds it to commit, so the worker's completion blocks rather than
    reading a stale ``processing``. When it wakes it sees ``cancelled``,
    which is what makes the worker's mid-upload re-read a real guard:
    it takes the ``_close_out_retired_build`` path instead of writing
    ``completed`` over the operator's deletion.
    """
    build = await _seed_processing_build(db_session)

    async with (
        db_session_factory() as worker_session,
        db_session_factory() as delete_session,
        db_session_factory() as probe,
    ):
        worker_pid = await backend_pid(worker_session)
        service = _build_service(delete_session)
        cancelled = await service.cancel_if_unfinished(build_id=build.id)
        assert cancelled is not None
        assert await BuildStore(
            session=delete_session, logger=_logger()
        ).soft_delete(build_id=build.id)

        worker_store = BuildStore(session=worker_session, logger=_logger())
        completing = asyncio.ensure_future(
            worker_store.get_for_update(build_id=build.id)
        )
        try:
            await wait_until_blocked_on_lock(probe, pid=worker_pid)
            await delete_session.commit()
            observed = await completing
        finally:
            if not completing.done():
                completing.cancel()
                with suppress(asyncio.CancelledError):
                    await completing
            await worker_session.rollback()

    assert observed is not None
    assert observed.status == BuildStatus.cancelled
    assert observed.date_deleted is not None
