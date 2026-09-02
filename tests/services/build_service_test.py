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

import pytest
import structlog
from safir.arq import MockArqQueue
from sqlalchemy.ext.asyncio import AsyncSession

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
