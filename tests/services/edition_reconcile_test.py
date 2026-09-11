"""Tests for :class:`EditionReconcileService`.

The service's own contract is narrow — read, plan, apply — so what is
pinned here is the part neither the pure planner nor the worker can
speak for: that one edition's failure is contained, and that the grace
window the service plans with is the same window the orphan sweeps age
rows out against.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from safir.testing.sentry import capture_events_fixture, sentry_init_fixture
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    PublishStatus,
    TrackingMode,
)
from docverse_server.config import Configuration
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.domain.organization import Organization
from docverse_server.services.cdn_purge_coalescer import CdnPurgeCoalescer
from docverse_server.services.edition_publishing import (
    EditionPublishingService,
)
from docverse_server.services.edition_reconcile import (
    RECONCILE_GRACE_WINDOW,
    EditionReconcileService,
)
from docverse_server.storage import edition_build_history_store
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.cdncachepurger import CdnCachePurger
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.editionpublisher import EditionPublisher
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_backend import ArqQueueBackend, EnqueuedJob
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions._runless_reaper import ORPHAN_IDLE_WINDOW

_config = Configuration()
_HASH = "sha256:" + "c" * 64


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _unreachable_publisher_provider(
    *, org_id: int, service_label: str
) -> EditionPublisher:
    """Fail loudly if anything asks this module for a publisher.

    The organizations in this module have no ``cdn_service_label``, so
    the read-back leg must never run. Raising is the assertion: a silent
    stub would let the DB-only path quietly acquire a CDN dependency.
    """
    msg = f"publisher resolved for org {org_id} ({service_label})"
    raise AssertionError(msg)


async def _unreachable_purger_provider(
    *, org_id: int, service_label: str
) -> CdnCachePurger:
    """Fail loudly on a purger the reconciler has no reason to reach."""
    msg = f"purger resolved for org {org_id} ({service_label})"
    raise AssertionError(msg)


def _publishing_service(session: AsyncSession) -> EditionPublishingService:
    """Build the collaborator the unpublish leg would use."""
    logger = _logger()
    return EditionPublishingService(
        org_store=OrganizationStore(session=session, logger=logger),
        edition_store=EditionStore(session=session, logger=logger),
        history_store=EditionBuildHistoryStore(session=session, logger=logger),
        publisher_provider=_unreachable_publisher_provider,
        purger_provider=_unreachable_purger_provider,
        purge_coalescer=CdnPurgeCoalescer(),
        logger=logger,
    )


def test_grace_window_matches_the_orphan_sweep() -> None:
    """The loop waits exactly as long as the orphan sweep does.

    The sweep waits ``ORPHAN_IDLE_WINDOW`` before calling a ``queued``
    row with no backend job id abandoned. If the reconciler waited any
    less it would re-drive pairs whose enqueue the sweep still considers
    in progress; any more and there would be a window in which a pair is
    abandoned by one and untouched by the other.
    """
    assert RECONCILE_GRACE_WINDOW == ORPHAN_IDLE_WINDOW


class _OneFailingQueueBackend(ArqQueueBackend):
    """Queue backend whose first ``enqueue`` raises, then behaves.

    Stands in for the shapes a real enqueue can fail in — Redis
    unreachable for a moment, a payload one edition's data makes
    unserialisable — without having to arrange one.
    """

    def __init__(self, *, arq_queue: MockArqQueue) -> None:
        super().__init__(
            arq_queue=arq_queue,
            default_queue_name=_config.arq_queue_name,
        )
        self.calls = 0

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        queue_name: str | None = None,
    ) -> EnqueuedJob:
        self.calls += 1
        if self.calls == 1:
            msg = "queue unavailable"
            raise RuntimeError(msg)
        return await super().enqueue(job_type, payload, queue_name=queue_name)


async def _seed_two_drifted_editions(
    db_session: AsyncSession,
) -> Organization:
    """Seed an org with two editions whose publishes were never enqueued.

    Neither edition has an ``edition_build_history`` row, which is the
    lost-enqueue shape, and both are pushed outside the grace window so
    the tick plans an action for each.
    """
    logger = _logger()
    org = await OrganizationStore(session=db_session, logger=logger).create(
        OrganizationCreate(
            slug="svc-recon-org",
            title="Service Recon Org",
            base_domain="svc-recon.example.com",
        )
    )
    project = await ProjectStore(session=db_session, logger=logger).create(
        org_id=org.id,
        data=ProjectCreate(
            slug="svc-recon-proj",
            title="Service Recon Project",
            source_url="https://example.com/example/svc-recon",
        ),
    )
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)
    for slug in ("alpha", "beta"):
        edition = await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug=slug,
                title=slug.title(),
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": slug},
            ),
        )
        build = await build_store.create(
            project_id=project.id,
            project_slug=project.slug,
            data=BuildCreate(git_ref=slug, content_hash=_HASH),
            uploader="testuser",
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.completed
        )
        await edition_store.set_current_build(
            edition_id=edition.id, build_id=build.id
        )
    await db_session.execute(
        update(SqlEdition).values(
            date_updated=datetime.now(tz=UTC) - timedelta(hours=1)
        )
    )
    return org


@pytest.mark.asyncio
async def test_one_failed_enqueue_does_not_abort_the_organization(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A raise on one edition costs that edition, not the whole tick.

    The tick is the organization's only recovery path, so letting the
    first bad edition abort it would strand every drift behind it until
    someone noticed. The failure is counted, named, and sent to Sentry
    instead.
    """
    async with db_session.begin():
        org = await _seed_two_drifted_editions(db_session)

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    queue_backend = _OneFailingQueueBackend(arq_queue=mock_arq)

    async for session in db_session_dependency():
        service = EditionReconcileService(
            session=session,
            edition_store=EditionStore(session=session, logger=_logger()),
            history_store=EditionBuildHistoryStore(
                session=session, logger=_logger()
            ),
            queue_job_store=QueueJobStore(session=session, logger=_logger()),
            queue_backend=queue_backend,
            publisher_provider=_unreachable_publisher_provider,
            publishing_service=_publishing_service(session),
            logger=_logger(),
        )
        with sentry_init_fixture() as init:
            init(environment="test")
            captured = capture_events_fixture(monkeypatch)()
            outcome = await service.reconcile_org(org, limit=10)
        break

    assert outcome.editions_scanned == 2
    assert outcome.republished == 1
    assert outcome.republish_failed == 1
    assert outcome.failed_editions == ["svc-recon-proj/alpha"]
    assert outcome.has_errors
    assert outcome.as_progress()["republished"] == 1
    assert len(captured.errors) == 1
    assert (
        captured.errors[0]["exception"]["values"][0]["type"] == "RuntimeError"
    )


async def _seed_one_drifted_one_converged(
    db_session: AsyncSession,
) -> Organization:
    """Seed an org whose two editions land in different history chunks.

    ``alpha`` has no ``edition_build_history`` row at all — the
    lost-enqueue shape — while ``beta``'s row already reads
    ``published``. Both are pushed outside the grace window, so the
    tick's verdict on each rests entirely on the history rows the store
    hands back: a read that lost a chunk would misread ``beta`` as
    drifted too and re-drive a publish that already happened.
    """
    logger = _logger()
    org = await OrganizationStore(session=db_session, logger=logger).create(
        OrganizationCreate(
            slug="svc-chunk-org",
            title="Service Chunk Org",
            base_domain="svc-chunk.example.com",
        )
    )
    project = await ProjectStore(session=db_session, logger=logger).create(
        org_id=org.id,
        data=ProjectCreate(
            slug="svc-chunk-proj",
            title="Service Chunk Project",
            source_url="https://example.com/example/svc-chunk",
        ),
    )
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)
    history_store = EditionBuildHistoryStore(session=db_session, logger=logger)
    for slug in ("alpha", "beta"):
        edition = await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug=slug,
                title=slug.title(),
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": slug},
            ),
        )
        build = await build_store.create(
            project_id=project.id,
            project_slug=project.slug,
            data=BuildCreate(git_ref=slug, content_hash=_HASH),
            uploader="testuser",
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.completed
        )
        await edition_store.set_current_build(
            edition_id=edition.id, build_id=build.id
        )
        if slug == "beta":
            history = await history_store.record(
                edition_id=edition.id, build_id=build.id
            )
            await history_store.set_publish_status(
                history_id=history.id, status=PublishStatus.published
            )
    await db_session.execute(
        update(SqlEdition).values(
            date_updated=datetime.now(tz=UTC) - timedelta(hours=1)
        )
    )
    return org


@pytest.mark.asyncio
async def test_plan_reads_every_history_chunk(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An org with more pairs than one chunk holds is still classified.

    ``_plan`` hands the store one ``(edition_id, build_id)`` pair per
    pointed edition, and the store answers in chunks so asyncpg's
    32,767-parameter ceiling cannot cap how large an organization the
    loop can reconcile. The chunk size is patched to one pair here
    rather than seeding the ~16,384 editions it would otherwise take.
    """
    monkeypatch.setattr(edition_build_history_store, "_PAIR_CHUNK_SIZE", 1)
    async with db_session.begin():
        org = await _seed_one_drifted_one_converged(db_session)

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    queue_backend = ArqQueueBackend(
        arq_queue=mock_arq, default_queue_name=_config.arq_queue_name
    )

    async for session in db_session_dependency():
        service = EditionReconcileService(
            session=session,
            edition_store=EditionStore(session=session, logger=_logger()),
            history_store=EditionBuildHistoryStore(
                session=session, logger=_logger()
            ),
            queue_job_store=QueueJobStore(session=session, logger=_logger()),
            queue_backend=queue_backend,
            publisher_provider=_unreachable_publisher_provider,
            publishing_service=_publishing_service(session),
            logger=_logger(),
        )
        outcome = await service.reconcile_org(org, limit=10)
        break

    assert outcome.editions_scanned == 2
    assert outcome.healthy == 1
    assert outcome.republished == 1
    assert outcome.republished_editions == ["svc-chunk-proj/alpha"]
    assert not outcome.has_errors
