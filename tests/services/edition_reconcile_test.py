"""Tests for :class:`EditionReconcileService`.

The service's own contract is narrow — read, plan, apply — so what is
pinned here is the part neither the pure planner nor the worker can
speak for: that one edition's failure is contained, and that the grace
window the service plans with is the same window the orphan sweeps age
rows out against.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from safir.testing.sentry import capture_events_fixture, sentry_init_fixture
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    JobKind,
    JobStatus,
    OrganizationCreate,
    ProjectCreate,
    PublishStatus,
    TrackingMode,
)
from docverse_server.config import Configuration
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.dbschema.edition_build_history import (
    SqlEditionBuildHistory,
)
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.edition_build_history import EditionBuildHistory
from docverse_server.domain.edition_reconcile import EditionReconcilePlan
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


@dataclass(frozen=True, slots=True)
class _DriftedOrg:
    """An org seeded for the apply-time re-check tests.

    Carries the ids those tests steer with: the edition whose action a
    rival writer will invalidate, the build the plan was made against,
    and a second completed build for the repoint to land on.
    """

    org: Organization
    project_id: int
    alpha_edition_id: int
    alpha_build_id: int
    spare_build_id: int


async def _seed_drift_with_a_spare_build(
    db_session: AsyncSession, *, key: str
) -> _DriftedOrg:
    """Seed two lost-enqueue editions plus a build nothing points at.

    The same shape as :func:`_seed_two_drifted_editions` — neither
    edition has an ``edition_build_history`` row, and both are aged out
    of the grace window, so the tick plans a republish for each — with
    one extra completed build in the project for a test to repoint
    ``alpha`` onto after the plan has been made. ``beta`` is seeded
    alongside so each test also shows that dropping one action leaves
    the rest of the org's repairs alone.
    """
    logger = _logger()
    org = await OrganizationStore(session=db_session, logger=logger).create(
        OrganizationCreate(
            slug=f"svc-{key}-org",
            title=f"Service {key} Org",
            base_domain=f"svc-{key}.example.com",
        )
    )
    project = await ProjectStore(session=db_session, logger=logger).create(
        org_id=org.id,
        data=ProjectCreate(
            slug=f"svc-{key}-proj",
            title=f"Service {key} Project",
            source_url=f"https://example.com/example/svc-{key}",
        ),
    )
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)

    async def _completed_build(git_ref: str) -> int:
        build = await build_store.create(
            project_id=project.id,
            project_slug=project.slug,
            data=BuildCreate(git_ref=git_ref, content_hash=_HASH),
            uploader="testuser",
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.completed
        )
        return build.id

    edition_ids: dict[str, int] = {}
    build_ids: dict[str, int] = {}
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
        build_id = await _completed_build(slug)
        await edition_store.set_current_build(
            edition_id=edition.id, build_id=build_id
        )
        edition_ids[slug] = edition.id
        build_ids[slug] = build_id
    spare_build_id = await _completed_build("alpha-next")
    await db_session.execute(
        update(SqlEdition)
        .where(SqlEdition.project_id == project.id)
        .values(date_updated=datetime.now(tz=UTC) - timedelta(hours=1))
    )
    return _DriftedOrg(
        org=org,
        project_id=project.id,
        alpha_edition_id=edition_ids["alpha"],
        alpha_build_id=build_ids["alpha"],
        spare_build_id=spare_build_id,
    )


class _InterruptedReconcileService(EditionReconcileService):
    """Reconciler that lets a rival writer land between plan and apply.

    Both apply-time re-checks defend one window: the plan transaction
    has closed and Phase A has not opened. Nothing the service exposes
    reaches into that window — it is precisely where the service holds
    nothing — so a subclass that runs the rival writer as ``_plan``
    returns is how a test puts a writer there deterministically.
    """

    def __init__(
        self, *, interruption: Callable[[], Awaitable[None]], **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self._interruption = interruption

    async def _plan(
        self, org: Organization, *, limit: int
    ) -> EditionReconcilePlan:
        plan = await super()._plan(org, limit=limit)
        await self._interruption()
        return plan


def _interrupted_service(
    session: AsyncSession,
    *,
    queue_backend: ArqQueueBackend,
    interruption: Callable[[], Awaitable[None]],
) -> _InterruptedReconcileService:
    """Build the reconciler under test with its rival writer attached."""
    return _InterruptedReconcileService(
        interruption=interruption,
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


async def _publish_job_count(
    db_session: AsyncSession, *, edition_id: int
) -> int:
    """Count the ``publish_edition`` queue rows written for one edition."""
    result = await db_session.execute(
        select(func.count())
        .select_from(SqlQueueJob)
        .where(
            SqlQueueJob.kind == JobKind.publish_edition.value,
            SqlQueueJob.edition_id == edition_id,
        )
    )
    return int(result.scalar_one())


async def _publish_status(
    db_session: AsyncSession, *, edition_id: int
) -> str | None:
    """Read an edition's ``publish_status`` column."""
    result = await db_session.execute(
        select(SqlEdition.publish_status).where(SqlEdition.id == edition_id)
    )
    return result.scalar_one()


@pytest.mark.asyncio
async def test_a_repoint_between_plan_and_apply_drops_the_republish(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A superseded action is dropped rather than enqueued.

    The plan captures ``(edition, build)`` and Phase A can run up to a
    whole tick's worth of actions later. If tracking, a rollback, or
    keeper-sync repoints the edition in that window, enqueuing the
    planned build would put a publish of superseded content on the
    queue — and because the edition advisory lock serializes publishes
    by pickup order rather than enqueue order, the edge could land on
    the old build and stay there until the next tick.
    """
    async with db_session.begin():
        seeded = await _seed_drift_with_a_spare_build(
            db_session, key="superseded"
        )

    async def _repoint() -> None:
        async with db_session.begin():
            await db_session.execute(
                update(SqlEdition)
                .where(SqlEdition.id == seeded.alpha_edition_id)
                .values(current_build_id=seeded.spare_build_id)
            )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    queue_backend = ArqQueueBackend(
        arq_queue=mock_arq, default_queue_name=_config.arq_queue_name
    )
    async for session in db_session_dependency():
        service = _interrupted_service(
            session, queue_backend=queue_backend, interruption=_repoint
        )
        outcome = await service.reconcile_org(seeded.org, limit=10)
        break

    assert outcome.superseded_skipped == 1
    assert outcome.as_progress()["superseded_skipped"] == 1
    assert outcome.republish_failed == 0
    assert not outcome.has_errors
    # The org's other drift is still repaired.
    assert outcome.republished == 1
    assert outcome.republished_editions == ["svc-superseded-proj/beta"]
    # Phase A wrote nothing at all for the superseded pair.
    assert (
        await _publish_job_count(
            db_session, edition_id=seeded.alpha_edition_id
        )
        == 0
    )
    assert (
        await _publish_status(db_session, edition_id=seeded.alpha_edition_id)
        is None
    )


@pytest.mark.asyncio
async def test_a_rival_publish_between_plan_and_apply_drops_the_republish(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A pair that acquired a live publish job is not enqueued twice.

    The planner's in-flight gate reads a snapshot taken in the plan
    transaction, which closes before the read-back and every enqueue.
    Another driver enqueuing the same pair inside that window would
    otherwise give the pair two publish jobs, two KV writes, and two
    ``edition_published`` events.
    """
    async with db_session.begin():
        seeded = await _seed_drift_with_a_spare_build(
            db_session, key="inflight"
        )

    async def _enqueue_a_rival_publish() -> None:
        async with db_session.begin():
            await QueueJobStore(session=db_session, logger=_logger()).create(
                kind=JobKind.publish_edition,
                org_id=seeded.org.id,
                project_id=seeded.project_id,
                build_id=seeded.alpha_build_id,
                edition_id=seeded.alpha_edition_id,
                backend_job_id="rival-publish-job",
                backend_queue_name=_config.arq_queue_name,
            )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    queue_backend = ArqQueueBackend(
        arq_queue=mock_arq, default_queue_name=_config.arq_queue_name
    )
    async for session in db_session_dependency():
        service = _interrupted_service(
            session,
            queue_backend=queue_backend,
            interruption=_enqueue_a_rival_publish,
        )
        outcome = await service.reconcile_org(seeded.org, limit=10)
        break

    assert outcome.in_flight_skipped == 1
    assert outcome.as_progress()["in_flight_skipped"] == 1
    assert outcome.republish_failed == 0
    assert not outcome.has_errors
    assert outcome.republished == 1
    assert outcome.republished_editions == ["svc-inflight-proj/beta"]
    # Only the rival's row exists; the reconciler added none.
    assert (
        await _publish_job_count(
            db_session, edition_id=seeded.alpha_edition_id
        )
        == 1
    )
    assert (
        await _publish_status(db_session, edition_id=seeded.alpha_edition_id)
        is None
    )


@dataclass(frozen=True, slots=True)
class _MidFlightOrg:
    """An org whose one edition is mid-publish when the tick starts.

    Carries the rows the completion the test races in has to touch: the
    history row a finishing publish stamps ``published`` and the
    ``queue_jobs`` row it then completes.
    """

    org: Organization
    edition_id: int
    build_id: int
    history_id: int
    job_id: int


async def _seed_a_publish_mid_flight(
    db_session: AsyncSession,
) -> _MidFlightOrg:
    """Seed one edition whose publish is running and nearly done.

    The pair reads ``publishing`` with a live ``publish_edition`` job
    behind it, and both the edition and the history row are aged out of
    the grace window — the shape of a publish that has been waiting on
    the queue longer than ``RECONCILE_GRACE_WINDOW``, which is the only
    shape in which the read order can cost anything.
    """
    logger = _logger()
    org = await OrganizationStore(session=db_session, logger=logger).create(
        OrganizationCreate(
            slug="svc-midflight-org",
            title="Service Midflight Org",
            base_domain="svc-midflight.example.com",
        )
    )
    project = await ProjectStore(session=db_session, logger=logger).create(
        org_id=org.id,
        data=ProjectCreate(
            slug="svc-midflight-proj",
            title="Service Midflight Project",
            source_url="https://example.com/example/svc-midflight",
        ),
    )
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)
    history_store = EditionBuildHistoryStore(session=db_session, logger=logger)
    edition = await edition_store.create(
        project_id=project.id,
        data=EditionCreate(
            slug="alpha",
            title="Alpha",
            kind=EditionKind.draft,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "alpha"},
        ),
    )
    build = await build_store.create(
        project_id=project.id,
        project_slug=project.slug,
        data=BuildCreate(git_ref="alpha", content_hash=_HASH),
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
    history = await history_store.record(
        edition_id=edition.id, build_id=build.id
    )
    await history_store.set_publish_status(
        history_id=history.id, status=PublishStatus.publishing
    )
    job = await QueueJobStore(session=db_session, logger=logger).create(
        kind=JobKind.publish_edition,
        org_id=org.id,
        project_id=project.id,
        build_id=build.id,
        edition_id=edition.id,
        backend_job_id="midflight-publish-job",
        backend_queue_name=_config.arq_queue_name,
    )
    aged = datetime.now(tz=UTC) - timedelta(hours=1)
    await db_session.execute(
        update(SqlEdition)
        .where(SqlEdition.id == edition.id)
        .values(
            date_updated=aged, publish_status=PublishStatus.publishing.value
        )
    )
    await db_session.execute(
        update(SqlEditionBuildHistory)
        .where(SqlEditionBuildHistory.id == history.id)
        .values(date_created=aged)
    )
    return _MidFlightOrg(
        org=org,
        edition_id=edition.id,
        build_id=build.id,
        history_id=history.id,
        job_id=job.id,
    )


def _race_the_plans_first_read(
    monkeypatch: pytest.MonkeyPatch,
    mutation: Callable[[], Awaitable[None]],
) -> None:
    """Commit ``mutation`` in the gap between the plan's two reads.

    The plan's reads do not share a snapshot — the session runs at the
    database default of READ COMMITTED — so whichever of the live-pairs
    read and the history read ``_plan`` issues first, the other sees
    anything that committed in between. Hooking the first of the two to
    return, rather than one named method, is what makes the test a
    statement about the *order* instead of about today's spelling of it.
    """
    original_pairs = QueueJobStore.list_live_publish_pairs
    original_history = EditionBuildHistoryStore.list_by_edition_build_pairs
    fired = False

    async def _fire() -> None:
        nonlocal fired
        if not fired:
            fired = True
            await mutation()

    async def _pairs(
        self: QueueJobStore, *, org_id: int
    ) -> set[tuple[int, int]]:
        result = await original_pairs(self, org_id=org_id)
        await _fire()
        return result

    async def _history(
        self: EditionBuildHistoryStore, pairs: Sequence[tuple[int, int]]
    ) -> list[EditionBuildHistory]:
        result = await original_history(self, pairs)
        await _fire()
        return result

    monkeypatch.setattr(QueueJobStore, "list_live_publish_pairs", _pairs)
    monkeypatch.setattr(
        EditionBuildHistoryStore, "list_by_edition_build_pairs", _history
    )


@pytest.mark.asyncio
async def test_a_publish_finishing_between_the_plans_reads_is_not_re_driven(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A publish that lands mid-plan is never called a stalled publish.

    A ``publish_edition`` job that was enqueued longer ago than the
    grace window — a queue backlog, or lock contention during a
    keeper-sync burst — commits ``published`` and then completes its
    queue row. Reading history before live jobs would catch it at
    exactly the wrong pair of instants: ``publishing`` with no live job,
    which is the ``stalled_publish`` shape, and the tick would spend a
    redundant job and a hostname purge resetting a converged pair to
    ``pending``.
    """
    async with db_session.begin():
        seeded = await _seed_a_publish_mid_flight(db_session)

    async def _finish_the_publish() -> None:
        async with db_session.begin():
            await db_session.execute(
                update(SqlEditionBuildHistory)
                .where(SqlEditionBuildHistory.id == seeded.history_id)
                .values(publish_status=PublishStatus.published.value)
            )
            await db_session.execute(
                update(SqlQueueJob)
                .where(SqlQueueJob.id == seeded.job_id)
                .values(
                    status=JobStatus.completed.value,
                    date_completed=datetime.now(tz=UTC),
                )
            )

    _race_the_plans_first_read(monkeypatch, _finish_the_publish)

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
        outcome = await service.reconcile_org(seeded.org, limit=10)
        break

    assert outcome.editions_scanned == 1
    assert outcome.republished == 0
    assert outcome.republished_editions == []
    assert outcome.republish_failed == 0
    assert not outcome.has_errors
    # The pair is held by the job the live-pairs read saw, not re-driven.
    assert outcome.in_flight_skipped == 1
    # Only the seeded publish row exists; the reconciler enqueued none.
    assert (
        await _publish_job_count(db_session, edition_id=seeded.edition_id) == 1
    )
    assert await _publish_status(db_session, edition_id=seeded.edition_id) == (
        PublishStatus.publishing.value
    )
