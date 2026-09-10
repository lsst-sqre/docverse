"""Tests for the ``edition_reconcile`` per-org worker function.

The per-org job is where the reconciliation loop meets the rest of the
system, so these tests are about the wiring rather than the decision
table (which ``tests/domain/edition_reconcile_test.py`` pins case by
case): that a lost publish really does come back onto the queue tagged
as a repair, that a key the edge should not still be serving is deleted
through the same path a delete would use, that an organization with no
CDN never reaches for one, and that the ``queue_jobs`` row an operator
reads says what happened.

The drift itself is installed with ``MockEditionPublisher``'s seeding
methods rather than by driving a publish and breaking it afterwards: the
loop's whole job is to find state nothing in the tree produced on
purpose, so the fixtures have to be able to produce it the same way.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Any, Self

import httpx
import pytest
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher
from safir.testing.sentry import capture_events_fixture, sentry_init_fixture
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    JobKind,
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
from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.edition_pointer import EditionPointer
from docverse_server.domain.queue import JobStatus
from docverse_server.factory import Factory
from docverse_server.metrics import build_event_manager
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.editionpublisher import MockEditionPublisher
from docverse_server.storage.keeper_sync.state_store import TombstoneReason
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.edition_reconcile import (
    RECONCILED_DRIFT_MESSAGE,
    edition_reconcile,
)
from tests.support.arq_testing import get_jobs_by_name
from tests.worker.conftest import make_worker_ctx

_config = Configuration()

_HASH = "sha256:" + "b" * 64
_ORG_SLUG = "recon-org"
_PROJECT_SLUG = "recon-proj"
_EDITION_SLUG = "trunk"

_SETTLED = timedelta(hours=1)
"""How far back the fixtures push timestamps out of the grace window."""


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _payload(
    *, org_id: int, queue_job_id: int, org_slug: str = _ORG_SLUG
) -> dict[str, Any]:
    """Build the payload the dispatcher would have enqueued.

    ``org_slug`` defaults to the single-org fixture's slug; the tests
    that seed a CDN organization pass their own, because the slug is a
    dimension of the tick's metrics event and its Sentry tag rather than
    decoration on a log line.
    """
    return {
        "org_id": org_id,
        "org_slug": org_slug,
        "queue_job_id": queue_job_id,
    }


async def _seed_lost_phase_b(
    db_session: AsyncSession,
) -> tuple[int, int, int, int]:
    """Seed the exact rows a lost Phase B leaves behind.

    A client-upload project — no keeper-sync state, so none of
    ``keeper_sync_project``'s self-heal legs would ever look at it —
    whose edition points at a completed build, whose history row for the
    pair reads ``pending``, and whose ``publish_edition`` queue row is
    ``queued`` with a NULL ``backend_job_id`` because the arq enqueue
    never happened. Both timestamps are pushed outside the grace window
    so the tick treats the pair as settled rather than mid-enqueue.

    Returns the org id, the edition id, the build id and its public id.
    """
    logger = _logger()
    org = await OrganizationStore(session=db_session, logger=logger).create(
        OrganizationCreate(
            slug=_ORG_SLUG,
            title="Recon Org",
            base_domain=f"{_ORG_SLUG}.example.com",
        )
    )
    project = await ProjectStore(session=db_session, logger=logger).create(
        org_id=org.id,
        data=ProjectCreate(
            slug=_PROJECT_SLUG,
            title="Recon Project",
            source_url="https://example.com/example/recon",
        ),
    )
    edition_store = EditionStore(session=db_session, logger=logger)
    edition = await edition_store.create(
        project_id=project.id,
        data=EditionCreate(
            slug=_EDITION_SLUG,
            title="Trunk",
            kind=EditionKind.draft,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "main"},
        ),
    )
    build_store = BuildStore(session=db_session, logger=logger)
    build = await build_store.create(
        project_id=project.id,
        project_slug=project.slug,
        data=BuildCreate(git_ref="main", content_hash=_HASH),
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
    history_store = EditionBuildHistoryStore(session=db_session, logger=logger)
    history = await history_store.record(
        edition_id=edition.id, build_id=build.id
    )
    await history_store.set_publish_status(
        history_id=history.id, status=PublishStatus.pending
    )
    await edition_store.set_publish_status(
        edition_id=edition.id, status=PublishStatus.pending
    )
    # The orphaned Phase A row: committed, never enqueued.
    await QueueJobStore(session=db_session, logger=logger).create(
        kind=JobKind.publish_edition,
        org_id=org.id,
        project_id=project.id,
        build_id=build.id,
        edition_id=edition.id,
    )
    settled = datetime.now(tz=UTC) - _SETTLED
    await db_session.execute(
        update(SqlEdition)
        .where(SqlEdition.id == edition.id)
        .values(date_updated=settled)
    )
    await db_session.execute(
        update(SqlEditionBuildHistory)
        .where(SqlEditionBuildHistory.id == history.id)
        .values(date_created=settled)
    )
    return org.id, edition.id, build.id, build.public_id


async def _seed_reconcile_job(db_session: AsyncSession, *, org_id: int) -> int:
    """Create the queued per-org row the dispatcher would have written."""
    job = await QueueJobStore(session=db_session, logger=_logger()).create(
        kind=JobKind.edition_reconcile,
        org_id=org_id,
        subject_label=_ORG_SLUG,
        backend_job_id="test-edition-reconcile-job",
    )
    return job.id


async def _read_queue_job(job_id: int) -> SqlQueueJob:
    async for session in db_session_dependency():
        async with session.begin():
            row = await session.get(SqlQueueJob, job_id)
            assert row is not None
            await session.refresh(row)
            return row
    msg = "No database session available"
    raise RuntimeError(msg)


async def _read_history_status(
    *, edition_id: int, build_id: int
) -> PublishStatus | None:
    async for session in db_session_dependency():
        async with session.begin():
            history = await EditionBuildHistoryStore(
                session=session, logger=_logger()
            ).get_by_edition_and_build(
                edition_id=edition_id, build_id=build_id
            )
            assert history is not None
            return history.publish_status
    msg = "No database session available"
    raise RuntimeError(msg)


@pytest.mark.asyncio
async def test_edition_reconcile_redrives_a_lost_publish(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A lost Phase B comes back onto the queue, tagged as a repair.

    This is the gap the loop was written for: a client-upload project
    has no keeper-sync run to heal it, the orphan sweep only fails the
    ``queue_jobs`` row, and nothing else in the tree ever re-reads the
    pair. The re-driven job carries ``trigger=reconcile`` so the publish
    is attributable to the repair rather than to a build fan-out that
    never happened.
    """
    async with db_session.begin():
        (
            org_id,
            edition_id,
            build_id,
            build_public_id,
        ) = await _seed_lost_phase_b(db_session)
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)

    result = await edition_reconcile(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
    )
    await ctx["http_client"].aclose()

    assert result == "completed"
    jobs = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name=_config.arq_queue_name
    )
    assert len(jobs) == 1
    published_payload = jobs[0].kwargs["payload"]
    assert published_payload["trigger"] == "reconcile"
    assert published_payload["edition_id"] == edition_id
    assert published_payload["build_id"] == build_id
    assert published_payload["build_public_id"] == serialize_base32_id(
        build_public_id
    )
    assert (
        await _read_history_status(edition_id=edition_id, build_id=build_id)
        == PublishStatus.pending
    )

    row = await _read_queue_job(queue_job_id)
    assert row.status == JobStatus.completed.value
    assert row.progress is not None
    assert row.progress["republished"] == 1
    assert row.progress["republish_failed"] == 0
    assert row.progress["editions_scanned"] == 1
    assert row.progress["capped"] == 0


@pytest.mark.asyncio
async def test_edition_reconcile_is_idempotent_across_ticks(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A second tick enqueues nothing for a pair the first re-drove.

    The re-drive leaves a live ``publish_edition`` row behind, and the
    loop's whole safety story rests on recognising that: without it,
    every tick would pile another publish onto a queue that is already
    working on the pair.
    """
    async with db_session.begin():
        org_id, _, _, _ = await _seed_lost_phase_b(db_session)
        first_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    await edition_reconcile(
        ctx, _payload(org_id=org_id, queue_job_id=first_job_id)
    )

    async with db_session.begin():
        second_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    result = await edition_reconcile(
        ctx, _payload(org_id=org_id, queue_job_id=second_job_id)
    )
    await ctx["http_client"].aclose()

    assert result == "completed"
    jobs = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name=_config.arq_queue_name
    )
    assert len(jobs) == 1
    row = await _read_queue_job(second_job_id)
    assert row.progress is not None
    assert row.progress["republished"] == 0


@pytest.mark.asyncio
async def test_edition_reconcile_skips_a_row_it_did_not_claim(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A row a reaper already failed is not re-run by a late delivery.

    Same late-delivery guard the other per-org jobs take first: the
    reaper failing the row is what releases the org's mutex, so running
    the tick anyway would put a second reconciler on an org that already
    has one.
    """
    async with db_session.begin():
        org_id, _, _, _ = await _seed_lost_phase_b(db_session)
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)
        await QueueJobStore(session=db_session, logger=_logger()).fail(
            queue_job_id, errors={"message": "reaped"}
        )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)

    result = await edition_reconcile(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
    )
    await ctx["http_client"].aclose()

    assert result == "skipped"
    assert (
        get_jobs_by_name(
            mock_arq, "publish_edition", queue_name=_config.arq_queue_name
        )
        == []
    )


@dataclass(frozen=True)
class _SeededEdition:
    """One edition of a CDN organization, as the fixtures leave it."""

    edition_id: int
    project_id: int
    slug: str
    build_id: int
    build_public_id: str
    storage_prefix: str


async def _tombstone(
    db_session: AsyncSession, edition: _SeededEdition, *, org_id: int
) -> None:
    """Soft-delete a seeded edition without unpublishing its pointer.

    Exactly the state an API pod dying between the tombstone commit and
    the post-commit unpublish leaves behind, which is the drift the
    reconciler's unpublish leg exists to clean up.
    """
    deleted = await EditionStore(
        session=db_session, logger=_logger()
    ).soft_delete(
        org_id=org_id,
        project_id=edition.project_id,
        slug=edition.slug,
        reason=TombstoneReason.manual_delete,
    )
    assert deleted


def _mock_publisher_provider(publisher: Any) -> Any:
    """Patch the publisher provider to hand back ``publisher``."""

    async def _create(
        self: Factory, *, org_id: int, service_label: str
    ) -> Any:
        _ = (self, org_id, service_label)
        return publisher

    return _create


async def _refuse_to_resolve_publisher(
    self: Factory, *, org_id: int, service_label: str
) -> Any:
    """Patch the provider so resolving a publisher at all is a failure."""
    _ = self
    msg = f"publisher resolved for org {org_id} ({service_label})"
    raise AssertionError(msg)


class _FlakyUnpublisher:
    """A publisher whose ``unpublish`` raises for one edition slug.

    Stands in for the ways a single key delete can fail on its own — a
    Cloudflare 5xx for that one call, a transient timeout — while the
    rest of the organization's keys are perfectly deletable.
    """

    def __init__(
        self, inner: MockEditionPublisher, *, failing_slug: str
    ) -> None:
        self._inner = inner
        self._failing_slug = failing_slug

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    async def publish(self, **kwargs: Any) -> None:
        await self._inner.publish(**kwargs)

    async def unpublish(self, *, project_slug: str, edition_slug: str) -> None:
        if edition_slug == self._failing_slug:
            msg = f"KV delete failed for {edition_slug}"
            raise RuntimeError(msg)
        await self._inner.unpublish(
            project_slug=project_slug, edition_slug=edition_slug
        )

    async def get_pointers(
        self, keys: Sequence[str]
    ) -> Mapping[str, EditionPointer | None]:
        return await self._inner.get_pointers(keys)


async def _seed_cdn_org(
    db_session: AsyncSession,
    *,
    org_slug: str,
    project_slug: str,
    edition_slugs: Sequence[str],
) -> tuple[int, list[_SeededEdition]]:
    """Seed an org with a CDN and fully published editions.

    Every edition points at a completed build and its history pair reads
    ``published``, which is the converged state: on its own this org has
    nothing for the loop to do, so whatever a test then does to the
    edge's pointers is the only drift in play.
    """
    logger = _logger()
    org = await OrganizationStore(session=db_session, logger=logger).create(
        OrganizationCreate(
            slug=org_slug,
            title="CDN Recon Org",
            base_domain=f"{org_slug}.example.com",
        )
    )
    await db_session.execute(
        update(SqlOrganization)
        .where(SqlOrganization.id == org.id)
        .values(cdn_service_label="cdn-prod")
    )
    project = await ProjectStore(session=db_session, logger=logger).create(
        org_id=org.id,
        data=ProjectCreate(
            slug=project_slug,
            title="CDN Recon Project",
            source_url="https://example.com/example/cdn-recon",
        ),
    )
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)
    history_store = EditionBuildHistoryStore(session=db_session, logger=logger)
    seeded: list[_SeededEdition] = []
    for slug in edition_slugs:
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
        history = await history_store.record(
            edition_id=edition.id, build_id=build.id
        )
        await history_store.set_publish_status(
            history_id=history.id, status=PublishStatus.published
        )
        await edition_store.set_publish_status(
            edition_id=edition.id, status=PublishStatus.published
        )
        seeded.append(
            _SeededEdition(
                edition_id=edition.id,
                project_id=project.id,
                slug=slug,
                build_id=build.id,
                build_public_id=serialize_base32_id(build.public_id),
                storage_prefix=build.storage_prefix,
            )
        )
    settled = datetime.now(tz=UTC) - _SETTLED
    await db_session.execute(
        update(SqlEdition)
        .where(SqlEdition.project_id == project.id)
        .values(date_updated=settled)
    )
    await db_session.execute(
        update(SqlEditionBuildHistory)
        .where(
            SqlEditionBuildHistory.edition_id.in_(
                [item.edition_id for item in seeded]
            )
        )
        .values(date_created=settled)
    )
    return org.id, seeded


def _seed_pointers(
    publisher: MockEditionPublisher,
    *,
    project_slug: str,
    editions: Sequence[_SeededEdition],
) -> None:
    """Install the pointer a converged edge would serve for each edition."""
    for item in editions:
        publisher.seed_pointer(
            project_slug=project_slug,
            edition_slug=item.slug,
            build_public_id=item.build_public_id,
            object_key_prefix=item.storage_prefix,
        )


@pytest.mark.asyncio
async def test_edition_reconcile_republishes_a_hand_deleted_pointer(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A published edition whose key vanished is put back on the queue.

    Nothing in the database is wrong here — the pair reads ``published``
    and every other check in the tree agrees — so the only thing that
    can find this drift is reading the edge back and disagreeing with
    it.
    """
    org_slug = "recon-cdn-missing"
    project_slug = "cdn-missing-proj"
    async with db_session.begin():
        org_id, editions = await _seed_cdn_org(
            db_session,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slugs=("main",),
        )
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    publisher = MockEditionPublisher()
    _seed_pointers(publisher, project_slug=project_slug, editions=editions)
    publisher.remove_pointer(
        project_slug=project_slug, edition_slug=editions[0].slug
    )
    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_publisher_provider(publisher),
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    result = await edition_reconcile(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
    )
    await ctx["http_client"].aclose()

    assert result == "completed"
    jobs = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name=_config.arq_queue_name
    )
    assert len(jobs) == 1
    assert jobs[0].kwargs["payload"]["trigger"] == "reconcile"
    assert jobs[0].kwargs["payload"]["edition_id"] == editions[0].edition_id

    row = await _read_queue_job(queue_job_id)
    assert row.progress is not None
    assert row.progress["republished"] == 1
    assert row.progress["healthy"] == 0
    assert row.progress["cdn_checked"] is True


@pytest.mark.asyncio
async def test_edition_reconcile_leaves_a_converged_org_alone(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An org whose edge agrees with its database is reported healthy.

    The steady state, and the case that decides whether the loop can be
    left switched on: a tick that manufactured work out of a converged
    org would republish the whole estate twice an hour.
    """
    org_slug = "recon-cdn-healthy"
    project_slug = "cdn-healthy-proj"
    async with db_session.begin():
        org_id, editions = await _seed_cdn_org(
            db_session,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slugs=("main",),
        )
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    publisher = MockEditionPublisher()
    _seed_pointers(publisher, project_slug=project_slug, editions=editions)
    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_publisher_provider(publisher),
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    result = await edition_reconcile(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
    )
    await ctx["http_client"].aclose()

    assert result == "completed"
    assert (
        get_jobs_by_name(
            mock_arq, "publish_edition", queue_name=_config.arq_queue_name
        )
        == []
    )
    assert publisher.unpublish_calls == []
    row = await _read_queue_job(queue_job_id)
    assert row.progress is not None
    assert row.progress["healthy"] == 1
    assert row.progress["republished"] == 0
    assert row.progress["unpublished"] == 0


@pytest.mark.asyncio
async def test_edition_reconcile_deletes_a_tombstoned_editions_key(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A key that outlived its edition is removed from the edge.

    The soft-delete path removes the pointer only after the tombstone
    commits, so an API pod dying in between leaves a deleted edition
    serving content forever. Nothing else re-reads that key.
    """
    org_slug = "recon-cdn-tombstone"
    project_slug = "cdn-tombstone-proj"
    async with db_session.begin():
        org_id, editions = await _seed_cdn_org(
            db_session,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slugs=("doomed",),
        )
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)
        await _tombstone(db_session, editions[0], org_id=org_id)

    publisher = MockEditionPublisher()
    _seed_pointers(publisher, project_slug=project_slug, editions=editions)
    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_publisher_provider(publisher),
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    result = await edition_reconcile(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
    )
    await ctx["http_client"].aclose()

    assert result == "completed"
    assert [call.edition_slug for call in publisher.unpublish_calls] == [
        "doomed"
    ]
    assert publisher.pointers == {}
    assert (
        get_jobs_by_name(
            mock_arq, "publish_edition", queue_name=_config.arq_queue_name
        )
        == []
    )
    row = await _read_queue_job(queue_job_id)
    assert row.progress is not None
    assert row.progress["unpublished"] == 1
    assert row.progress["tombstoned"] == 0


@pytest.mark.asyncio
async def test_edition_reconcile_never_opens_a_publisher_without_a_cdn(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An org with no ``cdn_service_label`` runs the database leg only.

    It still recovers a lost enqueue — that half of the table needs no
    edge at all — but resolving a publisher for an org that has no CDN
    service would raise, so the tick must not even try.
    """
    async with db_session.begin():
        org_id, _, _, _ = await _seed_lost_phase_b(db_session)
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _refuse_to_resolve_publisher,
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    result = await edition_reconcile(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
    )
    await ctx["http_client"].aclose()

    assert result == "completed"
    assert (
        len(
            get_jobs_by_name(
                mock_arq, "publish_edition", queue_name=_config.arq_queue_name
            )
        )
        == 1
    )
    row = await _read_queue_job(queue_job_id)
    assert row.progress is not None
    assert row.progress["republished"] == 1
    assert row.progress["cdn_checked"] is False


@pytest.mark.asyncio
async def test_edition_reconcile_survives_one_failing_unpublish(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One key the edge refuses to delete costs that key, not the tick.

    The tick is the org's only recovery path for a stranded key, so
    letting the first failure abort it would leave every key behind it
    serving deleted content until an operator noticed.
    """
    org_slug = "recon-cdn-flaky"
    project_slug = "cdn-flaky-proj"
    async with db_session.begin():
        org_id, editions = await _seed_cdn_org(
            db_session,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slugs=("first", "second"),
        )
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)
        for item in editions:
            await _tombstone(db_session, item, org_id=org_id)

    inner = MockEditionPublisher()
    _seed_pointers(inner, project_slug=project_slug, editions=editions)
    publisher = _FlakyUnpublisher(inner, failing_slug="first")
    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_publisher_provider(publisher),
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    result = await edition_reconcile(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
    )
    await ctx["http_client"].aclose()

    assert result == "completed_with_errors"
    assert [call.edition_slug for call in inner.unpublish_calls] == ["second"]
    row_job = await _read_queue_job(queue_job_id)
    assert row_job.progress is not None
    assert row_job.progress["unpublished"] == 1
    assert row_job.progress["unpublish_failed"] == 1
    assert row_job.progress["failed_editions"] == [f"{project_slug}/first"]


@pytest.mark.asyncio
async def test_edition_reconcile_reports_what_the_edge_answered_with(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tick records how many keys the edge actually served.

    ``cdn_checked`` says the read-back happened; on its own it cannot
    say whether the edge answered for the whole org or for none of it.
    Seeded with one converged edition and one whose key is gone, so the
    two numbers have to differ: two editions scanned, one pointer read.
    """
    org_slug = "recon-cdn-counted"
    project_slug = "cdn-counted-proj"
    async with db_session.begin():
        org_id, editions = await _seed_cdn_org(
            db_session,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slugs=("kept", "lost"),
        )
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    publisher = MockEditionPublisher()
    _seed_pointers(publisher, project_slug=project_slug, editions=editions)
    publisher.remove_pointer(project_slug=project_slug, edition_slug="lost")
    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_publisher_provider(publisher),
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    await edition_reconcile(
        ctx,
        _payload(
            org_id=org_id,
            queue_job_id=queue_job_id,
            org_slug=org_slug,
        ),
    )
    await ctx["http_client"].aclose()

    row = await _read_queue_job(queue_job_id)
    assert row.progress is not None
    assert row.progress["editions_scanned"] == 2
    assert row.progress["cdn_checked"] is True
    assert row.progress["pointers_read"] == 1


@pytest.mark.asyncio
async def test_edition_reconcile_names_every_edition_it_repaired(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tick that repaired something says which editions, at warning.

    The counts alone tell an operator that an org is drifting and
    nothing about where; the loop has already repaired the drift by the
    time anyone reads the line, so naming the editions is the only way
    back to what went wrong. Both action buckets are exercised at once
    because they are separate lists: an edition that came back and one
    whose key went away are opposite repairs.
    """
    org_slug = "recon-cdn-named"
    project_slug = "cdn-named-proj"
    async with db_session.begin():
        org_id, editions = await _seed_cdn_org(
            db_session,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slugs=("alpha", "beta"),
        )
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)
        await _tombstone(db_session, editions[1], org_id=org_id)

    publisher = MockEditionPublisher()
    _seed_pointers(publisher, project_slug=project_slug, editions=editions)
    publisher.remove_pointer(project_slug=project_slug, edition_slug="alpha")
    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_publisher_provider(publisher),
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    with capture_logs() as logs:
        result = await edition_reconcile(
            ctx,
            _payload(
                org_id=org_id,
                queue_job_id=queue_job_id,
                org_slug=org_slug,
            ),
        )
    await ctx["http_client"].aclose()

    assert result == "completed"
    drift = [
        entry
        for entry in logs
        if entry["event"] == "Reconciled drifted editions"
    ]
    assert len(drift) == 1
    assert drift[0]["log_level"] == "warning"
    assert drift[0]["republished"] == 1
    assert drift[0]["unpublished"] == 1
    assert drift[0]["republished_editions"] == [f"{project_slug}/alpha"]
    assert drift[0]["unpublished_editions"] == [f"{project_slug}/beta"]


@pytest.mark.asyncio
async def test_edition_reconcile_keeps_a_clean_tick_at_debug(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A converged org's tick must not page anyone.

    The loop runs twice an hour against every org, so the steady state
    is nearly every line it will ever emit. Logging that at warning
    would bury the ticks that actually repaired something.
    """
    org_slug = "recon-cdn-quiet"
    project_slug = "cdn-quiet-proj"
    async with db_session.begin():
        org_id, editions = await _seed_cdn_org(
            db_session,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slugs=("main",),
        )
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    publisher = MockEditionPublisher()
    _seed_pointers(publisher, project_slug=project_slug, editions=editions)
    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_publisher_provider(publisher),
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    with capture_logs() as logs:
        await edition_reconcile(
            ctx,
            _payload(
                org_id=org_id,
                queue_job_id=queue_job_id,
                org_slug=org_slug,
            ),
        )
    await ctx["http_client"].aclose()

    quiet = [
        entry
        for entry in logs
        if entry["event"] == "No edition drift to reconcile"
    ]
    assert len(quiet) == 1
    assert quiet[0]["log_level"] == "debug"
    assert quiet[0]["healthy"] == 1
    assert [
        entry
        for entry in logs
        if entry["event"] == "Reconciled drifted editions"
    ] == []


@pytest.mark.asyncio
async def test_edition_reconcile_publishes_a_drifted_orgs_tally(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tick's durable record is the event it publishes.

    The ``queue_jobs`` row is subject to retention and the warning log
    to whatever the cluster keeps, so the org-scoped completion event is
    what survives to answer "how much of this environment's publish
    traffic is the system healing itself". Seeded so both action buckets
    and a skip bucket land in one tick, which is what makes the event's
    numbers worth charting rather than just a boolean.
    """
    org_slug = "recon-cdn-metrics"
    project_slug = "cdn-metrics-proj"
    manager, events = await build_event_manager(Configuration())
    async with db_session.begin():
        org_id, editions = await _seed_cdn_org(
            db_session,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slugs=("alpha", "beta", "gamma"),
        )
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)
        await _tombstone(db_session, editions[1], org_id=org_id)

    publisher = MockEditionPublisher()
    _seed_pointers(publisher, project_slug=project_slug, editions=editions)
    publisher.remove_pointer(project_slug=project_slug, edition_slug="alpha")
    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_publisher_provider(publisher),
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(), arq_queue=mock_arq, events=events
    )
    result = await edition_reconcile(
        ctx,
        _payload(
            org_id=org_id,
            queue_job_id=queue_job_id,
            org_slug=org_slug,
        ),
    )
    await ctx["http_client"].aclose()

    assert result == "completed"
    completed = events.edition_reconcile_completed
    assert isinstance(completed, MockEventPublisher)
    assert len(completed.published) == 1
    tick = completed.published[0]
    assert tick.organization == org_slug
    # One tick spans every project in the org, so it is org-scoped.
    assert tick.project is None
    assert tick.editions_scanned == 3
    # ``alpha``'s key was removed, so only ``beta`` and ``gamma`` answer.
    assert tick.pointers_read == 2
    assert tick.republished == 1
    assert tick.unpublished == 1
    assert tick.in_flight_skipped == 0
    assert tick.failed_left_alone == 0
    assert tick.unexpected_pointers == 0
    assert tick.capped == 0
    assert tick.cdn_checked is True
    assert tick.elapsed >= timedelta(0)
    await manager.aclose()


@pytest.mark.asyncio
async def test_edition_reconcile_publishes_a_clean_orgs_zero_tally(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A converged org still publishes, with every action count zero.

    Silence would be ambiguous in exactly the wrong direction: a tick
    that found nothing and a tick that never ran look identical if the
    clean case publishes nothing, and the loop's whole value is the
    claim that it is running.
    """
    org_slug = "recon-cdn-zero"
    project_slug = "cdn-zero-proj"
    manager, events = await build_event_manager(Configuration())
    async with db_session.begin():
        org_id, editions = await _seed_cdn_org(
            db_session,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slugs=("main",),
        )
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    publisher = MockEditionPublisher()
    _seed_pointers(publisher, project_slug=project_slug, editions=editions)
    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_publisher_provider(publisher),
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(), arq_queue=mock_arq, events=events
    )
    await edition_reconcile(
        ctx,
        _payload(
            org_id=org_id,
            queue_job_id=queue_job_id,
            org_slug=org_slug,
        ),
    )
    await ctx["http_client"].aclose()

    completed = events.edition_reconcile_completed
    assert isinstance(completed, MockEventPublisher)
    assert len(completed.published) == 1
    tick = completed.published[0]
    assert tick.editions_scanned == 1
    assert tick.pointers_read == 1
    assert tick.republished == 0
    assert tick.unpublished == 0
    assert tick.capped == 0
    assert tick.cdn_checked is True
    await manager.aclose()


@pytest.mark.asyncio
async def test_edition_reconcile_pages_once_for_a_drifted_org(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drift the loop repaired still owes an operator one Sentry message.

    The repair is not the end of the story: something else lost a
    publish or a delete, and the loop can say that an organization is
    drifting but never why. One message per org tick, not one per
    edition — a badly drifted org would otherwise open a hundred issues
    for a single cause.
    """
    org_slug = "recon-cdn-paged"
    project_slug = "cdn-paged-proj"
    async with db_session.begin():
        org_id, editions = await _seed_cdn_org(
            db_session,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slugs=("alpha", "beta"),
        )
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    publisher = MockEditionPublisher()
    _seed_pointers(publisher, project_slug=project_slug, editions=editions)
    for item in editions:
        publisher.remove_pointer(
            project_slug=project_slug, edition_slug=item.slug
        )
    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_publisher_provider(publisher),
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    with sentry_init_fixture() as init:
        init(environment="test")
        captured = capture_events_fixture(monkeypatch)()
        await edition_reconcile(
            ctx,
            _payload(
                org_id=org_id,
                queue_job_id=queue_job_id,
                org_slug=org_slug,
            ),
        )
    await ctx["http_client"].aclose()

    assert len(captured.errors) == 1
    event = captured.errors[0]
    assert event["level"] == "warning"
    assert event["message"] == RECONCILED_DRIFT_MESSAGE
    assert event["tags"]["organization"] == org_slug
    context = event["contexts"]["edition_reconcile"]
    assert context["republished"] == 2
    assert context["unpublished"] == 0
    assert context["editions_scanned"] == 2
    assert context["republished_editions"] == [
        f"{project_slug}/alpha",
        f"{project_slug}/beta",
    ]


@pytest.mark.asyncio
async def test_edition_reconcile_pages_nobody_for_a_clean_org(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A converged tick is the steady state and must reach nobody.

    Every org is reconciled twice an hour, so a message on the clean
    path would be thousands of Sentry events a day saying nothing was
    wrong — and the drifted ones would be unfindable among them.
    """
    org_slug = "recon-cdn-unpaged"
    project_slug = "cdn-unpaged-proj"
    async with db_session.begin():
        org_id, editions = await _seed_cdn_org(
            db_session,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slugs=("main",),
        )
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    publisher = MockEditionPublisher()
    _seed_pointers(publisher, project_slug=project_slug, editions=editions)
    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_publisher_provider(publisher),
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    with sentry_init_fixture() as init:
        init(environment="test")
        captured = capture_events_fixture(monkeypatch)()
        await edition_reconcile(
            ctx,
            _payload(
                org_id=org_id,
                queue_job_id=queue_job_id,
                org_slug=org_slug,
            ),
        )
    await ctx["http_client"].aclose()

    assert captured.errors == []
