"""Integration tests for the build_processing worker function."""

from __future__ import annotations

import io
import tarfile
import time
from datetime import timedelta
from typing import Any

import httpx
import pytest
import respx
import structlog
from rubin.repertoire import DiscoveryClient, register_mock_discovery
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.client.models.queue_enums import PublishStatus
from docverse.config import Configuration
from docverse.dbschema.organization import SqlOrganization
from docverse.dbschema.project import SqlProject
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.api_urls import edition_url, queue_job_url
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.queue import JobKind, JobStatus
from docverse.factory import Factory
from docverse.metrics import build_event_manager
from docverse.services.edition_tracking import EditionTrackingService
from docverse.services.lock_service import LockClass, LockKey
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.objectstore import MockObjectStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_backend import ArqQueueBackend
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.build_processing import build_processing
from tests.support.arq_testing import (
    count_jobs_by_name,
    get_jobs_by_name,
    queue_names,
)
from tests.support.lock_service_spy import install_recording_lock_service
from tests.worker.conftest import make_worker_ctx

_HASH = "sha256:" + "a" * 64

_config = Configuration()

#: Docverse API base URL registered for the ``docverse`` internal service
#: in ``tests/data/discovery.json`` (the autouse ``mock_discovery``
#: fixture). HATEOAS links in build_processing progress hang off it.
_DISCOVERY_BASE = "https://example.test/docverse/api"


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _make_tarball(files: dict[str, bytes]) -> bytes:
    """Create a gzipped tarball from a dict of filename -> content."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


async def _setup_org_and_project(
    db_session: AsyncSession,
) -> tuple[Any, Any]:
    """Create an org and project for testing."""
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)

    org = await org_store.create(
        OrganizationCreate(
            slug="worker-test-org",
            title="Worker Test Org",
            base_domain="worker-test.example.com",
        )
    )
    # Set publishing_store_label so the worker can resolve an object store
    await db_session.execute(
        update(SqlOrganization)
        .where(SqlOrganization.id == org.id)
        .values(publishing_store_label="mock-store")
    )
    await db_session.flush()
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug="worker-test-proj",
            title="Worker Test Project",
            source_url="https://example.com/example/repo",
        ),
    )
    return org, project


async def _create_build_in_processing(
    db_session: AsyncSession,
    project_id: int,
    *,
    git_ref: str = "main",
) -> Any:
    """Create a build and transition it to processing status."""
    logger = _logger()
    build_store = BuildStore(session=db_session, logger=logger)

    build = await build_store.create(
        project_id=project_id,
        data=BuildCreate(git_ref=git_ref, content_hash=_HASH),
        uploader="testuser",
        project_slug="worker-test-proj",
    )
    await build_store.transition_status(
        build_id=build.id, new_status=BuildStatus.processing
    )
    # Re-fetch to get updated state
    refreshed = await build_store.get_by_id(build.id)
    assert refreshed is not None
    return refreshed


def _mock_create_objectstore(
    mock_store: MockObjectStore,
) -> Any:
    """Return a patched create_objectstore_for_org that returns
    the given mock store.
    """  # noqa: D205

    async def _create(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
    ) -> MockObjectStore:
        return mock_store

    return _create


class _RecordingMockObjectStore(MockObjectStore):
    """``MockObjectStore`` that timestamps every mutating call.

    Tests use this to verify that worker-issued object-store ops happen
    *after* an advisory-lock acquisition, by comparing the recorded
    timestamps against the lock-event timestamps.
    """

    def __init__(self, op_timestamps: list[float]) -> None:
        super().__init__()
        self._op_timestamps = op_timestamps

    async def upload_object(
        self, *, key: str, data: bytes, content_type: str
    ) -> None:
        self._op_timestamps.append(time.monotonic())
        await super().upload_object(
            key=key, data=data, content_type=content_type
        )

    async def download_object(self, *, key: str) -> bytes:
        self._op_timestamps.append(time.monotonic())
        return await super().download_object(key=key)

    async def delete_object(self, *, key: str) -> None:
        self._op_timestamps.append(time.monotonic())
        await super().delete_object(key=key)


@pytest.mark.asyncio
async def test_build_processing_updates_edition(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Build processing auto-creates and updates an edition."""
    logger = _logger()
    mock_store = MockObjectStore()

    # Set up org, project, build, and queue job
    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-job-1",
        )

    # Stage a tarball in the mock object store
    tarball = _make_tarball({"index.html": b"<html>hello</html>"})
    await mock_store.upload_object(
        key=build.staging_key,
        data=tarball,
        content_type="application/gzip",
    )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-job-1",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
    }

    result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "completed"

    # Verify build, edition, and queue job state
    async for session in db_session_dependency():
        async with session.begin():
            build_store = BuildStore(session=session, logger=_logger())
            updated_build = await build_store.get_by_id(build.id)
            assert updated_build is not None
            assert updated_build.status == BuildStatus.completed
            assert updated_build.object_count == 1

            # Verify an edition was auto-created
            edition_store = EditionStore(session=session, logger=_logger())
            edition = await edition_store.get_by_slug(
                project_id=project.id, slug="main"
            )
            assert edition is not None
            assert edition.current_build_id == build.id

            # Verify queue job completed without errors
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get_by_backend_job_id("test-arq-job-1")
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.phase == "complete"
            assert job.progress is not None
            assert job.progress["object_count"] == 1
            assert len(job.progress["editions_updated"]) == 1
            assert job.progress["editions_updated"][0]["slug"] == "main"
            assert job.progress["editions_updated"][0]["action"] == "created"


@pytest.mark.asyncio
async def test_build_processing_publishes_build_processed(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A processed build emits one ``build_processed`` metric event."""
    logger = _logger()
    mock_store = MockObjectStore()
    _manager, events = await build_event_manager(Configuration())

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-metrics",
        )

    file_body = b"<html>hello</html>"
    tarball = _make_tarball({"index.html": file_body})
    await mock_store.upload_object(
        key=build.staging_key,
        data=tarball,
        content_type="application/gzip",
    )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-metrics",
        events=events,
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
    }

    result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "completed"

    publisher = events.build_processed
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == org.slug
    assert event.project == project.slug
    assert event.success is True
    assert event.object_count == 1
    assert event.total_size_bytes == len(file_body)
    assert event.editions_updated == 1
    assert event.editions_skipped == 0
    assert event.stale_skipped is False
    assert event.elapsed >= timedelta(0)


@pytest.mark.asyncio
async def test_build_processing_uses_stored_storage_prefix(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Build processing uploads files under build.storage_prefix."""
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-prefix",
        )

    tarball = _make_tarball({"index.html": b"<html>hello</html>"})
    await mock_store.upload_object(
        key=build.staging_key,
        data=tarball,
        content_type="application/gzip",
    )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-prefix",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
    }

    result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "completed"

    # Verify the uploaded key uses storage_prefix from the build
    expected_key = f"{build.storage_prefix}index.html"
    assert expected_key in mock_store.objects


@pytest.mark.asyncio
async def test_build_processing_edition_failure_no_build_fail(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Edition tracking failure gives completed_with_errors,
    not failed.
    """  # noqa: D205
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-job-2",
        )

    # Stage a tarball
    tarball = _make_tarball({"index.html": b"<html>hello</html>"})
    await mock_store.upload_object(
        key=build.staging_key,
        data=tarball,
        content_type="application/gzip",
    )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    # Monkeypatch edition tracking to raise an exception
    async def _broken_track(
        self: EditionTrackingService,
        build: Any,
    ) -> None:
        msg = "Simulated edition tracking failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(EditionTrackingService, "track_build", _broken_track)

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-job-2",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
    }

    result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()

    # Build still completes successfully
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            build_store = BuildStore(session=session, logger=_logger())
            updated_build = await build_store.get_by_id(build.id)
            assert updated_build is not None
            assert updated_build.status == BuildStatus.completed

            # Queue job should be completed_with_errors
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get_by_backend_job_id("test-arq-job-2")
            assert job is not None
            assert job.status == JobStatus.completed_with_errors
            assert job.progress is not None
            assert job.progress.get("edition_tracking_error") is True


@pytest.mark.asyncio
async def test_build_processing_enqueues_publish_edition(  # noqa: PLR0915
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Build processing spawns a publish_edition child job per updated edition.

    Asserts the enqueued arq job payload, the child QueueJob row, the parent
    progress ``publish_jobs`` mapping, and ``publish_status = "pending"`` on
    the affected edition and history entry.
    """
    logger = _logger()
    mock_store = MockObjectStore()
    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-publish-1",
        )

    tarball = _make_tarball({"index.html": b"<html>hello</html>"})
    await mock_store.upload_object(
        key=build.staging_key,
        data=tarball,
        content_type="application/gzip",
    )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        arq_queue=mock_arq,
        job_id="test-arq-publish-1",
    )
    build_public_id = serialize_base32_id(build.public_id)
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": build_public_id,
    }

    result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "completed"

    # Inspect enqueued arq jobs for publish_edition. They must land under
    # the configured queue name (not arq's default "arq:queue"), so that
    # the worker listening on ``config.arq_queue_name`` actually picks
    # them up.
    assert "arq:queue" not in queue_names(mock_arq)
    publish_arq_jobs = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name=_config.arq_queue_name
    )
    assert len(publish_arq_jobs) == 1
    assert publish_arq_jobs[0].queue_name == _config.arq_queue_name
    pj_payload = publish_arq_jobs[0].kwargs["payload"]
    assert pj_payload["org_id"] == org.id
    assert pj_payload["project_slug"] == project.slug
    assert pj_payload["edition_slug"] == "main"
    assert pj_payload["build_id"] == build.id
    assert pj_payload["build_public_id"] == build_public_id
    assert "edition_id" in pj_payload
    assert "queue_job_id" in pj_payload

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            edition = await edition_store.get_by_slug(
                project_id=project.id, slug="main"
            )
            assert edition is not None
            assert edition.publish_status == PublishStatus.pending
            assert pj_payload["edition_id"] == edition.id

            history_store = EditionBuildHistoryStore(
                session=session, logger=_logger()
            )
            history = await history_store.get_by_edition_and_build(
                edition_id=edition.id, build_id=build.id
            )
            assert history is not None
            assert history.publish_status == PublishStatus.pending

            qjs = QueueJobStore(session=session, logger=_logger())
            child = await qjs.get(pj_payload["queue_job_id"])
            assert child is not None
            assert child.kind == JobKind.publish_edition
            assert child.edition_id == edition.id
            assert child.build_id == build.id
            assert child.org_id == org.id
            assert child.project_id == project.id
            assert child.backend_job_id == publish_arq_jobs[0].id

            parent = await qjs.get_by_backend_job_id("test-arq-publish-1")
            assert parent is not None
            assert parent.progress is not None
            publish_jobs_progress = parent.progress.get("publish_jobs")
            assert publish_jobs_progress is not None
            assert len(publish_jobs_progress) == 1
            entry = publish_jobs_progress[0]
            assert entry["edition_slug"] == "main"
            assert entry["publish_queue_job_public_id"] == serialize_base32_id(
                child.public_id
            )


@pytest.mark.asyncio
async def test_build_processing_embeds_hateoas_urls(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Progress entries carry HATEOAS ``edition_url`` / ``queue_job_url``.

    The autouse ``mock_discovery`` fixture registers the ``docverse``
    internal service, so the worker resolves the API base from Repertoire
    and embeds an absolute edition link on each ``editions_updated`` entry
    and an absolute queue-job link on each ``publish_jobs`` entry.
    """
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-hateoas",
        )

    tarball = _make_tarball({"index.html": b"<html>hello</html>"})
    await mock_store.upload_object(
        key=build.staging_key,
        data=tarball,
        content_type="application/gzip",
    )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-hateoas",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
    }

    result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            parent = await qjs.get_by_backend_job_id("test-arq-hateoas")
            assert parent is not None
            assert parent.progress is not None

            updated = parent.progress["editions_updated"][0]
            assert updated["slug"] == "main"
            assert updated["edition_url"] == edition_url(
                _DISCOVERY_BASE,
                org=org.slug,
                project=project.slug,
                edition="main",
            )

            entry = parent.progress["publish_jobs"][0]
            child_public_id = entry["publish_queue_job_public_id"]
            assert entry["queue_job_url"] == queue_job_url(
                _DISCOVERY_BASE, job=child_public_id
            )


@pytest.mark.asyncio
async def test_build_processing_omits_urls_when_docverse_unregistered(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    mock_discovery: respx.Router,
) -> None:
    """No Docverse Repertoire registration => URL fields omitted, no fail.

    The build still completes and the existing slug/ID fields remain; only
    the HATEOAS ``edition_url`` / ``queue_job_url`` links are dropped.
    """
    # Re-register discovery with no internal ``docverse`` service so the
    # worker's ``url_for_internal("docverse")`` resolves to ``None``.
    mock_discovery.reset()
    register_mock_discovery(mock_discovery, {"services": {"internal": {}}})

    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-nourl",
        )

    tarball = _make_tarball({"index.html": b"<html>hello</html>"})
    await mock_store.upload_object(
        key=build.staging_key,
        data=tarball,
        content_type="application/gzip",
    )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-nourl",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
    }

    result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            parent = await qjs.get_by_backend_job_id("test-arq-nourl")
            assert parent is not None
            assert parent.status == JobStatus.completed
            assert parent.progress is not None

            updated = parent.progress["editions_updated"][0]
            assert updated["slug"] == "main"
            assert "edition_url" not in updated

            entry = parent.progress["publish_jobs"][0]
            assert "publish_queue_job_public_id" in entry
            assert "queue_job_url" not in entry


@pytest.mark.asyncio
async def test_build_processing_skips_url_resolution_when_no_updates(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No updated editions => no Repertoire discovery call; build completes.

    A project ignore rule suppresses edition tracking for the build's git
    ref, so ``tracking_result.updated`` is empty. ``_finalize_success`` must
    not resolve the Docverse API base URL in that case — sparing the
    discovery round-trip and the "unregistered" warning when there is no
    edition link to embed — while the build still completes.
    """
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        # Ignore the build's git ref so edition tracking yields no updates.
        await db_session.execute(
            update(SqlProject)
            .where(SqlProject.id == project.id)
            .values(slug_rewrite_rules=[{"type": "ignore", "glob": "main"}])
        )
        await db_session.flush()
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-noupdate",
        )

    tarball = _make_tarball({"index.html": b"<html>hello</html>"})
    await mock_store.upload_object(
        key=build.staging_key,
        data=tarball,
        content_type="application/gzip",
    )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    # Spy on Repertoire discovery: resolving the Docverse API base must not
    # happen when there are no updated editions to link.
    discovery_services: list[str] = []
    real_url_for_internal = DiscoveryClient.url_for_internal

    async def _spy_url_for_internal(
        self: DiscoveryClient,
        service: str,
        *,
        version: str | None = None,
    ) -> str | None:
        discovery_services.append(service)
        return await real_url_for_internal(self, service, version=version)

    monkeypatch.setattr(
        DiscoveryClient, "url_for_internal", _spy_url_for_internal
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-noupdate",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
    }

    result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "completed"

    # No Docverse discovery lookup happened: nothing was updated, so no
    # HATEOAS link needed resolving (and no "unregistered" warning fired).
    assert "docverse" not in discovery_services

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            parent = await qjs.get_by_backend_job_id("test-arq-noupdate")
            assert parent is not None
            assert parent.status == JobStatus.completed
            assert parent.progress is not None
            # Tracking ran and succeeded but matched nothing to update.
            assert parent.progress.get("editions_updated") == []


@pytest.mark.asyncio
async def test_build_processing_skips_stale_build(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A superseded build skips before any object-store interaction.

    Two builds exist for the same ``(project, git_ref)``. The older
    build is dispatched after the newer one. The stale-build guard
    inside the BUILD_PROCESSING lock detects that the incoming
    ``build_id`` is not the max for the ``(project_id, git_ref)``
    pair, marks the parent ``QueueJob`` ``completed`` with
    ``progress["stale_skipped"] = True`` and the latest id, and
    returns without invoking any uploads or touching edition state.
    """
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        older_build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        newer_build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        # Pre-create the edition with no current_build so we can later
        # assert the pointer was never moved by the stale dispatch.
        edition_store = EditionStore(session=db_session, logger=logger)
        await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug="main",
                title="Main",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=older_build.id,
            backend_job_id="test-arq-stale",
        )

    # Intentionally do NOT stage a tarball: the stale-build guard
    # must short-circuit before any download or upload is attempted.

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-stale",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": older_build.id,
        "build_public_id": serialize_base32_id(older_build.public_id),
    }

    result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "completed"

    # No uploads or downloads occurred — the mock store stayed empty.
    assert mock_store.objects == {}

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get_by_backend_job_id("test-arq-stale")
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.progress is not None
            assert job.progress.get("stale_skipped") is True
            assert job.progress.get("latest_build_id") == newer_build.id

            # The pre-created edition's pointer must be untouched.
            edition_store = EditionStore(session=session, logger=_logger())
            edition = await edition_store.get_by_slug(
                project_id=project.id, slug="main"
            )
            assert edition is not None
            assert edition.current_build_id is None

            # The older build's status was not transitioned by the
            # stale-skip path; it stays in ``processing``.
            build_store = BuildStore(session=session, logger=_logger())
            refreshed_older = await build_store.get_by_id(older_build.id)
            assert refreshed_older is not None
            assert refreshed_older.status == BuildStatus.processing


@pytest.mark.asyncio
async def test_build_processing_publish_enqueue_failure_leaves_db_consistent(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Phase-A DB writes commit per-pair before phase B raises.

    Two editions track ``main``. The first ``publish_edition`` arq enqueue
    raises while running Phase B for edition 1, after that pair's Phase A
    has already committed. Because the publish enqueue helper splits
    Phase A (DB writes) from Phase B (arq enqueue) per ``(edition, build)``
    pair and the helper raises Phase B failures up the loop, edition 1
    has its full Phase-A footprint (``publish_status=pending`` on both
    edition + history, child ``QueueJob`` row present, ``backend_job_id``
    still NULL) and edition 2 is entirely untouched until the next
    reconciliation pass picks it up. This is the failure shape a future
    reconciliation loop has to handle.
    """
    logger = _logger()
    mock_store = MockObjectStore()
    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        edition_store = EditionStore(session=db_session, logger=logger)
        await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug="main",
                title="Main",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )
        await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug="latest",
                title="Latest",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-publish-fail",
        )

    tarball = _make_tarball({"index.html": b"<html>hello</html>"})
    await mock_store.upload_object(
        key=build.staging_key,
        data=tarball,
        content_type="application/gzip",
    )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    async def failing_enqueue(
        self: ArqQueueBackend,
        job_type: str,
        payload: dict[str, Any],
        *,
        queue_name: str | None = None,
    ) -> str:
        msg = "Simulated arq enqueue failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(ArqQueueBackend, "enqueue", failing_enqueue)

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        arq_queue=mock_arq,
        job_id="test-arq-publish-fail",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
    }

    with pytest.raises(RuntimeError, match="Simulated arq enqueue failure"):
        await build_processing(ctx, payload)
    await ctx["http_client"].aclose()

    # No arq publish_edition jobs were successfully enqueued.
    assert (
        count_jobs_by_name(
            mock_arq, "publish_edition", queue_name=_config.arq_queue_name
        )
        == 0
    )

    # The first iteration's Phase A commits before its Phase B raises, so
    # exactly one of the two editions has its publish-pending footprint
    # (and exactly one child QueueJob row) committed. The second edition
    # was never reached by the loop.
    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            history_store = EditionBuildHistoryStore(
                session=session, logger=_logger()
            )

            statuses: list[PublishStatus | None] = []
            history_statuses: list[PublishStatus | None] = []
            for slug in ("main", "latest"):
                edition = await edition_store.get_by_slug(
                    project_id=project.id, slug=slug
                )
                assert edition is not None
                statuses.append(edition.publish_status)

                history = await history_store.get_by_edition_and_build(
                    edition_id=edition.id, build_id=build.id
                )
                history_statuses.append(
                    history.publish_status if history is not None else None
                )

            pending_editions = [
                s for s in statuses if s == PublishStatus.pending
            ]
            assert len(pending_editions) == 1
            pending_histories = [
                s for s in history_statuses if s == PublishStatus.pending
            ]
            assert len(pending_histories) == 1

            child_rows = (
                (
                    await session.execute(
                        select(SqlQueueJob).where(
                            SqlQueueJob.build_id == build.id,
                            SqlQueueJob.kind == JobKind.publish_edition.value,
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(child_rows) == 1
            assert child_rows[0].backend_job_id is None


@pytest.mark.asyncio
async def test_build_processing_acquires_build_lock_before_object_store(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """build_processing acquires BUILD_PROCESSING before any obj-store op.

    Verifies the worker wires the lock to the correct key — the
    integration tests in ``tests/services/locks_integration_test.py``
    already prove the mechanism works, but they do not pin which key
    each worker uses. A spy ``LockService`` records every acquire
    timestamp; a spy ``MockObjectStore`` records every mutating call's
    timestamp. The first BUILD_PROCESSING acquire must precede every
    worker-issued upload/download/delete.
    """
    logger = _logger()
    op_timestamps: list[float] = []
    mock_store = _RecordingMockObjectStore(op_timestamps=op_timestamps)

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-lock-bp",
        )

    tarball = _make_tarball({"index.html": b"<html>hello</html>"})
    await mock_store.upload_object(
        key=build.staging_key,
        data=tarball,
        content_type="application/gzip",
    )
    # Discard the staging-upload bookkeeping so only worker-issued ops
    # are compared against the lock-event timestamps.
    op_timestamps.clear()

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )
    events = install_recording_lock_service(monkeypatch)

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-lock-bp",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
    }

    result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "completed"

    expected = LockKey.for_build_processing(
        org_id=org.id, project_id=project.id, git_ref="main"
    )
    bp_enters = [
        e
        for e in events
        if e.event == "enter"
        and e.lock_key.lock_class == LockClass.BUILD_PROCESSING
    ]
    assert len(bp_enters) == 1
    assert bp_enters[0].lock_key == expected

    assert op_timestamps, "expected at least one worker object-store call"
    bp_enter_ts = bp_enters[0].timestamp
    assert all(ts > bp_enter_ts for ts in op_timestamps)


@pytest.mark.asyncio
async def test_build_processing_nested_lock_sequence(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EDITION_UPDATE acquisitions nest inside the BUILD_PROCESSING block.

    The ``main`` git_ref auto-creates a ``main`` edition and updates its
    pointer via ``EditionTrackingService.set_current_build``, which
    acquires an EDITION_UPDATE lock. The recorded event sequence must
    be ``BUILD_PROCESSING.enter -> EDITION_UPDATE.enter ->
    EDITION_UPDATE.exit -> BUILD_PROCESSING.exit`` so the per-edition
    pointer cannot diverge from the build-level state mid-flight.
    """
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-lock-nested",
        )

    tarball = _make_tarball({"index.html": b"<html>hello</html>"})
    await mock_store.upload_object(
        key=build.staging_key,
        data=tarball,
        content_type="application/gzip",
    )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )
    events = install_recording_lock_service(monkeypatch)

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-lock-nested",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
    }

    result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "completed"

    # Outer brackets: the BUILD_PROCESSING enter must be the very first
    # recorded event and its exit the very last.
    expected_bp = LockKey.for_build_processing(
        org_id=org.id, project_id=project.id, git_ref="main"
    )
    assert len(events) >= 4
    assert events[0].event == "enter"
    assert events[0].lock_key == expected_bp
    assert events[-1].event == "exit"
    assert events[-1].lock_key == expected_bp

    # At least one EDITION_UPDATE acquire/release pair is fully nested
    # inside the BUILD_PROCESSING block. Each pair's enter precedes its
    # exit, and both indices fall strictly between the outer brackets.
    inner = events[1:-1]
    eu_enter_idx = [
        i
        for i, e in enumerate(inner)
        if e.event == "enter"
        and e.lock_key.lock_class == LockClass.EDITION_UPDATE
    ]
    eu_exit_idx = [
        i
        for i, e in enumerate(inner)
        if e.event == "exit"
        and e.lock_key.lock_class == LockClass.EDITION_UPDATE
    ]
    assert len(eu_enter_idx) >= 1
    assert len(eu_enter_idx) == len(eu_exit_idx)
    for enter_i, exit_i in zip(eu_enter_idx, eu_exit_idx, strict=True):
        assert enter_i < exit_i
        assert inner[enter_i].lock_key == inner[exit_i].lock_key

    # No BUILD_PROCESSING events fire inside the outer brackets — the
    # outer lock is taken once and released once.
    inner_bp = [
        e for e in inner if e.lock_key.lock_class == LockClass.BUILD_PROCESSING
    ]
    assert inner_bp == []
