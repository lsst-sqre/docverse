"""Integration tests for the build_processing worker function."""

from __future__ import annotations

import asyncio
import hashlib
import io
import tarfile
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from types import TracebackType
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
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.models.queue_enums import PublishStatus
from docverse_server.config import Configuration
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.dbschema.project import SqlProject
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.api_urls import edition_url, job_url
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.content_hash import (
    EMPTY_MANIFEST_HASH,
    hash_manifest_pairs,
)
from docverse_server.domain.queue import JobKind, JobStatus
from docverse_server.factory import Factory
from docverse_server.metrics import build_event_manager
from docverse_server.services.edition_tracking import EditionTrackingService
from docverse_server.services.lock_service import LockClass, LockKey
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.objectstore import MockObjectStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_backend import ArqQueueBackend
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.build_processing import build_processing
from docverse_server.worker.functions.build_processing_reaper import (
    build_processing_reaper,
)
from tests.support.arq_testing import (
    count_jobs_by_name,
    get_jobs_by_name,
    queue_names,
)
from tests.support.lock_service_spy import install_recording_lock_service
from tests.support.rowlocks import backend_pid, wait_until_blocked_or_finished
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
    """

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
async def test_build_processing_stores_manifest_content_hash(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The completed row carries the server-computed manifest hash.

    The client's tarball digest (``_HASH`` on the pending row) is a
    transport check over gzip bytes and cannot match the copier's
    per-file manifest hash, so a completed build has to be re-stamped
    with the identity the server derives from the extracted files. The
    tarball is written with ``./``-prefixed member names, the layout the
    client's ``arcname="."`` produces, so this also exercises the
    normalization end to end.
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
            backend_job_id="test-arq-content-hash",
        )

    files = {
        "./index.html": b"<html>hello</html>",
        "./_static/app.css": b"body { margin: 0; }",
    }
    expected_hash = hash_manifest_pairs(
        (name, hashlib.sha256(data).hexdigest())
        for name, data in files.items()
    )
    await mock_store.upload_object(
        key=build.staging_key,
        data=_make_tarball(files),
        content_type="application/gzip",
    )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-content-hash",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
    }

    with capture_logs() as captured:
        result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            build_store = BuildStore(session=session, logger=_logger())
            updated_build = await build_store.get_by_id(build.id)
            assert updated_build is not None
            assert updated_build.status == BuildStatus.completed
            assert updated_build.content_hash == expected_hash
            assert updated_build.content_hash != _HASH

    # The hash is reported alongside the counts it was derived from, so
    # a triager reading the upload log can match a build to its content
    # without a database lookup.
    uploads = [
        event for event in captured if event["event"] == "Upload complete"
    ]
    assert len(uploads) == 1
    assert uploads[0]["content_hash"] == expected_hash


@pytest.mark.asyncio
async def test_build_processing_empty_tarball_stores_empty_manifest_hash(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tarball with no files completes with ``EMPTY_MANIFEST_HASH``.

    Keeper-sync's copier already reports that constant for an empty
    source prefix, so agreeing here keeps the convergence property
    honest for empty content instead of leaving two builds that hold
    nothing looking like different content.
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
            backend_job_id="test-arq-empty-tarball",
        )

    await mock_store.upload_object(
        key=build.staging_key,
        data=_make_tarball({}),
        content_type="application/gzip",
    )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-empty-tarball",
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
            build_store = BuildStore(session=session, logger=_logger())
            updated_build = await build_store.get_by_id(build.id)
            assert updated_build is not None
            assert updated_build.status == BuildStatus.completed
            assert updated_build.object_count == 0
            assert updated_build.content_hash == EMPTY_MANIFEST_HASH


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
async def test_build_processing_enqueues_publish_edition(
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
    """Progress entries carry HATEOAS ``edition_url`` / ``job_url``.

    The autouse ``mock_discovery`` fixture registers the ``docverse``
    internal service, so the worker resolves the API base from Repertoire
    and embeds an absolute edition link on each ``editions_updated`` entry
    and an absolute job link on each ``publish_jobs`` entry.
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
            assert entry["job_url"] == job_url(
                _DISCOVERY_BASE, org=org.slug, job=child_public_id
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
    the HATEOAS ``edition_url`` / ``job_url`` links are dropped.
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
            assert "job_url" not in entry


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

    This is the *started*-then-stale skip: the row was picked up
    normally, so it keeps reporting a stale-skipped success metric (the
    contrast case for the reaped skip below).
    """
    logger = _logger()
    mock_store = MockObjectStore()
    _manager, events = await build_event_manager(Configuration())

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
        events=events,
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

    # The run is reported as a stale-skipped success.
    publisher = events.build_processed
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    assert publisher.published[0].success is True
    assert publisher.published[0].stale_skipped is True

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

            # The stale-skip path transitions the build in the same
            # transaction that completes its queue job, so the row lands
            # on the terminal ``superseded`` rather than being stranded
            # in ``processing`` with no worker on it (#575).
            build_store = BuildStore(session=session, logger=_logger())
            refreshed_older = await build_store.get_by_id(older_build.id)
            assert refreshed_older is not None
            assert refreshed_older.status == BuildStatus.superseded
            assert refreshed_older.date_completed is not None


@pytest.mark.asyncio
async def test_build_processing_deleted_superseder_publishes(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A soft-deleted newer build no longer orphans its ref (#575).

    The reproduction from #575: build A is created, build B is created
    for the same ``(project, git_ref)``, B is deleted before either is
    processed, and only then is A uploaded and signalled. While the
    supersession lookup counted deleted rows, A saw B's higher id, skipped
    itself, and the ref was left with no live build at all — nothing would
    ever publish it. With the lookup restricted to live rows, A observes
    itself as the latest build for the ref and processes normally.
    """
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build_a = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        build_store = BuildStore(session=db_session, logger=logger)
        build_b = await build_store.create(
            project_id=project.id,
            data=BuildCreate(git_ref="main", content_hash=_HASH),
            uploader="testuser",
            project_slug=project.slug,
        )
        # B is the newer build by id — the supersession marker that used
        # to strand A — and is deleted before anything processes it.
        assert build_b.id > build_a.id
        assert await build_store.soft_delete(build_id=build_b.id) is True
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build_a.id,
            backend_job_id="test-arq-deleted-superseder",
        )

    page = b"<html>hello</html>"
    tarball = _make_tarball({"index.html": page})
    await mock_store.upload_object(
        key=build_a.staging_key,
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
        job_id="test-arq-deleted-superseder",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build_a.id,
        "build_public_id": serialize_base32_id(build_a.public_id),
    }

    result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "completed"

    # The build's files really landed in the serving store: the skip
    # path would have left the mock store holding only the staged
    # tarball it was primed with.
    assert f"{build_a.storage_prefix}index.html" in mock_store.objects

    async for session in db_session_dependency():
        async with session.begin():
            build_store = BuildStore(session=session, logger=_logger())
            processed = await build_store.get_by_id(build_a.id)
            assert processed is not None
            assert processed.status == BuildStatus.completed
            assert processed.object_count == 1
            # A real manifest hash, not the client's transport digest.
            expected_hash = hash_manifest_pairs(
                [("index.html", hashlib.sha256(page).hexdigest())]
            )
            assert processed.content_hash == expected_hash

            edition_store = EditionStore(session=session, logger=_logger())
            edition = await edition_store.get_by_slug(
                project_id=project.id, slug="main"
            )
            assert edition is not None
            assert edition.current_build_id == build_a.id


@pytest.mark.asyncio
async def test_build_processing_skips_deleted_build(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A build deleted before its job ran is cancelled, not processed.

    The build was uploaded and signalled ``uploaded``, then deleted
    before the worker picked the job up. Inside the BUILD_PROCESSING
    lock the guard re-reads the row, sees ``date_deleted``, and retires
    the build instead of running it: nothing is downloaded, unpacked or
    uploaded, the queue job completes carrying ``deleted_skipped``, and
    the build lands on the terminal ``cancelled`` rather than being
    stranded in ``processing`` with no worker on it (#575).
    """
    logger = _logger()
    mock_store = MockObjectStore()
    _manager, events = await build_event_manager(Configuration())

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        # Pre-create the edition with no current_build so we can later
        # assert the pointer was never moved by the deleted dispatch.
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
        # Delete the row at the store level so it is still ``processing``:
        # the shape a DELETE that raced the worker leaves behind, and the
        # shape rows deleted before #580 already have.
        build_store = BuildStore(session=db_session, logger=logger)
        assert await build_store.soft_delete(build_id=build.id) is True
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-deleted",
        )

    # Intentionally do NOT stage a tarball: the deleted-self guard must
    # short-circuit before any download or upload is attempted.

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-deleted",
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

    # The deleted skip reuses the stale-skip metric shape: the run did
    # no work, but it ended deliberately rather than in error.
    publisher = events.build_processed
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    assert publisher.published[0].success is True
    assert publisher.published[0].stale_skipped is True

    # No uploads or downloads occurred — the mock store stayed empty.
    assert mock_store.objects == {}

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get_by_backend_job_id("test-arq-deleted")
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.progress is not None
            assert job.progress.get("deleted_skipped") is True
            assert job.progress.get("stale_skipped") is None

            build_store = BuildStore(session=session, logger=_logger())
            refreshed = await build_store.get_by_id(build.id)
            assert refreshed is not None
            assert refreshed.status == BuildStatus.cancelled
            assert refreshed.date_completed is not None

            # The pre-created edition's pointer must be untouched.
            edition_store = EditionStore(session=session, logger=_logger())
            edition = await edition_store.get_by_slug(
                project_id=project.id, slug="main"
            )
            assert edition is not None
            assert edition.current_build_id is None


@pytest.mark.asyncio
async def test_build_processing_deleted_build_already_cancelled(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DELETE that already cancelled the row is a no-op for the guard.

    ``BuildService.soft_delete`` cancels a non-terminal build as it
    deletes it (#580), so by the time the worker's guard runs the row is
    usually *already* ``cancelled``. Re-cancelling a terminal row would
    normally raise ``InvalidBuildStateError``; the guard's cancel is
    idempotent for exactly this ordering, so the job still completes.
    """
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        # DELETE got there first: the row is cancelled *and* deleted.
        build_store = BuildStore(session=db_session, logger=logger)
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.cancelled
        )
        assert await build_store.soft_delete(build_id=build.id) is True
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-deleted-cancelled",
        )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-deleted-cancelled",
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
    assert mock_store.objects == {}

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get_by_backend_job_id("test-arq-deleted-cancelled")
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.progress is not None
            assert job.progress.get("deleted_skipped") is True

            build_store = BuildStore(session=session, logger=_logger())
            refreshed = await build_store.get_by_id(build.id)
            assert refreshed is not None
            assert refreshed.status == BuildStatus.cancelled


async def _cancel_and_soft_delete(build_id: int) -> None:
    """Cancel and soft-delete a build on its own committed session.

    The state a DELETE leaves behind — ``BuildService.soft_delete``
    cancels an unfinished row before stamping ``date_deleted`` —
    committed independently of whatever transaction the caller is
    inside, which is what makes it visible to a worker mid-run.
    """
    async for session in db_session_dependency():
        async with session.begin():
            store = BuildStore(session=session, logger=_logger())
            await store.transition_status(
                build_id=build_id, new_status=BuildStatus.cancelled
            )
            assert await store.soft_delete(build_id=build_id) is True
        break


class _DeletingMockObjectStore(MockObjectStore):
    """``MockObjectStore`` that deletes the build during the first upload.

    Stands in for a DELETE landing while the worker is streaming files
    out. The handler does not take the BUILD_PROCESSING lock, so nothing
    stops it; the worker keeps uploading and only finds out when it goes
    to write the terminal transition. Interception starts at
    :meth:`arm`, so the test can stage its own tarball through the same
    store first.

    With ``fail_after`` the armed upload also raises once the delete has
    committed, driving the worker's error path over an already-terminal
    row.
    """

    def __init__(self, build_id: int, *, fail_after: bool = False) -> None:
        super().__init__()
        self._build_id = build_id
        self._fail_after = fail_after
        self._armed = False

    def arm(self) -> None:
        """Delete the build on the next upload."""
        self._armed = True

    async def upload_object(
        self, *, key: str, data: bytes, content_type: str
    ) -> None:
        if not self._armed:
            await super().upload_object(
                key=key, data=data, content_type=content_type
            )
            return
        self._armed = False
        await _cancel_and_soft_delete(self._build_id)
        await super().upload_object(
            key=key, data=data, content_type=content_type
        )
        if self._fail_after:
            msg = "Object store went away"
            raise RuntimeError(msg)


@pytest.mark.asyncio
async def test_build_processing_build_deleted_mid_upload(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DELETE landing mid-upload retires the job instead of stranding it.

    The deleted-self guard only runs before the uploads start, so a
    DELETE that lands after it leaves the worker holding a row that is
    already ``cancelled``. Completing it would both publish a build the
    operator asked us to drop and raise ``InvalidBuildStateError`` inside
    the transaction that still has to close the queue job out, leaving
    the job ``in_progress`` until the silent reaper (#575). Instead the
    worker re-reads the row before writing anything terminal, skips the
    completion and edition tracking, and completes the job carrying
    ``deleted_skipped``.
    """
    logger = _logger()
    _manager, events = await build_event_manager(Configuration())

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        # Pre-create the edition with no current_build so we can assert
        # the pointer was never moved onto the cancelled build.
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
            build_id=build.id,
            backend_job_id="test-arq-deleted-mid-upload",
        )

    mock_store = _DeletingMockObjectStore(build.id)
    tarball = _make_tarball({"index.html": b"<html>hello</html>"})
    await mock_store.upload_object(
        key=build.staging_key,
        data=tarball,
        content_type="application/gzip",
    )
    mock_store.arm()

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-deleted-mid-upload",
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

    # The same metric shape as the pre-work deleted skip: deliberate,
    # not a failure.
    publisher = events.build_processed
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    assert publisher.published[0].success is True
    assert publisher.published[0].stale_skipped is True

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get_by_backend_job_id(
                "test-arq-deleted-mid-upload"
            )
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.progress is not None
            assert job.progress.get("deleted_skipped") is True

            build_store = BuildStore(session=session, logger=_logger())
            refreshed = await build_store.get_by_id(build.id)
            assert refreshed is not None
            # The DELETE's status stands, and the inventory a completion
            # would have written never landed.
            assert refreshed.status == BuildStatus.cancelled
            assert refreshed.date_deleted is not None
            assert refreshed.object_count is None

            # The edition pointer was never moved onto the dead build.
            edition_store = EditionStore(session=session, logger=_logger())
            edition = await edition_store.get_by_slug(
                project_id=project.id, slug="main"
            )
            assert edition is not None
            assert edition.current_build_id is None
        break


async def _soft_delete_only(build_id: int) -> None:
    """Stamp ``date_deleted`` on a build without touching its status.

    Reproduces one half of review finding f1: the DELETE's UPDATE won the
    row lock while the build was still ``processing``, so the worker's
    later ``completed`` UPDATE overwrote the ``cancelled`` it had
    written. What survives is a row that is both ``completed`` and
    soft-deleted, which ``BuildStore.get_by_id`` happily returns.
    """
    async for session in db_session_dependency():
        async with session.begin():
            store = BuildStore(session=session, logger=_logger())
            assert await store.soft_delete(build_id=build_id) is True
        break


async def _force_status(build_id: int, status: BuildStatus) -> None:
    """Write a status straight onto a build row, bypassing the guard.

    The other ordering of the same race: the DELETE read the row while it
    was still ``processing``, validated ``processing -> cancelled``, and
    wrote ``cancelled`` over the ``completed`` the worker had just
    committed. ``BuildStore.transition_status`` refuses that transition
    from ``completed``, so the test writes the row directly. Closing the
    read-then-write window itself is the sibling f1b task; this task only
    stops the consequence.
    """
    async for session in db_session_dependency():
        async with session.begin():
            await session.execute(
                update(SqlBuild)
                .where(SqlBuild.id == build_id)
                .values(status=status)
            )
        break


class _PostCompletionMockObjectStore(MockObjectStore):
    """``MockObjectStore`` that fires a callback once the worker releases it.

    ``_process_build`` runs inside ``async with object_store,
    session.begin()``, and the inner context manager exits first, so the
    session has already committed the build's ``completed`` transition by
    the time this store's ``__aexit__`` runs — and ``_track_editions``
    re-reads the build only afterwards. Awaiting the injected callback
    here therefore lands a competing committed write in exactly the
    window review finding f1 describes.
    """

    def __init__(self, on_release: Callable[[], Awaitable[None]]) -> None:
        super().__init__()
        self._on_release = on_release

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await super().__aexit__(exc_type, exc_val, exc_tb)
        await self._on_release()


@pytest.mark.asyncio
async def test_build_processing_skips_tracking_for_deleted_build(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DELETE landing after completion keeps the edition pointer put.

    ``BuildStore.transition_status`` is a plain read-then-write, so a
    DELETE racing the worker can leave a row that is both ``completed``
    and soft-deleted. ``_track_editions`` re-reads through
    ``get_by_id``, which does not filter ``date_deleted``, so without a
    guard it would move the edition onto a build the operator asked us to
    drop and enqueue a ``publish_edition`` job for it. Tracking must skip
    instead, while the queue job still completes normally.
    """
    logger = _logger()
    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        # Pre-create the edition with no current_build so the assertion
        # below distinguishes "never moved" from "moved and moved back".
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
            build_id=build.id,
            backend_job_id="test-arq-deleted-after-completion",
        )

    build_id = build.id

    async def _delete_after_completion() -> None:
        await _soft_delete_only(build_id)

    mock_store = _PostCompletionMockObjectStore(_delete_after_completion)
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
        job_id="test-arq-deleted-after-completion",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
    }

    with capture_logs() as captured:
        result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "completed"

    skips = [
        event
        for event in captured
        if event["event"] == "Skipping edition tracking for a retired build"
    ]
    assert len(skips) == 1
    assert skips[0]["build_id"] == build.id
    assert skips[0]["build_status"] == BuildStatus.completed.value
    assert skips[0]["deleted"] is True

    # Nothing to publish: the build will never be served.
    assert (
        count_jobs_by_name(
            mock_arq, "publish_edition", queue_name=_config.arq_queue_name
        )
        == 0
    )

    async for session in db_session_dependency():
        async with session.begin():
            # The edition pointer was never moved onto the dead build.
            edition_store = EditionStore(session=session, logger=_logger())
            edition = await edition_store.get_by_slug(
                project_id=project.id, slug="main"
            )
            assert edition is not None
            assert edition.current_build_id is None

            # The job still closes out cleanly — the skip is not an error.
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get_by_backend_job_id(
                "test-arq-deleted-after-completion"
            )
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.progress is not None
            assert job.progress.get("editions_updated") == []
            assert job.progress.get("edition_tracking_error") is None
        break


@pytest.mark.asyncio
async def test_build_processing_skips_tracking_for_non_completed_build(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A status overwritten out from under the worker also stops tracking.

    The mirror ordering of review finding f1: the racing DELETE's UPDATE
    lands *after* the worker's, leaving the row ``cancelled`` with no
    ``date_deleted`` yet visible. ``_track_editions`` must refuse to
    publish anything but a ``completed`` build, so the status check
    guards this case on its own.
    """
    logger = _logger()
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
            backend_job_id="test-arq-cancelled-after-completion",
        )

    build_id = build.id

    async def _cancel_after_completion() -> None:
        await _force_status(build_id, BuildStatus.cancelled)

    mock_store = _PostCompletionMockObjectStore(_cancel_after_completion)
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
        job_id="test-arq-cancelled-after-completion",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
    }

    with capture_logs() as captured:
        result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "completed"

    skips = [
        event
        for event in captured
        if event["event"] == "Skipping edition tracking for a retired build"
    ]
    assert len(skips) == 1
    assert skips[0]["build_id"] == build.id
    assert skips[0]["build_status"] == BuildStatus.cancelled.value
    assert skips[0]["deleted"] is False

    assert (
        count_jobs_by_name(
            mock_arq, "publish_edition", queue_name=_config.arq_queue_name
        )
        == 0
    )

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            edition = await edition_store.get_by_slug(
                project_id=project.id, slug="main"
            )
            assert edition is not None
            assert edition.current_build_id is None

            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get_by_backend_job_id(
                "test-arq-cancelled-after-completion"
            )
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.progress is not None
            assert job.progress.get("editions_updated") == []
            assert job.progress.get("edition_tracking_error") is None
        break


@pytest.mark.asyncio
async def test_build_processing_failure_after_delete_still_fails_job(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An upload error over an already-cancelled row still closes the job.

    The residual race the mid-upload guard cannot close: the DELETE
    commits and *then* the upload blows up, so the error path runs
    against a row that has already gone terminal. Failing the build
    there is not a legal transition, and letting that raise would abort
    the same transaction that marks the queue job failed — stranding the
    job ``in_progress``. The worker leaves the ``cancelled`` status
    alone and fails the job regardless.
    """
    logger = _logger()

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
            backend_job_id="test-arq-deleted-then-failed",
        )

    mock_store = _DeletingMockObjectStore(build.id, fail_after=True)
    tarball = _make_tarball({"index.html": b"<html>hello</html>"})
    await mock_store.upload_object(
        key=build.staging_key,
        data=tarball,
        content_type="application/gzip",
    )
    mock_store.arm()

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-deleted-then-failed",
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

    assert result == "failed"

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get_by_backend_job_id(
                "test-arq-deleted-then-failed"
            )
            assert job is not None
            # The job is closed out rather than left in_progress for the
            # silent reaper to find eight hours later.
            assert job.status == JobStatus.failed

            build_store = BuildStore(session=session, logger=_logger())
            refreshed = await build_store.get_by_id(build.id)
            assert refreshed is not None
            assert refreshed.status == BuildStatus.cancelled
        break


@pytest.mark.asyncio
async def test_build_processing_deleted_reaped_build_reports_no_success(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reaped delivery for a deleted build writes nothing at all.

    The pickup guard runs before the cancel, exactly as it does on the
    stale path: a row the abandoned sweep already failed is in nobody's
    hands, so this delivery has no build to retire and no metric to
    emit. It reports ``"skipped"`` and leaves the build alone.
    """
    logger = _logger()
    mock_store = MockObjectStore()
    _manager, events = await build_event_manager(Configuration())

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        build_store = BuildStore(session=db_session, logger=logger)
        assert await build_store.soft_delete(build_id=build.id) is True
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
            backend_job_id="test-arq-deleted-reaped",
        )
        # Stand in for the reaper's abandoned sweep having failed the row
        # between enqueue and this (late) delivery.
        await queue_job_store.fail(
            queue_job.id,
            errors={
                "message": "Abandoned build_processing",
                "type": "AbandonedQueueJob",
            },
        )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-deleted-reaped",
        events=events,
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }

    with capture_logs() as captured:
        result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "skipped"
    assert not [
        event
        for event in captured
        if event["event"] == "Deleted build skipped"
    ]

    # No outcome to report: the reaped row emits no build_processed event.
    publisher = events.build_processed
    assert isinstance(publisher, MockEventPublisher)
    assert publisher.published == []

    assert mock_store.objects == {}
    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.failed
            assert job.date_started is None
            assert job.progress is None

            # The build keeps the status the reaped delivery found it in:
            # a delivery that did nothing must not retire the row either.
            build_store = BuildStore(session=session, logger=_logger())
            refreshed = await build_store.get_by_id(build.id)
            assert refreshed is not None
            assert refreshed.status == BuildStatus.processing


@pytest.mark.asyncio
async def test_build_processing_stale_reaped_build_reports_no_success(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reaped row that is *also* stale skips instead of succeeding.

    Task #551: the stale branch used to report ``"completed"`` and a
    ``build_processed(success=True, stale_skipped=True)`` event
    unconditionally, even when the pickup guard refused the row — so a
    job the abandoned sweep had already failed still emitted a success
    for work nobody did. The reaped delivery now matches its sibling
    pickup path: ``"skipped"``, no metric, terminal row untouched.
    """
    logger = _logger()
    mock_store = MockObjectStore()
    _manager, events = await build_event_manager(Configuration())

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        older_build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=older_build.id,
            backend_job_id="test-arq-stale-reaped",
        )
        # Stand in for the reaper's abandoned sweep having failed the row
        # between enqueue and this (late) delivery.
        await queue_job_store.fail(
            queue_job.id,
            errors={
                "message": "Abandoned build_processing",
                "type": "AbandonedQueueJob",
            },
        )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-stale-reaped",
        events=events,
    )
    queue_job_public_id = serialize_base32_id(queue_job.public_id)
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": older_build.id,
        "build_public_id": serialize_base32_id(older_build.public_id),
        "queue_job_id": queue_job.id,
        "queue_job_public_id": queue_job_public_id,
    }

    with capture_logs() as captured:
        result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "skipped"

    # The pickup guard's skip warning is the only thing logged about
    # this delivery; nothing claimed the build was stale-skipped.
    warnings = [
        event
        for event in captured
        if event.get("log_level") == "warning"
        and event.get("queue_job_status") == JobStatus.failed.value
    ]
    assert len(warnings) == 1
    assert warnings[0]["queue_job_id"] == queue_job_public_id
    assert not [
        event for event in captured if event["event"] == "Stale build skipped"
    ]

    # No outcome to report: the reaped row emits no build_processed event.
    publisher = events.build_processed
    assert isinstance(publisher, MockEventPublisher)
    assert publisher.published == []

    # No uploads, and the terminal row keeps its reaped bookkeeping.
    assert mock_store.objects == {}
    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.failed
            assert job.date_started is None
            assert job.phase is None
            assert job.progress is None


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


@pytest.mark.asyncio
async def test_build_processing_drives_row_not_yet_stamped(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The delivery-time row shape — queued, no ``backend_job_id`` — is driven.

    Since task #550 the arq enqueue happens *after* the ``queue_jobs``
    row commits, so at the moment a worker is handed the job the row
    exists but has not been stamped with its ``backend_job_id`` yet.
    Resolving the row from ``payload["queue_job_id"]`` is what keeps the
    worker from falling through its "no row at all" tolerance, processing
    the build anyway, and leaving a forever-``queued`` row behind.
    """
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build = await _create_build_in_processing(
            db_session, project.id, git_ref="main"
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=org.id,
            project_id=project.id,
            build_id=build.id,
        )
    assert queue_job.backend_job_id is None

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

    # ``job_id`` is the arq id the dispatcher is about to stamp on the
    # row; no row carries it yet, so a backend_job_id lookup would miss.
    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-arq-unstamped",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_slug": project.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }

    result = await build_processing(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.phase == "complete"


@pytest.mark.asyncio
async def test_worker_delivery_racing_the_commit_cannot_be_reaped(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A delivery that beats the request still leaves a terminal row.

    Drives the whole race rather than narrowing it: the worker runs from
    *inside* ``QueueBackend.enqueue``, the earliest instant arq could
    hand the job to anyone. Because the enqueue is deferred until after
    the request transaction commits, that worker sees both the build and
    its ``queue_jobs`` row and drives the row to ``completed``.

    A pre-fix enqueue — issued before the row was written and from
    inside the uncommitted transaction — left the worker with nothing to
    find, so the row stayed ``queued`` with a ``backend_job_id`` and the
    reaper's abandoned sweep eventually stamped a *succeeded* build's job
    ``AbandonedQueueJob``. The final reaper tick here, run against a
    backend with no record of the job, is that regression guard.
    """
    logger = _logger()
    mock_store = MockObjectStore()
    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        build_store = BuildStore(session=db_session, logger=logger)
        build = await build_store.create(
            project_id=project.id,
            data=BuildCreate(git_ref="main", content_hash=_HASH),
            uploader="testuser",
            project_slug=project.slug,
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

    delivered: list[str] = []
    original_enqueue = ArqQueueBackend.enqueue

    async def enqueue_and_deliver(
        self: ArqQueueBackend,
        job_type: str,
        payload: dict[str, Any],
        *,
        queue_name: str | None = None,
    ) -> Any:
        enqueued = await original_enqueue(
            self, job_type, payload, queue_name=queue_name
        )
        if job_type == "build_processing":
            worker_client = httpx.AsyncClient()
            worker_ctx = make_worker_ctx(
                http_client=worker_client, job_id=enqueued.id
            )
            try:
                delivered.append(await build_processing(worker_ctx, payload))
            finally:
                await worker_client.aclose()
        return enqueued

    monkeypatch.setattr(ArqQueueBackend, "enqueue", enqueue_and_deliver)

    factory = Factory(
        session=db_session,
        logger=logger,
        arq_queue=mock_arq,
        default_queue_name=_config.arq_queue_name,
    )
    # Stand in for the PATCH handler: the service body runs inside the
    # request transaction, the handler commits, and only then does the
    # dispatcher hand anything to arq.
    async with db_session.begin():
        service = factory.create_build_service()
        _, queue_job = await service.signal_upload_complete(
            org_slug=org.slug,
            project_slug=project.slug,
            build_id=serialize_base32_id(build.public_id),
        )
        await db_session.commit()
    await factory.queue_dispatcher.dispatch()

    assert delivered == ["completed"]

    # Age the row well past the build_processing threshold so only its
    # terminal status keeps the abandoned sweep off it.
    async for session in db_session_dependency():
        async with session.begin():
            await session.execute(
                update(SqlQueueJob)
                .where(SqlQueueJob.id == queue_job.id)
                .values(date_created=datetime.now(tz=UTC) - timedelta(days=1))
            )
        break

    # A reaper whose backend has no record of the arq job at all — the
    # exact condition the abandoned sweep fails a queued row on.
    reaper_client = httpx.AsyncClient()
    reaper_ctx = make_worker_ctx(http_client=reaper_client)
    try:
        assert await build_processing_reaper(reaper_ctx) == "completed"
    finally:
        await reaper_client.aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.errors is None


@pytest.mark.asyncio
async def test_build_processing_holds_the_build_across_its_completion(
    app: None,
    db_session: AsyncSession,
    db_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DELETE arriving after the guard waits for the completion.

    The worker's mid-upload guard and the completion it protects are two
    statements in one transaction. While the guard read was unlocked, a
    DELETE could commit its ``cancelled`` in between: the guard saw
    ``processing`` and let the worker through, and the completion then
    raised ``InvalidBuildStateError`` inside the very transaction that
    still had to close the queue job out — the #575 failure mode, from
    the other direction (review of PR #583, finding f1).

    Reading the row ``FOR UPDATE`` closes that window. The DELETE parks
    on the lock until the worker commits, then makes its own decision on
    what the worker actually wrote: the build keeps ``completed``, the
    queue job is completed rather than failed, and no ``cancelled`` is
    lost under a later ``completed``.

    The DELETE is driven for real, on its own committed session, from a
    hook between the guard and the completion — the exact window under
    test.
    """
    logger = _logger()
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
            backend_job_id="test-arq-delete-after-guard",
        )

    mock_store = MockObjectStore()
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

    original_update_inventory = BuildStore.update_inventory
    parked: bool | None = None

    async with (
        db_session_factory() as delete_session,
        db_session_factory() as probe,
    ):
        delete_pid = await backend_pid(delete_session)

        async def run_delete() -> None:
            store = BuildStore(session=delete_session, logger=_logger())
            existing = await store.get_for_update(build_id=build.id)
            assert existing is not None
            if existing.status in (
                BuildStatus.pending,
                BuildStatus.processing,
            ):
                await store.transition_status(
                    build_id=build.id, new_status=BuildStatus.cancelled
                )
            assert await store.soft_delete(build_id=build.id) is True
            await delete_session.commit()

        deleting: asyncio.Future[None] | None = None

        async def racing_update_inventory(
            self: BuildStore, **kwargs: Any
        ) -> Any:
            nonlocal deleting, parked
            if deleting is None:
                deleting = asyncio.ensure_future(run_delete())
                parked = await wait_until_blocked_or_finished(
                    probe, pid=delete_pid, task=deleting
                )
            return await original_update_inventory(self, **kwargs)

        monkeypatch.setattr(
            BuildStore, "update_inventory", racing_update_inventory
        )

        ctx = make_worker_ctx(
            http_client=httpx.AsyncClient(),
            job_id="test-arq-delete-after-guard",
            events=events,
        )
        payload: dict[str, Any] = {
            "org_id": org.id,
            "org_slug": org.slug,
            "project_slug": project.slug,
            "build_id": build.id,
            "build_public_id": serialize_base32_id(build.public_id),
        }
        try:
            result = await build_processing(ctx, payload)
        finally:
            await ctx["http_client"].aclose()
            if deleting is not None:
                await deleting
            await delete_session.rollback()

    # The DELETE really did have to wait for the worker's lock.
    assert parked is True
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get_by_backend_job_id(
                "test-arq-delete-after-guard"
            )
            assert job is not None
            # Not ``failed``: nothing raised inside the completion.
            assert job.status == JobStatus.completed

            store = BuildStore(session=session, logger=_logger())
            refreshed = await store.get_by_id(build.id)
            assert refreshed is not None
            assert refreshed.status == BuildStatus.completed
            assert refreshed.object_count == 1
            # The DELETE still took effect; it just did not rewrite the
            # status the worker had already earned.
            assert refreshed.date_deleted is not None
        break
