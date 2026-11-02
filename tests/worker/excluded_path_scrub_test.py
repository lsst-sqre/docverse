"""Tests for the ``excluded_path_scrub`` worker function.

The scrub walks one project's completed, live builds, lists each build's
objects with their sizes, and reports the excluded cache paths it finds
(``.doctrees/``, ``.buildinfo`` and the rest of
``docverse.excluded_paths``) in its queue job's ``progress``. Deletion
is not wired yet, so every run here must leave the store untouched.
"""

from __future__ import annotations

import importlib
from typing import Any

import httpx
import pytest
import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    ExcludedPathScrubProgress,
    JobKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.api_urls import build_url
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.build import Build
from docverse_server.domain.queue import JobStatus
from docverse_server.factory import Factory
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.objectstore import (
    MockObjectStore,
    ObjectEntry,
    ObjectStoreError,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.excluded_path_scrub import (
    excluded_path_scrub,
)
from tests.worker.conftest import make_worker_ctx

_SCRUB_MODULE = importlib.import_module(
    "docverse_server.worker.functions.excluded_path_scrub"
)
"""The scrub worker's module.

``docverse_server.worker.functions`` re-exports the function under the
module's own name, so attribute access reaches the function instead.
"""

_ORG_SLUG = "scrub-org"
_PROJECT_SLUG = "scrub-proj"

#: Docverse API base the autouse ``mock_discovery`` fixture registers.
_DISCOVERY_BASE = "https://example.test/docverse/api"


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _mock_create_objectstore(store: MockObjectStore) -> Any:
    """Patch ``create_objectstore_for_org`` to hand back one mock store."""

    async def _create(
        self: Factory, *, org_id: int, service_label: str
    ) -> MockObjectStore:
        return store

    return _create


async def _seed_org_and_project(
    db_session: AsyncSession, *, store_label: str | None = "mock-store"
) -> tuple[int, int]:
    """Create an org (with a publishing store label) and one project."""
    org = await OrganizationStore(session=db_session, logger=_logger()).create(
        OrganizationCreate(
            slug=_ORG_SLUG,
            title="Scrub Org",
            base_domain=f"{_ORG_SLUG}.example.com",
        )
    )
    await db_session.execute(
        update(SqlOrganization)
        .where(SqlOrganization.id == org.id)
        .values(publishing_store_label=store_label)
    )
    project = await ProjectStore(session=db_session, logger=_logger()).create(
        org_id=org.id,
        data=ProjectCreate(
            slug=_PROJECT_SLUG,
            title="Scrub Project",
            source_url="https://example.com/example/scrub",
        ),
    )
    return org.id, project.id


async def _seed_build(
    db_session: AsyncSession,
    *,
    project_id: int,
    git_ref: str,
    status: BuildStatus = BuildStatus.completed,
    deleted: bool = False,
) -> Build:
    """Create a build in a chosen status, optionally soft-deleted."""
    build = await BuildStore(session=db_session, logger=_logger()).create(
        project_id=project_id,
        project_slug=_PROJECT_SLUG,
        data=BuildCreate(git_ref=git_ref),
        uploader="testuser",
    )
    values: dict[str, Any] = {"status": status}
    if deleted:
        values["date_deleted"] = func.now()
    await db_session.execute(
        update(SqlBuild).where(SqlBuild.id == build.id).values(**values)
    )
    return build


async def _point_edition(
    db_session: AsyncSession, *, project_id: int, slug: str, build: Build
) -> None:
    """Create a live edition whose current build is ``build``."""
    edition_store = EditionStore(session=db_session, logger=_logger())
    edition = await edition_store.create(
        project_id=project_id,
        data=EditionCreate(
            slug=slug,
            title=f"Edition {slug}",
            kind=EditionKind.draft,
            tracking_mode=TrackingMode.git_ref,
        ),
    )
    await edition_store.set_current_build(
        edition_id=edition.id, build_id=build.id
    )


async def _seed_queue_job(
    db_session: AsyncSession, *, org_id: int, project_id: int, dry_run: bool
) -> int:
    """Create the queued scrub row the endpoint would have written."""
    store = QueueJobStore(session=db_session, logger=_logger())
    queue_job = await store.create(
        kind=JobKind.excluded_path_scrub,
        org_id=org_id,
        project_id=project_id,
        subject_label=_PROJECT_SLUG,
        backend_job_id="test-scrub-job",
    )
    await store.update_progress(
        queue_job.id,
        ExcludedPathScrubProgress(dry_run=dry_run).model_dump(mode="json"),
    )
    return queue_job.id


async def _put(
    store: MockObjectStore, build: Build, files: dict[str, int]
) -> None:
    """Store ``files`` (relative path -> size) under a build's prefix."""
    for path, size in files.items():
        await store.upload_object(
            key=f"{build.storage_prefix}{path}",
            data=b"x" * size,
            content_type="application/octet-stream",
        )


async def _read_queue_job(job_id: int) -> SqlQueueJob:
    async for session in db_session_dependency():
        async with session.begin():
            row = await session.get(SqlQueueJob, job_id)
            assert row is not None
            await session.refresh(row)
            return row
    msg = "No database session available"
    raise RuntimeError(msg)


def _payload(
    *, org_id: int, project_id: int, queue_job_id: int, dry_run: bool = True
) -> dict[str, Any]:
    return {
        "org_id": org_id,
        "org_slug": _ORG_SLUG,
        "project_id": project_id,
        "project_slug": _PROJECT_SLUG,
        "dry_run": dry_run,
        "queue_job_id": queue_job_id,
    }


def _url(build: Build) -> str:
    return build_url(
        _DISCOVERY_BASE,
        org=_ORG_SLUG,
        project=_PROJECT_SLUG,
        build=serialize_base32_id(build.public_id),
    )


@pytest.mark.asyncio
async def test_dry_run_reports_excluded_paths_per_build(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Affected builds get a row; clean, deleted and unfinished ones do not.

    Each row names the build, the live editions serving it, and every
    excluded entry with its object count and bytes, including a cache
    nested inside a sub-site. A clean build is counted as examined
    without a row, and soft-deleted or non-completed builds are never
    examined at all. Nothing is deleted.
    """
    store = MockObjectStore()
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        sphinx = await _seed_build(
            db_session, project_id=project_id, git_ref="sphinx"
        )
        nested = await _seed_build(
            db_session, project_id=project_id, git_ref="nested"
        )
        clean = await _seed_build(
            db_session, project_id=project_id, git_ref="clean"
        )
        deleted = await _seed_build(
            db_session, project_id=project_id, git_ref="deleted", deleted=True
        )
        pending = await _seed_build(
            db_session,
            project_id=project_id,
            git_ref="pending",
            status=BuildStatus.pending,
        )
        failed = await _seed_build(
            db_session,
            project_id=project_id,
            git_ref="failed",
            status=BuildStatus.failed,
        )
        await _point_edition(
            db_session, project_id=project_id, slug="main", build=sphinx
        )
        await _point_edition(
            db_session, project_id=project_id, slug="v1", build=sphinx
        )
        await _point_edition(
            db_session, project_id=project_id, slug="v2", build=nested
        )
        queue_job_id = await _seed_queue_job(
            db_session, org_id=org_id, project_id=project_id, dry_run=True
        )
    await _put(
        store,
        sphinx,
        {
            "index.html": 10,
            ".doctrees/environment.pickle": 900,
            ".doctrees/index.doctree": 100,
            ".buildinfo": 7,
        },
    )
    await _put(
        store,
        nested,
        {
            "index.html": 10,
            "sub/index.html": 10,
            "sub/.doctrees/page.doctree": 30,
            "sub/.buildinfo": 5,
        },
    )
    await _put(store, clean, {"index.html": 10, "_static/app.js": 20})
    for retired in (deleted, pending, failed):
        await _put(store, retired, {".doctrees/environment.pickle": 900})
    before = {key: obj.data for key, obj in store.objects.items()}

    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    result = await excluded_path_scrub(
        ctx,
        _payload(
            org_id=org_id, project_id=project_id, queue_job_id=queue_job_id
        ),
    )

    assert result == "completed"
    assert {key: obj.data for key, obj in store.objects.items()} == before
    row = await _read_queue_job(queue_job_id)
    assert row.status == JobStatus.completed.value
    assert row.progress == {
        "dry_run": True,
        "builds_examined": 3,
        "builds_affected": 2,
        "objects_removed": 5,
        "bytes_removed": 1042,
        "builds_failed": 0,
        "builds": [
            {
                "build_id": serialize_base32_id(sphinx.public_id),
                "build_url": _url(sphinx),
                "edition_slugs": ["main", "v1"],
                "entries": [
                    {
                        "path": ".buildinfo",
                        "object_count": 1,
                        "total_size_bytes": 7,
                    },
                    {
                        "path": ".doctrees/",
                        "object_count": 2,
                        "total_size_bytes": 1000,
                    },
                ],
                "error": None,
            },
            {
                "build_id": serialize_base32_id(nested.public_id),
                "build_url": _url(nested),
                "edition_slugs": ["v2"],
                "entries": [
                    {
                        "path": "sub/.buildinfo",
                        "object_count": 1,
                        "total_size_bytes": 5,
                    },
                    {
                        "path": "sub/.doctrees/",
                        "object_count": 1,
                        "total_size_bytes": 30,
                    },
                ],
                "error": None,
            },
        ],
    }


class _FailingListingStore(MockObjectStore):
    """Mock store whose listing of one prefix fails."""

    def __init__(self, *, failing_prefix: str) -> None:
        super().__init__()
        self._failing_prefix = failing_prefix

    async def list_object_entries(self, *, prefix: str) -> list[ObjectEntry]:
        if prefix == self._failing_prefix:
            raise ObjectStoreError(
                bucket="docs",
                prefix=prefix,
                operation="ListObjectsV2",
                failures=["listing refused"],
            )
        return await super().list_object_entries(prefix=prefix)


@pytest.mark.asyncio
async def test_a_failing_build_is_reported_and_the_scrub_continues(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One build the store refuses gets an error row; the rest still run.

    The job finishes ``completed_with_errors`` rather than ``failed``,
    because the report for every other build is still good.
    """
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        broken = await _seed_build(
            db_session, project_id=project_id, git_ref="broken"
        )
        after = await _seed_build(
            db_session, project_id=project_id, git_ref="after"
        )
        queue_job_id = await _seed_queue_job(
            db_session, org_id=org_id, project_id=project_id, dry_run=True
        )
    store = _FailingListingStore(failing_prefix=broken.storage_prefix)
    await _put(store, after, {".buildinfo": 7})
    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    result = await excluded_path_scrub(
        ctx,
        _payload(
            org_id=org_id, project_id=project_id, queue_job_id=queue_job_id
        ),
    )

    assert result == "completed_with_errors"
    row = await _read_queue_job(queue_job_id)
    assert row.status == JobStatus.completed_with_errors.value
    progress = ExcludedPathScrubProgress.model_validate(row.progress)
    assert progress.builds_examined == 1
    assert progress.builds_affected == 1
    assert progress.builds_failed == 1
    failed_row, affected_row = progress.builds
    assert failed_row.build_id == serialize_base32_id(broken.public_id)
    assert failed_row.entries == []
    assert failed_row.error is not None
    assert failed_row.error.startswith("ObjectStoreError:")
    assert affected_row.build_id == serialize_base32_id(after.public_id)
    assert affected_row.error is None


class _ProbingStore(MockObjectStore):
    """Mock store that reads the job's progress before each listing."""

    def __init__(self) -> None:
        super().__init__()
        self.queue_job_id: int | None = None
        self.seen: list[int] = []

    async def list_object_entries(self, *, prefix: str) -> list[ObjectEntry]:
        assert self.queue_job_id is not None
        row = await _read_queue_job(self.queue_job_id)
        assert row.progress is not None
        self.seen.append(row.progress["builds_examined"])
        return await super().list_object_entries(prefix=prefix)


@pytest.mark.asyncio
async def test_progress_is_written_while_the_scrub_runs(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A long scrub's report can be read before it finishes."""
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        for git_ref in ("one", "two", "three"):
            await _seed_build(
                db_session, project_id=project_id, git_ref=git_ref
            )
        queue_job_id = await _seed_queue_job(
            db_session, org_id=org_id, project_id=project_id, dry_run=True
        )
    store = _ProbingStore()
    store.queue_job_id = queue_job_id
    monkeypatch.setattr(_SCRUB_MODULE, "PROGRESS_FLUSH_BUILDS", 1)
    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    await excluded_path_scrub(
        ctx,
        _payload(
            org_id=org_id, project_id=project_id, queue_job_id=queue_job_id
        ),
    )

    assert store.seen == [0, 1, 2]


@pytest.mark.asyncio
async def test_late_delivery_is_skipped(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row that is no longer queued is left alone."""
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        queue_job_id = await _seed_queue_job(
            db_session, org_id=org_id, project_id=project_id, dry_run=True
        )
        await QueueJobStore(session=db_session, logger=_logger()).fail(
            queue_job_id
        )
    store = MockObjectStore()
    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    result = await excluded_path_scrub(
        ctx,
        _payload(
            org_id=org_id, project_id=project_id, queue_job_id=queue_job_id
        ),
    )

    assert result == "skipped"
    assert (await _read_queue_job(queue_job_id)).status == (
        JobStatus.failed.value
    )


@pytest.mark.asyncio
async def test_org_without_a_store_fails_the_job(
    app: None,
    db_session: AsyncSession,
) -> None:
    """With nowhere to list builds from, the job fails rather than lying."""
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(
            db_session, store_label=None
        )
        await _seed_build(db_session, project_id=project_id, git_ref="main")
        queue_job_id = await _seed_queue_job(
            db_session, org_id=org_id, project_id=project_id, dry_run=True
        )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    with pytest.raises(RuntimeError, match="No object store service"):
        await excluded_path_scrub(
            ctx,
            _payload(
                org_id=org_id, project_id=project_id, queue_job_id=queue_job_id
            ),
        )

    row = await _read_queue_job(queue_job_id)
    assert row.status == JobStatus.failed.value
    assert row.errors is not None
    assert row.errors["type"] == "RuntimeError"
