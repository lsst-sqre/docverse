"""Tests for the ``excluded_path_scrub`` worker function.

The scrub walks one project's completed, live builds, lists each build's
objects with their sizes, and reports the excluded cache paths it finds
(``.doctrees/``, ``.buildinfo`` and the rest of
``docverse.excluded_paths``) in its queue job's ``progress``. A dry run
leaves the store and the build rows untouched; a real run deletes those
objects, records each build's inventory as listed less them and stamps
the build so a later run passes it by.
"""

from __future__ import annotations

import importlib
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import structlog
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher
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
from docverse_server.config import Configuration, config
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.api_urls import build_url
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.build import Build
from docverse_server.domain.queue import JobStatus
from docverse_server.exceptions import InvalidBuildStateError
from docverse_server.factory import Factory
from docverse_server.metrics import build_event_manager
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
from docverse_server.worker.functions.excluded_path_scrub_reaper import (
    excluded_path_scrub_reaper,
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
    files: dict[str, int] | None = None,
) -> Build:
    """Create a build in a chosen status, optionally soft-deleted.

    ``files`` (relative path -> size) sets the build's inventory to what
    unpacking those files would have recorded; `_put` stores them.
    """
    build = await BuildStore(session=db_session, logger=_logger()).create(
        project_id=project_id,
        project_slug=_PROJECT_SLUG,
        data=BuildCreate(git_ref=git_ref),
        uploader="testuser",
    )
    values: dict[str, Any] = {"status": status}
    if files is not None:
        values["object_count"] = len(files)
        values["total_size_bytes"] = sum(files.values())
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


async def _read_build(build_id: int) -> SqlBuild:
    async for session in db_session_dependency():
        async with session.begin():
            row = await session.get(SqlBuild, build_id)
            assert row is not None
            await session.refresh(row)
            return row
    msg = "No database session available"
    raise RuntimeError(msg)


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
        "builds_skipped": 0,
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
                "skip_reason": None,
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
                "skip_reason": None,
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


@pytest.mark.asyncio
async def test_a_build_that_fails_once_listed_still_counts_as_examined(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure after a build's listing is an examined, affected error.

    Only a build whose listing failed goes unexamined. One whose
    edition-slug read fails was listed, so it counts toward
    ``builds_examined`` and ``builds_affected`` as well as
    ``builds_failed``, and its row names the excluded entries the
    listing found while removing nothing.
    """
    store = MockObjectStore()
    org_id, project_id, (broken, after) = await _seed_sphinx_project(
        db_session, store, "broken", "after"
    )
    async with db_session.begin():
        queue_job_id = await _seed_queue_job(
            db_session, org_id=org_id, project_id=project_id, dry_run=True
        )
    real_list_slugs = EditionStore.list_live_slugs_by_current_build

    async def _refuse_broken(self: EditionStore, *, build_id: int) -> Any:
        if build_id == broken.id:
            msg = "edition read refused"
            raise RuntimeError(msg)
        return await real_list_slugs(self, build_id=build_id)

    monkeypatch.setattr(
        EditionStore, "list_live_slugs_by_current_build", _refuse_broken
    )
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
    progress = ExcludedPathScrubProgress.model_validate(
        (await _read_queue_job(queue_job_id)).progress
    )
    assert progress.builds_examined == 2
    assert progress.builds_affected == 2
    assert progress.builds_failed == 1
    assert progress.objects_removed == 3
    failed_row, affected_row = progress.builds
    assert failed_row.build_id == serialize_base32_id(broken.public_id)
    assert failed_row.error == "RuntimeError: edition read refused"
    assert [entry.path for entry in failed_row.entries] == [
        ".buildinfo",
        ".doctrees/",
    ]
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


_SPHINX_FILES = {
    "index.html": 10,
    "_static/app.js": 20,
    ".doctrees/environment.pickle": 900,
    ".doctrees/index.doctree": 100,
    ".buildinfo": 7,
}
"""A Sphinx build's tree: two served files and three cache objects."""

_SPHINX_SURVIVORS = {"index.html", "_static/app.js"}


def _relative_keys(store: MockObjectStore, build: Build) -> set[str]:
    """Return the keys under ``build``'s prefix, relative to it."""
    return {
        key[len(build.storage_prefix) :]
        for key in store.objects
        if key.startswith(build.storage_prefix)
    }


@pytest.mark.asyncio
async def test_real_run_removes_excluded_objects_and_corrects_inventory(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real scrub deletes exactly the excluded keys and stamps the build.

    The served files survive, the build's ``object_count`` and
    ``total_size_bytes`` drop by what was removed, the build carries the
    ``excluded_paths_scrubbed`` stamp recording when and how much, and
    its ``content_hash`` stays the one it was ingested with.
    """
    store = MockObjectStore()
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        sphinx = await _seed_build(
            db_session,
            project_id=project_id,
            git_ref="sphinx",
            files=_SPHINX_FILES,
        )
        queue_job_id = await _seed_queue_job(
            db_session, org_id=org_id, project_id=project_id, dry_run=False
        )
    await _put(store, sphinx, _SPHINX_FILES)
    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    result = await excluded_path_scrub(
        ctx,
        _payload(
            org_id=org_id,
            project_id=project_id,
            queue_job_id=queue_job_id,
            dry_run=False,
        ),
    )

    assert result == "completed"
    assert _relative_keys(store, sphinx) == _SPHINX_SURVIVORS
    row = await _read_build(sphinx.id)
    assert row.object_count == 2
    assert row.total_size_bytes == 30
    assert row.content_hash == sphinx.content_hash
    assert row.annotations is not None
    stamp = row.annotations["excluded_paths_scrubbed"]
    assert stamp["objects_removed"] == 3
    assert stamp["bytes_removed"] == 1007
    assert datetime.fromisoformat(stamp["date_scrubbed"]).tzinfo is not None
    progress = ExcludedPathScrubProgress.model_validate(
        (await _read_queue_job(queue_job_id)).progress
    )
    assert progress.dry_run is False
    assert progress.builds_affected == 1
    assert progress.objects_removed == 3
    assert progress.bytes_removed == 1007
    assert progress.builds_failed == 0


async def _run_scrub(
    store: MockObjectStore,
    monkeypatch: pytest.MonkeyPatch,
    *,
    org_id: int,
    project_id: int,
    dry_run: bool,
) -> tuple[str, ExcludedPathScrubProgress]:
    """Queue and run one scrub of the project; return result and report."""
    async for session in db_session_dependency():
        async with session.begin():
            queue_job_id = await _seed_queue_job(
                session, org_id=org_id, project_id=project_id, dry_run=dry_run
            )
    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    result = await excluded_path_scrub(
        make_worker_ctx(http_client=httpx.AsyncClient()),
        _payload(
            org_id=org_id,
            project_id=project_id,
            queue_job_id=queue_job_id,
            dry_run=dry_run,
        ),
    )
    row = await _read_queue_job(queue_job_id)
    assert row.status == result
    return result, ExcludedPathScrubProgress.model_validate(row.progress)


async def _seed_sphinx_project(
    db_session: AsyncSession, store: MockObjectStore, *git_refs: str
) -> tuple[int, int, list[Build]]:
    """Seed the project with one stored Sphinx build per git ref."""
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        builds = [
            await _seed_build(
                db_session,
                project_id=project_id,
                git_ref=git_ref,
                files=_SPHINX_FILES,
            )
            for git_ref in git_refs
        ]
    for build in builds:
        await _put(store, build, _SPHINX_FILES)
    return org_id, project_id, builds


async def _soft_delete(build_id: int) -> None:
    """Soft-delete a build from outside the scrub's session."""
    async for session in db_session_dependency():
        async with session.begin():
            await session.execute(
                update(SqlBuild)
                .where(SqlBuild.id == build_id)
                .values(date_deleted=func.now())
            )
        return


@pytest.mark.asyncio
async def test_dry_run_leaves_build_rows_untouched(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dry run deletes nothing, writes no build row and stamps nothing.

    So a second dry run finds the same build again.
    """
    store = MockObjectStore()
    org_id, project_id, (sphinx,) = await _seed_sphinx_project(
        db_session, store, "sphinx"
    )
    before = {key: obj.data for key, obj in store.objects.items()}

    for _ in range(2):
        result, progress = await _run_scrub(
            store,
            monkeypatch,
            org_id=org_id,
            project_id=project_id,
            dry_run=True,
        )
        assert result == "completed"
        assert progress.builds_affected == 1
        assert progress.objects_removed == 3

    assert {key: obj.data for key, obj in store.objects.items()} == before
    row = await _read_build(sphinx.id)
    assert row.object_count == len(_SPHINX_FILES)
    assert row.total_size_bytes == sum(_SPHINX_FILES.values())
    assert row.annotations is None


@pytest.mark.asyncio
async def test_second_real_run_reports_no_affected_builds(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stamped builds are passed by, so a re-run has nothing to do."""
    store = MockObjectStore()
    org_id, project_id, _ = await _seed_sphinx_project(
        db_session, store, "one", "two"
    )

    _, first = await _run_scrub(
        store, monkeypatch, org_id=org_id, project_id=project_id, dry_run=False
    )
    result, second = await _run_scrub(
        store, monkeypatch, org_id=org_id, project_id=project_id, dry_run=False
    )

    assert first.builds_affected == 2
    assert result == "completed"
    assert second.builds_examined == 0
    assert second.builds_affected == 0
    assert second.objects_removed == 0
    assert second.builds == []


class _HookedStore(MockObjectStore):
    """Mock store that runs a side effect at a chosen point of the scrub.

    ``on_list`` runs before the listing of a given prefix and
    ``on_delete`` before the first delete under it, each once, so a test
    can retire a build at exactly the moment it means to. ``failing_keys``
    are refused deletes.
    """

    def __init__(self) -> None:
        super().__init__()
        self.on_list: dict[str, Callable[[], Awaitable[None]]] = {}
        self.on_delete: dict[str, Callable[[], Awaitable[None]]] = {}
        self.failing_keys: set[str] = set()

    async def list_object_entries(self, *, prefix: str) -> list[ObjectEntry]:
        hook = self.on_list.pop(prefix, None)
        if hook is not None:
            await hook()
        return await super().list_object_entries(prefix=prefix)

    async def delete_object(self, *, key: str) -> None:
        for prefix in list(self.on_delete):
            if key.startswith(prefix):
                await self.on_delete.pop(prefix)()
        if key in self.failing_keys:
            raise ObjectStoreError(
                bucket="docs",
                prefix=key,
                operation="DeleteObject",
                failures=["delete refused"],
            )
        await super().delete_object(key=key)


@pytest.mark.asyncio
async def test_build_deleted_before_its_objects_go_is_skipped(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A build soft-deleted after the job listed it keeps its objects.

    It is reported as skipped, its row is not written, the other build
    is still scrubbed, and a skip alone is not an error.
    """
    store = _HookedStore()
    org_id, project_id, (retired, live) = await _seed_sphinx_project(
        db_session, store, "retired", "live"
    )
    store.on_list[retired.storage_prefix] = lambda: _soft_delete(retired.id)

    result, progress = await _run_scrub(
        store, monkeypatch, org_id=org_id, project_id=project_id, dry_run=False
    )

    assert result == "completed"
    assert _relative_keys(store, retired) == set(_SPHINX_FILES)
    assert _relative_keys(store, live) == _SPHINX_SURVIVORS
    retired_row = await _read_build(retired.id)
    assert retired_row.object_count == len(_SPHINX_FILES)
    assert retired_row.annotations is None
    assert progress.builds_affected == 2
    assert progress.builds_skipped == 1
    assert progress.builds_failed == 0
    assert progress.objects_removed == 3
    skipped, scrubbed = progress.builds
    assert skipped.build_id == serialize_base32_id(retired.public_id)
    assert skipped.skip_reason is not None
    assert "deleted" in skipped.skip_reason
    assert skipped.error is None
    assert [entry.path for entry in skipped.entries] == [
        ".buildinfo",
        ".doctrees/",
    ]
    assert scrubbed.skip_reason is None


@pytest.mark.asyncio
async def test_build_deleted_while_its_objects_go_is_not_written(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A build retired mid-delete loses its cache but keeps its row as is.

    The locked re-read before the inventory write sees the deletion, so
    neither the counters nor the stamp are written, and the report
    names the build as skipped while counting the objects already gone.
    """
    store = _HookedStore()
    org_id, project_id, (retired,) = await _seed_sphinx_project(
        db_session, store, "retired"
    )
    store.on_delete[retired.storage_prefix] = lambda: _soft_delete(retired.id)

    result, progress = await _run_scrub(
        store, monkeypatch, org_id=org_id, project_id=project_id, dry_run=False
    )

    assert result == "completed"
    assert _relative_keys(store, retired) == _SPHINX_SURVIVORS
    row = await _read_build(retired.id)
    assert row.object_count == len(_SPHINX_FILES)
    assert row.total_size_bytes == sum(_SPHINX_FILES.values())
    assert row.annotations is None
    assert progress.builds_skipped == 1
    assert progress.objects_removed == 3
    assert progress.builds[0].skip_reason is not None


@pytest.mark.asyncio
async def test_a_failed_delete_is_recorded_and_other_builds_are_scrubbed(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One build's refused delete fails that build, not the job.

    The other excluded objects of that build still go and come off its
    inventory, but it is not stamped, so a re-run lists it again and
    finishes it. The build after it is scrubbed as usual, and the job
    ends ``completed_with_errors``.
    """
    store = _HookedStore()
    org_id, project_id, (broken, after) = await _seed_sphinx_project(
        db_session, store, "broken", "after"
    )
    refused = f"{broken.storage_prefix}.doctrees/environment.pickle"
    store.failing_keys.add(refused)

    result, progress = await _run_scrub(
        store, monkeypatch, org_id=org_id, project_id=project_id, dry_run=False
    )

    assert result == "completed_with_errors"
    assert progress.builds_failed == 1
    assert progress.builds_affected == 2
    assert progress.objects_removed == 5
    assert progress.bytes_removed == 107 + 1007
    failed_row, scrubbed_row = progress.builds
    assert failed_row.build_id == serialize_base32_id(broken.public_id)
    assert failed_row.error is not None
    assert "1 of 3 object deletes failed" in failed_row.error
    assert refused in failed_row.error
    assert scrubbed_row.error is None
    assert _relative_keys(store, broken) == {
        *_SPHINX_SURVIVORS,
        ".doctrees/environment.pickle",
    }
    assert _relative_keys(store, after) == _SPHINX_SURVIVORS
    broken_row = await _read_build(broken.id)
    assert broken_row.object_count == 3
    assert broken_row.total_size_bytes == 930
    assert broken_row.annotations is None
    after_row = await _read_build(after.id)
    assert after_row.annotations is not None
    assert "excluded_paths_scrubbed" in after_row.annotations

    store.failing_keys.clear()
    result, rerun = await _run_scrub(
        store, monkeypatch, org_id=org_id, project_id=project_id, dry_run=False
    )

    assert result == "completed"
    assert rerun.builds_examined == 1
    assert rerun.objects_removed == 1
    assert _relative_keys(store, broken) == _SPHINX_SURVIVORS
    broken_row = await _read_build(broken.id)
    assert broken_row.object_count == 2
    assert broken_row.total_size_bytes == 30
    assert broken_row.annotations is not None
    assert (
        broken_row.annotations["excluded_paths_scrubbed"]["objects_removed"]
        == 1
    )


@pytest.mark.asyncio
async def test_a_rerun_heals_an_interrupted_scrubs_unrecorded_deletes(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A re-run writes the inventory the store holds, not a decrement.

    A scrub that died part-way through a build's deletes removed
    ``.buildinfo`` without recording it, so the build's inventory still
    counts it. The re-run lists what is left, deletes the ``.doctrees/``
    objects, and records the build as listed less those: the dead
    scrub's delete comes off the inventory too.
    """
    store = MockObjectStore()
    org_id, project_id, (interrupted,) = await _seed_sphinx_project(
        db_session, store, "interrupted"
    )
    await store.delete_object(key=f"{interrupted.storage_prefix}.buildinfo")
    row = await _read_build(interrupted.id)
    assert row.object_count == len(_SPHINX_FILES)
    assert row.total_size_bytes == sum(_SPHINX_FILES.values())

    result, progress = await _run_scrub(
        store, monkeypatch, org_id=org_id, project_id=project_id, dry_run=False
    )

    assert result == "completed"
    assert progress.objects_removed == 2
    assert progress.bytes_removed == 1000
    assert _relative_keys(store, interrupted) == _SPHINX_SURVIVORS
    row = await _read_build(interrupted.id)
    assert row.object_count == 2
    assert row.total_size_bytes == 30
    assert row.annotations is not None
    assert row.annotations["excluded_paths_scrubbed"]["objects_removed"] == 2


@pytest.mark.asyncio
async def test_a_racing_scrub_corrects_the_inventory_only_once(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A build another scrub finished while this one deleted is left alone.

    Two scrubs of one project can both list and delete the same build;
    deletes are idempotent, and the locked re-read before the write sees
    the other scrub's stamp, so the cache comes off the inventory once.
    """
    store = _HookedStore()
    org_id, project_id, (raced,) = await _seed_sphinx_project(
        db_session, store, "raced"
    )

    async def _other_scrub_finishes() -> None:
        async for session in db_session_dependency():
            async with session.begin():
                build_store = BuildStore(session=session, logger=_logger())
                await build_store.correct_inventory(
                    build_id=raced.id, object_count=2, total_size_bytes=30
                )
                await build_store.set_annotation(
                    build_id=raced.id,
                    key="excluded_paths_scrubbed",
                    value={"objects_removed": 3, "bytes_removed": 1007},
                )
            return

    store.on_delete[raced.storage_prefix] = _other_scrub_finishes

    result, progress = await _run_scrub(
        store, monkeypatch, org_id=org_id, project_id=project_id, dry_run=False
    )

    assert result == "completed"
    assert progress.builds_skipped == 1
    assert progress.builds[0].skip_reason is not None
    assert "already scrubbed" in progress.builds[0].skip_reason
    row = await _read_build(raced.id)
    assert row.object_count == 2
    assert row.total_size_bytes == 30


@pytest.mark.asyncio
async def test_a_rejected_inventory_write_fails_the_build(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An inventory write the database refuses becomes the row's error.

    The objects are already gone, so they are still counted as removed;
    the build is left unstamped for a re-run.
    """
    store = MockObjectStore()
    org_id, project_id, (build,) = await _seed_sphinx_project(
        db_session, store, "rejected"
    )

    async def _reject(self: BuildStore, **kwargs: Any) -> Build:
        raise InvalidBuildStateError(message="inventory write refused")

    monkeypatch.setattr(BuildStore, "correct_inventory", _reject)

    result, progress = await _run_scrub(
        store, monkeypatch, org_id=org_id, project_id=project_id, dry_run=False
    )

    assert result == "completed_with_errors"
    assert progress.builds_failed == 1
    assert progress.objects_removed == 3
    assert progress.builds[0].error == (
        "InvalidBuildStateError: inventory write refused"
    )
    row = await _read_build(build.id)
    assert row.object_count == len(_SPHINX_FILES)
    assert row.annotations is None


async def _seed_running_scrub(
    db_session: AsyncSession,
    *,
    org_id: int,
    project_id: int,
    project_slug: str,
    started_minutes_ago: int,
    progress: ExcludedPathScrubProgress,
) -> int:
    """Create a scrub row a worker picked up ``started_minutes_ago``."""
    store = QueueJobStore(session=db_session, logger=_logger())
    queue_job = await store.create(
        kind=JobKind.excluded_path_scrub,
        org_id=org_id,
        project_id=project_id,
        subject_label=project_slug,
        backend_job_id=f"arq-scrub-{project_slug}",
    )
    await db_session.execute(
        update(SqlQueueJob)
        .where(SqlQueueJob.id == queue_job.id)
        .values(
            status=JobStatus.in_progress.value,
            date_started=(
                datetime.now(tz=UTC) - timedelta(minutes=started_minutes_ago)
            ),
            progress=progress.model_dump(mode="json"),
        )
    )
    return queue_job.id


def _build_state(row: SqlBuild) -> dict[str, Any]:
    """Return the build-level state a reaped scrub must leave alone."""
    return {
        "status": row.status,
        "object_count": row.object_count,
        "total_size_bytes": row.total_size_bytes,
        "content_hash": row.content_hash,
        "annotations": row.annotations,
        "date_deleted": row.date_deleted,
    }


@pytest.mark.asyncio
async def test_reaper_fails_a_dead_scrub_and_leaves_its_builds_alone(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A scrub whose worker died is failed; its builds stay as they were.

    The dead job had scrubbed and stamped one build and not reached the
    other. Reaping the row releases the project for a new scrub without
    touching either build: the stamp, the corrected inventory and the
    untouched build's full inventory all survive, so the next scrub
    resumes with just the unstamped build. The report the job last
    flushed stays readable on the failed row. A scrub of another project
    that started a minute ago is left running.
    """
    threshold = timedelta(
        seconds=config.excluded_path_scrub_reaper_threshold_seconds
    )
    partial = ExcludedPathScrubProgress(
        dry_run=False,
        builds_examined=1,
        builds_affected=1,
        objects_removed=3,
        bytes_removed=1007,
    )
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        scrubbed = await _seed_build(
            db_session,
            project_id=project_id,
            git_ref="scrubbed",
            files=_SPHINX_FILES,
        )
        unreached = await _seed_build(
            db_session,
            project_id=project_id,
            git_ref="unreached",
            files=_SPHINX_FILES,
        )
        build_store = BuildStore(session=db_session, logger=_logger())
        await build_store.correct_inventory(
            build_id=scrubbed.id, object_count=2, total_size_bytes=30
        )
        await build_store.set_annotation(
            build_id=scrubbed.id,
            key="excluded_paths_scrubbed",
            value={"objects_removed": 3, "bytes_removed": 1007},
        )
        other_project = await ProjectStore(
            session=db_session, logger=_logger()
        ).create(
            org_id=org_id,
            data=ProjectCreate(
                slug="scrub-other",
                title="Other Scrub Project",
                source_url="https://example.com/example/scrub-other",
            ),
        )
        dead_id = await _seed_running_scrub(
            db_session,
            org_id=org_id,
            project_id=project_id,
            project_slug=_PROJECT_SLUG,
            started_minutes_ago=int(threshold.total_seconds() // 60) + 60,
            progress=partial,
        )
        fresh_id = await _seed_running_scrub(
            db_session,
            org_id=org_id,
            project_id=other_project.id,
            project_slug="scrub-other",
            started_minutes_ago=1,
            progress=ExcludedPathScrubProgress(dry_run=True),
        )
    before = {
        build.id: _build_state(await _read_build(build.id))
        for build in (scrubbed, unreached)
    }

    ctx = make_worker_ctx(http_client=httpx.AsyncClient())
    try:
        result = await excluded_path_scrub_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    assert result == "completed"
    dead = await _read_queue_job(dead_id)
    assert dead.status == JobStatus.failed.value
    assert dead.date_completed is not None
    assert dead.errors is not None
    assert dead.errors["type"] == "SilentWorker"
    assert dead.progress == partial.model_dump(mode="json")
    fresh = await _read_queue_job(fresh_id)
    assert fresh.status == JobStatus.in_progress.value
    assert fresh.date_completed is None
    for build in (scrubbed, unreached):
        assert _build_state(await _read_build(build.id)) == before[build.id]


@pytest.mark.asyncio
@pytest.mark.parametrize("dry_run", [True, False], ids=["dry_run", "real"])
async def test_completion_event_carries_the_report_counters(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    *,
    dry_run: bool,
) -> None:
    """One project-scoped event repeats the counters the report ends on.

    In a real run one build's delete is refused, so the event carries a
    failed build and only the objects that actually went; in a dry run
    the same project reports everything it would remove. Either way the
    event and the job's final ``progress`` agree, field for field.
    """
    manager, events = await build_event_manager(Configuration())
    store = _HookedStore()
    org_id, project_id, (_, refused) = await _seed_sphinx_project(
        db_session, store, "one", "two"
    )
    store.failing_keys.add(f"{refused.storage_prefix}.buildinfo")
    async with db_session.begin():
        queue_job_id = await _seed_queue_job(
            db_session, org_id=org_id, project_id=project_id, dry_run=dry_run
        )
    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(http_client=http_client, events=events)
    try:
        await excluded_path_scrub(
            ctx,
            _payload(
                org_id=org_id,
                project_id=project_id,
                queue_job_id=queue_job_id,
                dry_run=dry_run,
            ),
        )
    finally:
        await http_client.aclose()

    report = ExcludedPathScrubProgress.model_validate(
        (await _read_queue_job(queue_job_id)).progress
    )
    publisher = events.excluded_path_scrub_completed
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == _ORG_SLUG
    assert event.project == _PROJECT_SLUG
    assert event.dry_run is dry_run
    assert event.builds_examined == report.builds_examined == 2
    assert event.builds_affected == report.builds_affected == 2
    assert event.objects_removed == report.objects_removed
    assert event.bytes_removed == report.bytes_removed
    assert event.builds_failed == report.builds_failed
    assert event.builds_skipped == report.builds_skipped == 0
    assert event.elapsed >= timedelta(0)
    if dry_run:
        assert (event.objects_removed, event.bytes_removed) == (6, 2014)
        assert event.builds_failed == 0
    else:
        assert (event.objects_removed, event.bytes_removed) == (5, 2007)
        assert event.builds_failed == 1

    await manager.aclose()


@pytest.mark.asyncio
async def test_a_scrub_that_cannot_run_publishes_nothing(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A failed job is recorded on its row, never as a zero-count scrub."""
    manager, events = await build_event_manager(Configuration())
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(
            db_session, store_label=None
        )
        queue_job_id = await _seed_queue_job(
            db_session, org_id=org_id, project_id=project_id, dry_run=True
        )
    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(http_client=http_client, events=events)
    try:
        with pytest.raises(RuntimeError, match="No object store service"):
            await excluded_path_scrub(
                ctx,
                _payload(
                    org_id=org_id,
                    project_id=project_id,
                    queue_job_id=queue_job_id,
                ),
            )
    finally:
        await http_client.aclose()

    publisher = events.excluded_path_scrub_completed
    assert isinstance(publisher, MockEventPublisher)
    assert publisher.published == []

    await manager.aclose()
