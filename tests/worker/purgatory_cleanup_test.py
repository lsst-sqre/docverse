"""Tests for the ``purgatory_cleanup`` per-org worker function.

The per-org job is where object-store content actually goes, so the
tests here are mostly about what the job promises when something goes
wrong halfway: that a build it could not reclaim is counted rather than
silently stamped, that one bad build does not abandon the rest of the
work list, and that a restore landing in either of the two windows the
job leaves open resolves in the direction that keeps the row and its
content agreeing with each other.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.models import (
    BuildCreate,
    EditionCreate,
    EditionKind,
    JobKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse_server.config import config as runtime_config
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.build import Build
from docverse_server.domain.queue import JobStatus
from docverse_server.factory import Factory
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.objectstore import (
    MockObjectStore,
    ObjectStoreError,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.purgatory_cleanup import (
    purgatory_cleanup,
)
from tests.worker.conftest import make_worker_ctx

_HASH = "sha256:" + "f" * 64

_RETENTION = timedelta(days=7)

_ORG_SLUG = "purge-org"
_PROJECT_SLUG = "purge-proj"


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _mock_create_objectstore(mock_store: MockObjectStore) -> Any:
    """Patch ``create_objectstore_for_org`` to hand back one mock store."""

    async def _create(
        self: Factory, *, org_id: int, service_label: str
    ) -> MockObjectStore:
        return mock_store

    return _create


async def _seed_org_and_project(
    db_session: AsyncSession,
) -> tuple[int, int]:
    """Create an org with a staging store label and one project."""
    logger = _logger()
    org = await OrganizationStore(session=db_session, logger=logger).create(
        OrganizationCreate(
            slug=_ORG_SLUG,
            title="Purge Org",
            base_domain=f"{_ORG_SLUG}.example.com",
            purgatory_retention=int(_RETENTION.total_seconds()),
        )
    )
    await db_session.execute(
        update(SqlOrganization)
        .where(SqlOrganization.id == org.id)
        .values(publishing_store_label="mock-store")
    )
    project = await ProjectStore(session=db_session, logger=logger).create(
        org_id=org.id,
        data=ProjectCreate(
            slug=_PROJECT_SLUG,
            title="Purge Project",
            source_url="https://example.com/example/purge",
        ),
    )
    return org.id, project.id


async def _seed_live_build(
    db_session: AsyncSession, *, project_id: int, git_ref: str
) -> Build:
    """Create an ordinary, undeleted build in the seeded project."""
    return await BuildStore(session=db_session, logger=_logger()).create(
        project_id=project_id,
        project_slug=_PROJECT_SLUG,
        data=BuildCreate(git_ref=git_ref, content_hash=_HASH),
        uploader="testuser",
    )


async def _backdate_deletion(
    db_session: AsyncSession,
    *,
    build_id: int,
    deleted_days_ago: int,
    total_size_bytes: int = 1024,
) -> None:
    """Soft-delete a build at a chosen instant, with a recorded size.

    ``soft_delete`` stamps ``func.now()``, which is inside every
    plausible retention window; placing the timestamp is what lets a
    test say "this one is out of retention and that one is not".
    """
    await db_session.execute(
        update(SqlBuild)
        .where(SqlBuild.id == build_id)
        .values(
            date_deleted=datetime.now(tz=UTC)
            - timedelta(days=deleted_days_ago),
            total_size_bytes=total_size_bytes,
        )
    )


async def _seed_deleted_build(
    db_session: AsyncSession,
    *,
    project_id: int,
    git_ref: str,
    deleted_days_ago: int,
    total_size_bytes: int = 1024,
) -> Build:
    """Create a build already soft-deleted at a chosen instant."""
    build = await _seed_live_build(
        db_session, project_id=project_id, git_ref=git_ref
    )
    await _backdate_deletion(
        db_session,
        build_id=build.id,
        deleted_days_ago=deleted_days_ago,
        total_size_bytes=total_size_bytes,
    )
    return build


async def _stage_content(
    store: MockObjectStore, *, build: Build, files: int = 2
) -> None:
    """Put a build's unpacked tree and staged tarball on the store."""
    for index in range(files):
        await store.upload_object(
            key=f"{build.storage_prefix}page{index}.html",
            data=b"<html></html>",
            content_type="text/html",
        )
    await store.upload_object(
        key=build.staging_key,
        data=b"tarball",
        content_type="application/gzip",
    )


async def _seed_queue_job(db_session: AsyncSession, *, org_id: int) -> int:
    """Create the queued per-org row the dispatcher would have written."""
    queue_job = await QueueJobStore(
        session=db_session, logger=_logger()
    ).create(
        kind=JobKind.purgatory_cleanup,
        org_id=org_id,
        subject_label=_ORG_SLUG,
        backend_job_id="test-purgatory-job",
    )
    return queue_job.id


async def _read_queue_job(job_id: int) -> SqlQueueJob:
    async for session in db_session_dependency():
        async with session.begin():
            row = await session.get(SqlQueueJob, job_id)
            assert row is not None
            await session.refresh(row)
            return row
    msg = "No database session available"
    raise RuntimeError(msg)


async def _read_build(build_id: int) -> SqlBuild:
    async for session in db_session_dependency():
        async with session.begin():
            row = await session.get(SqlBuild, build_id)
            assert row is not None
            return row
    msg = "No database session available"
    raise RuntimeError(msg)


async def _read_progress(job_id: int) -> dict[str, Any]:
    """Read a finished job's ``progress`` JSONB, asserting it was set."""
    progress = (await _read_queue_job(job_id)).progress
    assert progress is not None
    return progress


def _payload(*, org_id: int, queue_job_id: int) -> dict[str, Any]:
    return {
        "org_id": org_id,
        "org_slug": _ORG_SLUG,
        "queue_job_id": queue_job_id,
    }


@pytest.mark.asyncio
async def test_purgatory_cleanup_reclaims_expired_builds_oldest_first(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both halves of a build's footprint go, and the row records it.

    The ordinary case the job exists for. Each build occupies an
    unpacked tree under ``storage_prefix`` and — until
    ``build_processing`` drops it at completion — a staged tarball; both
    have to go before ``date_purged`` may be stamped, because the stamp
    is what takes the build out of every later work list.
    """
    store = MockObjectStore()
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        older = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="old",
            deleted_days_ago=30,
            total_size_bytes=100,
        )
        newer = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="new",
            deleted_days_ago=20,
            total_size_bytes=200,
        )
        queue_job_id = await _seed_queue_job(db_session, org_id=org_id)
    await _stage_content(store, build=older)
    await _stage_content(store, build=newer)

    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    result = await purgatory_cleanup(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
    )

    assert result == "completed"
    assert store.objects == {}

    row = await _read_queue_job(queue_job_id)
    assert row.status == JobStatus.completed.value
    assert row.progress == {
        "builds_purged": 2,
        "builds_failed": 0,
        "builds_skipped_referenced": 0,
        "objects_deleted": 4,
        "bytes_reclaimed": 300,
        "capped": False,
        "failed_build_ids": [],
        "skipped_build_ids": [],
    }
    for build in (older, newer):
        assert (await _read_build(build.id)).date_purged is not None


class _Crash(BaseException):
    """A failure the job's per-build handler is not meant to absorb.

    Deliberately not an :class:`Exception`: the per-build ``except``
    block catches those and keeps going, which is the wrong model for
    the worker dying mid-loop (an OOM kill, a SIGKILL during a pod
    rotation). Raising outside that hierarchy is how a test asks for the
    process-death case rather than the per-build-failure case.
    """


class _HookedMockObjectStore(MockObjectStore):
    """``MockObjectStore`` that runs a callback before each delete.

    The hooks are how these tests reach the two windows the job leaves
    open by design — between planning and reclamation, and between
    reclamation and the ``date_purged`` stamp. Both are only reachable
    while the job holds no transaction, which is exactly when the store
    is being called, so the store is the natural place to stand.
    """

    def __init__(
        self,
        *,
        on_delete_prefix: Any = None,
        on_delete_object: Any = None,
    ) -> None:
        super().__init__()
        self._on_delete_prefix = on_delete_prefix
        self._on_delete_object = on_delete_object

    async def delete_prefix(self, *, prefix: str) -> int:
        if self._on_delete_prefix is not None:
            await self._on_delete_prefix(prefix)
        return await super().delete_prefix(prefix=prefix)

    async def delete_object(self, *, key: str) -> None:
        if self._on_delete_object is not None:
            await self._on_delete_object(key)
        await super().delete_object(key=key)


async def _restore_build(build_id: int) -> None:
    """Commit an admin restore on an independent session."""
    async for session in db_session_dependency():
        async with session.begin():
            await session.execute(
                update(SqlBuild)
                .where(SqlBuild.id == build_id)
                .values(date_deleted=None)
            )
        return
    msg = "No database session available"
    raise RuntimeError(msg)


@pytest.mark.asyncio
async def test_purgatory_cleanup_stops_at_the_cap_oldest_first(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A capped tick takes the longest-waiting build and says so.

    The cap bounds how long one org's backlog can hold a maintenance
    slot. Nothing is lost to it: the work list is ordered oldest
    deletion first and the rows it leaves are unstamped, so reporting
    ``capped`` tells an operator the next tick has more to do rather
    than that anything was dropped.
    """
    monkeypatch.setattr(
        runtime_config, "purgatory_cleanup_max_builds_per_job", 1
    )
    store = MockObjectStore()
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        older = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="old",
            deleted_days_ago=30,
        )
        newer = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="new",
            deleted_days_ago=10,
        )
        queue_job_id = await _seed_queue_job(db_session, org_id=org_id)
    await _stage_content(store, build=older)
    await _stage_content(store, build=newer)

    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    result = await purgatory_cleanup(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
    )

    assert result == "completed"
    progress = await _read_progress(queue_job_id)
    assert progress["builds_purged"] == 1
    assert progress["capped"] is True
    assert (await _read_build(older.id)).date_purged is not None
    assert (await _read_build(newer.id)).date_purged is None
    assert sorted(store.objects) == sorted(
        [
            f"{newer.storage_prefix}page0.html",
            f"{newer.storage_prefix}page1.html",
            newer.staging_key,
        ]
    )


@pytest.mark.asyncio
async def test_purgatory_cleanup_holds_back_a_served_build(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A build a live edition points at keeps its content and its chance.

    Retention says nothing about whether anything still resolves to the
    build, and reclaiming one an edition is serving would 404 a URL
    somebody is using. Leaving ``date_purged`` null is what puts the
    build back in front of the next tick once the edition moves on, and
    the warning names the edition so an operator knows what to roll
    back.
    """
    store = MockObjectStore()
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        # Pointed at while still live, then deleted: exactly the shape
        # of a row that predates the DELETE 409 guard, which is what the
        # first sweep on an existing deployment will meet.
        served = await _seed_live_build(
            db_session, project_id=project_id, git_ref="served"
        )
        free = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="free",
            deleted_days_ago=20,
        )
        edition_store = EditionStore(session=db_session, logger=_logger())
        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="live-edition",
                title="Live Edition",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        await edition_store.set_current_build(
            edition_id=edition.id, build_id=served.id
        )
        await _backdate_deletion(
            db_session, build_id=served.id, deleted_days_ago=30
        )
        queue_job_id = await _seed_queue_job(db_session, org_id=org_id)
    await _stage_content(store, build=served)
    await _stage_content(store, build=free)

    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    with capture_logs() as logs:
        result = await purgatory_cleanup(
            ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
        )

    assert result == "completed"
    progress = await _read_progress(queue_job_id)
    assert progress["builds_skipped_referenced"] == 1
    assert progress["builds_purged"] == 1
    assert progress["skipped_build_ids"] == [
        serialize_base32_id(served.public_id)
    ]
    assert (await _read_build(served.id)).date_purged is None
    assert f"{served.storage_prefix}page0.html" in store.objects

    warnings = [entry for entry in logs if entry["log_level"] == "warning"]
    assert any(entry["editions"] == ["live-edition"] for entry in warnings)


@pytest.mark.asyncio
async def test_purgatory_cleanup_counts_a_failed_build_and_carries_on(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One store failure costs its own build, not the whole work list.

    A prefix the store refuses is a per-build problem — a transient
    error, a permissions change on one project's keys — and the builds
    behind it in the list have nothing to do with it. The job reports
    ``completed_with_errors`` so the failure is visible without pretending
    the tick did not happen; the failed row keeps ``date_purged`` null
    and comes back on the next tick.
    """
    store = _HookedMockObjectStore()
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        doomed = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="doomed",
            deleted_days_ago=30,
        )
        healthy = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="healthy",
            deleted_days_ago=20,
        )
        queue_job_id = await _seed_queue_job(db_session, org_id=org_id)
    await _stage_content(store, build=doomed)
    await _stage_content(store, build=healthy)

    async def _refuse_doomed_prefix(prefix: str) -> None:
        if prefix == doomed.storage_prefix:
            raise ObjectStoreError(
                bucket="mock",
                prefix=prefix,
                operation="DeleteObjects",
                failures=[f"{prefix}page0.html (AccessDenied)"],
            )

    store._on_delete_prefix = _refuse_doomed_prefix

    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    result = await purgatory_cleanup(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
    )

    assert result == "completed_with_errors"
    assert (
        await _read_queue_job(queue_job_id)
    ).status == JobStatus.completed_with_errors.value
    progress = await _read_progress(queue_job_id)
    assert progress["builds_failed"] == 1
    assert progress["builds_purged"] == 1
    assert progress["failed_build_ids"] == [
        serialize_base32_id(doomed.public_id)
    ]
    assert (await _read_build(doomed.id)).date_purged is None
    assert (await _read_build(healthy.id)).date_purged is not None
    assert f"{doomed.storage_prefix}page0.html" in store.objects


@pytest.mark.asyncio
async def test_purgatory_cleanup_leaves_finished_builds_stamped_on_crash(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A worker dying mid-loop keeps every build it already finished.

    Each build's stamp commits in its own transaction immediately after
    its content goes, so process death costs at most the build in
    flight. The next tick reads the same work list minus the stamped
    rows and resumes — the property that makes an interrupted sweep
    safe to simply run again.
    """
    store = _HookedMockObjectStore()
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        first = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="first",
            deleted_days_ago=30,
        )
        second = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="second",
            deleted_days_ago=20,
        )
        third = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="third",
            deleted_days_ago=10,
        )
        queue_job_id = await _seed_queue_job(db_session, org_id=org_id)
    for build in (first, second, third):
        await _stage_content(store, build=build)

    async def _die_on_third(prefix: str) -> None:
        if prefix == third.storage_prefix:
            raise _Crash

    store._on_delete_prefix = _die_on_third

    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    with pytest.raises(_Crash):
        await purgatory_cleanup(
            ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
        )

    for build in (first, second):
        assert (await _read_build(build.id)).date_purged is not None
        assert f"{build.storage_prefix}page0.html" not in store.objects
        assert build.staging_key not in store.objects
    assert (await _read_build(third.id)).date_purged is None
    assert f"{third.storage_prefix}page0.html" in store.objects


@pytest.mark.asyncio
async def test_purgatory_cleanup_skips_a_build_restored_before_reclaim(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restore that beats the delete keeps the build's content.

    The plan is read minutes before the last build in it is reached, and
    an admin restore can land anywhere in that gap. Re-reading
    ``date_deleted`` immediately before the delete is the last moment at
    which the objects are still there to keep, so the restore wins and
    nothing is deleted.
    """
    store = _HookedMockObjectStore()
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        first = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="first",
            deleted_days_ago=30,
        )
        rescued = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="rescued",
            deleted_days_ago=20,
        )
        queue_job_id = await _seed_queue_job(db_session, org_id=org_id)
    await _stage_content(store, build=first)
    await _stage_content(store, build=rescued)

    async def _restore_second_build(prefix: str) -> None:
        if prefix == first.storage_prefix:
            await _restore_build(rescued.id)

    store._on_delete_prefix = _restore_second_build

    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    result = await purgatory_cleanup(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
    )

    assert result == "completed"
    progress = await _read_progress(queue_job_id)
    assert progress["builds_purged"] == 1
    assert progress["builds_failed"] == 0
    assert progress["skipped_build_ids"] == [
        serialize_base32_id(rescued.public_id)
    ]
    restored_row = await _read_build(rescued.id)
    assert restored_row.date_deleted is None
    assert restored_row.date_purged is None
    assert f"{rescued.storage_prefix}page0.html" in store.objects


@pytest.mark.asyncio
async def test_purgatory_cleanup_reports_a_restore_that_lost_the_content(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restore landing after the delete is an error, not a quiet stamp.

    ``mark_purged`` refuses a row that is live again, so the job finds
    zero rows updated. That combination — content gone, row restored —
    is the one outcome the sweep cannot repair itself, so it is counted
    as a failure and logged at error level rather than papered over by
    stamping ``date_purged`` on a build somebody just asked to keep.
    """
    store = _HookedMockObjectStore()
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        raced = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="raced",
            deleted_days_ago=30,
        )
        queue_job_id = await _seed_queue_job(db_session, org_id=org_id)
    await _stage_content(store, build=raced)

    async def _restore_after_the_tree_is_gone(key: str) -> None:
        if key == raced.staging_key:
            await _restore_build(raced.id)

    store._on_delete_object = _restore_after_the_tree_is_gone

    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    with capture_logs() as logs:
        result = await purgatory_cleanup(
            ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
        )

    assert result == "completed_with_errors"
    progress = await _read_progress(queue_job_id)
    assert progress["builds_failed"] == 1
    assert progress["failed_build_ids"] == [
        serialize_base32_id(raced.public_id)
    ]
    row = await _read_build(raced.id)
    assert row.date_deleted is None
    assert row.date_purged is None
    assert store.objects == {}

    errors = [entry for entry in logs if entry["log_level"] == "error"]
    assert [entry["build"] for entry in errors] == [
        serialize_base32_id(raced.public_id)
    ]


@pytest.mark.asyncio
async def test_purgatory_cleanup_fails_when_no_staging_store_is_configured(
    app: None,
    db_session: AsyncSession,
) -> None:
    """No store means the job cannot honour its contract, so it fails.

    Completing with nothing reclaimed would read as "there was nothing
    to do" on every dashboard, and the org's expired builds would keep
    their bytes indefinitely with no signal at all. The queue row goes
    ``failed`` so the org's misconfiguration is visible.
    """
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        await db_session.execute(
            update(SqlOrganization)
            .where(SqlOrganization.id == org_id)
            .values(publishing_store_label=None, staging_store_label=None)
        )
        await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="stranded",
            deleted_days_ago=30,
        )
        queue_job_id = await _seed_queue_job(db_session, org_id=org_id)

    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    with pytest.raises(RuntimeError, match="No object store service"):
        await purgatory_cleanup(
            ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
        )

    row = await _read_queue_job(queue_job_id)
    assert row.status == JobStatus.failed.value
    assert row.errors is not None
    assert row.errors["type"] == "RuntimeError"


@pytest.mark.asyncio
async def test_purgatory_cleanup_skips_a_late_delivered_job(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row a reaper already closed is not swept a second time.

    The pickup guard runs before anything else, so a re-delivery costs
    one lookup and touches neither the object store nor the build rows —
    the sweep must never double-count a tally or run two reclamations
    over one org at once.
    """
    store = MockObjectStore()
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        build = await _seed_deleted_build(
            db_session,
            project_id=project_id,
            git_ref="untouched",
            deleted_days_ago=30,
        )
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        queue_job = await queue_job_store.create(
            kind=JobKind.purgatory_cleanup,
            org_id=org_id,
            subject_label=_ORG_SLUG,
            backend_job_id="late-delivery",
        )
        await queue_job_store.fail(queue_job.id, errors={"message": "reaped"})
    await _stage_content(store, build=build)

    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _mock_create_objectstore(store)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    result = await purgatory_cleanup(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job.id)
    )

    assert result == "skipped"
    assert (await _read_build(build.id)).date_purged is None
    assert f"{build.storage_prefix}page0.html" in store.objects
