"""Integration tests for the build_processing worker function."""

from __future__ import annotations

import io
import tarfile
from typing import Any

import httpx
import pytest
import structlog
from cryptography.fernet import Fernet
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import (
    BuildCreate,
    BuildStatus,
    OrganizationCreate,
    ProjectCreate,
)
from docverse.dbschema.organization import SqlOrganization
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.queue import JobKind, JobStatus
from docverse.factory import WorkerFactory
from docverse.services.credential_encryptor import CredentialEncryptor
from docverse.services.edition_tracking import EditionTrackingService
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.objectstore import MockObjectStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.build_processing import build_processing

_HASH = "sha256:" + "a" * 64


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
    db_session: async_scoped_session[AsyncSession],
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
            doc_repo="https://github.com/example/repo",
        ),
    )
    return org, project


async def _create_build_in_processing(
    db_session: async_scoped_session[AsyncSession],
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
        self: WorkerFactory,  # noqa: ARG001
        *,
        org_id: int,  # noqa: ARG001
        service_label: str,  # noqa: ARG001
    ) -> MockObjectStore:
        return mock_store

    return _create


@pytest.mark.asyncio
async def test_build_processing_updates_edition(
    app: None,  # noqa: ARG001
    db_session: async_scoped_session[AsyncSession],
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
        WorkerFactory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    encryptor = CredentialEncryptor(current_key=Fernet.generate_key().decode())
    ctx: dict[str, Any] = {
        "encryptor": encryptor,
        "http_client": httpx.AsyncClient(),
        "job_id": "test-arq-job-1",
    }
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
async def test_build_processing_uses_stored_storage_prefix(
    app: None,  # noqa: ARG001
    db_session: async_scoped_session[AsyncSession],
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
        WorkerFactory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    encryptor = CredentialEncryptor(current_key=Fernet.generate_key().decode())
    ctx: dict[str, Any] = {
        "encryptor": encryptor,
        "http_client": httpx.AsyncClient(),
        "job_id": "test-arq-prefix",
    }
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
    app: None,  # noqa: ARG001
    db_session: async_scoped_session[AsyncSession],
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
        WorkerFactory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    # Monkeypatch edition tracking to raise an exception
    async def _broken_track(
        self: EditionTrackingService,  # noqa: ARG001
        build: Any,  # noqa: ARG001
    ) -> None:
        msg = "Simulated edition tracking failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(EditionTrackingService, "track_build", _broken_track)

    encryptor = CredentialEncryptor(current_key=Fernet.generate_key().decode())
    ctx: dict[str, Any] = {
        "encryptor": encryptor,
        "http_client": httpx.AsyncClient(),
        "job_id": "test-arq-job-2",
    }
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
