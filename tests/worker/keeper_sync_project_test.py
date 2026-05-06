"""Tests for the ``keeper_sync_project`` worker function.

Wires the real ``KeeperSyncService`` against the alembic-managed test DB,
stubs LTD HTTP via ``respx``, and patches ``Factory.create_ltd_s3_source``
+ ``Factory.create_objectstore_for_org`` so the source/destination side
of :class:`BuildContentCopier` runs against in-memory doubles. This
covers the worker's contract — service is invoked, queue job tracks
its lifecycle, and ``_maybe_finalise_run`` runs in both the happy and
failing paths — without depending on real S3 or R2.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import httpx
import pytest
import respx
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    BuildStatus,
    JobKind,
    KeeperSyncConfig,
    KeeperSyncRunStatus,
    OrganizationCreate,
)
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.domain.queue import JobStatus
from docverse.factory import Factory
from docverse.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.keeper_sync import KeeperSyncStateStore, ResourceType
from docverse.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse.storage.ltd import LtdNotFoundError
from docverse.storage.objectstore import MockObjectStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.keeper_sync import keeper_sync_project
from tests.support.arq_testing import register_queue
from tests.worker.conftest import make_worker_ctx

LTD_BASE = "https://keeper.lsst.codes"
FIXTURES_DIR = Path(__file__).parent.parent / "storage" / "ltd" / "fixtures"


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _load(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES_DIR / name).read_text())  # type: ignore[no-any-return]


class _FakeLtdSource:
    """In-memory ``LtdSourceProtocol`` backing for worker integration tests."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self._objects = objects

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return None

    async def list_keys(self, *, prefix: str) -> list[str]:
        return [k for k in self._objects if k.startswith(prefix)]

    async def download_object(self, *, key: str) -> bytes:
        return self._objects[key]


def _patch_factory_io(
    monkeypatch: pytest.MonkeyPatch,
    *,
    object_store: MockObjectStore,
    source_objects: dict[str, bytes],
) -> None:
    """Route the factory's S3/objectstore wiring through in-memory doubles."""

    async def _create_objectstore_for_org(
        self: Factory, *, org_id: int, service_label: str
    ) -> MockObjectStore:
        return object_store

    def _create_ltd_s3_source(
        self: Factory, *, bucket: str = "lsst-the-docs"
    ) -> _FakeLtdSource:
        return _FakeLtdSource(source_objects)

    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _create_objectstore_for_org
    )
    monkeypatch.setattr(Factory, "create_ltd_s3_source", _create_ltd_s3_source)


async def _seed_org(db_session: AsyncSession) -> tuple[int, str]:
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug="ks-worker",
            title="KS Worker",
            base_domain="ks-worker.example.com",
        )
    )
    await org_store.update_keeper_sync_config(
        slug=org.slug,
        config=KeeperSyncConfig(
            enabled=True,
            project_slugs=["pipelines"],
        ),
    )
    return org.id, org.slug


async def _seed_run(db_session: AsyncSession, *, org_id: int) -> int:
    row = SqlKeeperSyncRun(
        org_id=org_id, kind="backfill", status="in_progress"
    )
    db_session.add(row)
    await db_session.flush()
    await db_session.refresh(row)
    return row.id


async def _seed_project_queue_job(
    db_session: AsyncSession, *, org_id: int, run_id: int
) -> int:
    queue_job_store = QueueJobStore(session=db_session, logger=_logger())
    queue_job = await queue_job_store.create(
        kind=JobKind.keeper_sync_project,
        org_id=org_id,
        keeper_sync_run_id=run_id,
        backend_job_id="test-arq-project-1",
    )
    return queue_job.id


def _seed_ltd(mock_discovery: respx.Router) -> None:
    """Stub the canonical LTD endpoints for the ``pipelines`` product."""
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200, json={"editions": [f"{LTD_BASE}/editions/1"]}
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(
            200, json=_load("edition_main_git_refs.json")
        )
    )
    mock_discovery.get(f"{LTD_BASE}/builds/42").mock(
        return_value=httpx.Response(200, json=_load("build.json"))
    )


@pytest.mark.asyncio
async def test_keeper_sync_project_runs_service_and_finalises_run(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: project + edition + build + state rows, run -> succeeded."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
        "pipelines/builds/42/assets/app.js": b"console.log(1)",
    }
    _patch_factory_io(
        monkeypatch,
        object_store=object_store,
        source_objects=source_objects,
    )

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await keeper_sync_project(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "run_id": run_id,
            "queue_job_id": queue_job_id,
            "ltd_slug": "pipelines",
            "ltd_base_url": LTD_BASE,
        },
    )
    await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
            assert project is not None

            edition_store = EditionStore(session=session, logger=_logger())
            main_edition = await edition_store.get_by_slug(
                project_id=project.id, slug="__main"
            )
            assert main_edition is not None
            assert main_edition.current_build_id is not None

            build_store = BuildStore(session=session, logger=_logger())
            build = await build_store.get_by_id(main_edition.current_build_id)
            assert build is not None
            assert build.status == BuildStatus.completed
            assert build.uploader == "keeper-sync"

            state_store = KeeperSyncStateStore(
                session=session, logger=_logger()
            )
            project_state = await state_store.get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug="pipelines",
            )
            assert project_state is not None
            assert project_state.docverse_id == project.id
            build_state = await state_store.get(
                org_id=org_id,
                resource_type=ResourceType.build,
                ltd_id=42,
            )
            assert build_state is not None
            assert build_state.docverse_id == build.id

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            qj = await queue_job_store.get(queue_job_id)
            assert qj is not None
            assert qj.status == JobStatus.completed

            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.succeeded

    # Build content actually landed in the destination object store.
    assert any(k.endswith("/index.html") for k in object_store.objects)
    assert any(k.endswith("/app.js") for k in object_store.objects)


@pytest.mark.asyncio
async def test_keeper_sync_project_failure_marks_queue_job_and_finalises_run(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LTD 404 → exception bubbles, queue job ``failed``, run finalised."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    # Product endpoint returns 404 — the LTD client will raise after
    # exhausting its bounded retry, and the worker must surface the
    # exception while still flipping the queue-job + run status rows.
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(404)
    )

    object_store = MockObjectStore()
    _patch_factory_io(
        monkeypatch, object_store=object_store, source_objects={}
    )

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    with pytest.raises(LtdNotFoundError):
        await keeper_sync_project(
            ctx,
            {
                "org_id": org_id,
                "org_slug": org_slug,
                "run_id": run_id,
                "queue_job_id": queue_job_id,
                "ltd_slug": "pipelines",
                "ltd_base_url": LTD_BASE,
            },
        )
    await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            queue_job_store = QueueJobStore(session=session, logger=_logger())
            qj = await queue_job_store.get(queue_job_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            assert qj.errors is not None
            assert qj.errors.get("message")

            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            # Single child failed → run finalises as partial_failure.
            assert run.status == KeeperSyncRunStatus.partial_failure

    # Nothing was copied to the destination on the failure path.
    assert object_store.objects == {}
