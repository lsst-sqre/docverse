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
from collections.abc import Sequence
from datetime import timedelta
from importlib.metadata import version as pkg_version
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import httpx
import pytest
import respx
import sentry_sdk
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher
from safir.testing.sentry import (
    TestTransport,
    capture_events_fixture,
    sentry_init_fixture,
)
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.models import (
    BuildStatus,
    EditionCreate,
    EditionKind,
    JobKind,
    KeeperSyncConfig,
    KeeperSyncRunStatus,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.models.queue_enums import PublishStatus
from docverse_server.config import Configuration
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.dbschema.edition_build_history import (
    SqlEditionBuildHistory,
)
from docverse_server.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import (
    generate_base32_id,
    validate_base32_id,
)
from docverse_server.domain.edition import Edition
from docverse_server.domain.edition_build_history import EditionBuildHistory
from docverse_server.domain.queue import JobStatus
from docverse_server.exceptions import KeeperSyncSystemicFailureError
from docverse_server.factory import Factory
from docverse_server.metrics import build_event_manager
from docverse_server.sentry import initialize_sentry
from docverse_server.services.dashboard.enqueue import DashboardBuildEnqueuer
from docverse_server.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse_server.services.lock_service import LockClass, LockKey
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
)
from docverse_server.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse_server.storage.ltd import (
    LtdNotFoundError,
    LtdSourceAccessDeniedError,
)
from docverse_server.storage.objectstore import MockObjectStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_backend import ArqQueueBackend
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.keeper_sync import keeper_sync_project
from tests.support.arq_testing import get_jobs_by_name, register_queue
from tests.support.lock_service_spy import install_recording_lock_service
from tests.worker.conftest import make_worker_ctx

LTD_BASE = "https://keeper.lsst.codes"
FIXTURES_DIR = Path(__file__).parent.parent / "storage" / "ltd" / "fixtures"


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _load(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES_DIR / name).read_text())  # type: ignore[no-any-return]


class _FakeLtdSource:
    """In-memory ``LtdSourceProtocol`` backing for worker integration tests.

    ``denied_prefixes`` reproduces LTD's oldest uploads: the keys under
    the prefix are listable but every ``GetObject`` answers
    ``AccessDenied``, because those objects were written without a
    public-read ACL and this source is anonymous.
    """

    def __init__(
        self,
        objects: dict[str, bytes],
        *,
        denied_prefixes: frozenset[str] = frozenset(),
    ) -> None:
        self._objects = objects
        self._denied_prefixes = denied_prefixes

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
        if any(key.startswith(prefix) for prefix in self._denied_prefixes):
            raise LtdSourceAccessDeniedError(
                bucket="lsst-the-docs", key=key, operation="GetObject"
            )
        return self._objects[key]


def _patch_factory_io(
    monkeypatch: pytest.MonkeyPatch,
    *,
    object_store: MockObjectStore,
    source_objects: dict[str, bytes],
    denied_prefixes: frozenset[str] = frozenset(),
) -> None:
    """Route the factory's S3/objectstore wiring through in-memory doubles."""

    async def _create_objectstore_for_org(
        self: Factory, *, org_id: int, service_label: str
    ) -> MockObjectStore:
        return object_store

    def _create_ltd_s3_source(
        self: Factory, *, bucket: str = "lsst-the-docs"
    ) -> _FakeLtdSource:
        return _FakeLtdSource(source_objects, denied_prefixes=denied_prefixes)

    monkeypatch.setattr(
        Factory, "create_objectstore_for_org", _create_objectstore_for_org
    )
    monkeypatch.setattr(Factory, "create_ltd_s3_source", _create_ltd_s3_source)


async def _seed_org(
    db_session: AsyncSession,
    *,
    publishing_store_label: str | None = "mock-store",
) -> tuple[int, str]:
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
    if publishing_store_label is not None:
        await db_session.execute(
            update(SqlOrganization)
            .where(SqlOrganization.id == org.id)
            .values(publishing_store_label=publishing_store_label)
        )
        await db_session.flush()
    return org.id, org.slug


async def _seed_run(db_session: AsyncSession, *, org_id: int) -> int:
    row = SqlKeeperSyncRun(
        public_id=validate_base32_id(generate_base32_id()),
        org_id=org_id,
        kind="backfill",
        status="in_progress",
    )
    db_session.add(row)
    await db_session.flush()
    await db_session.refresh(row)
    return row.id


async def _seed_project_queue_job(
    db_session: AsyncSession,
    *,
    org_id: int,
    run_id: int,
    backend_job_id: str = "test-arq-project-1",
) -> int:
    queue_job_store = QueueJobStore(session=db_session, logger=_logger())
    queue_job = await queue_job_store.create(
        kind=JobKind.keeper_sync_project,
        org_id=org_id,
        keeper_sync_run_id=run_id,
        backend_job_id=backend_job_id,
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
async def test_keeper_sync_project_runs_service_and_enqueues_publish(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: project + edition + build, publish_edition enqueued.

    The keeper_sync_project worker must drive the synced edition's
    finalized build through the same publish path as a normal client
    upload. Asserts:

    * Project / edition / build / state rows landed (the v1 sync
      contract).
    * The edition's ``publish_status`` is ``pending`` and a matching
      ``EditionBuildHistory`` row exists with ``publish_status=pending``.
    * A ``publish_edition`` ``QueueJob`` row was created carrying
      ``keeper_sync_run_id`` so it rolls into the parent run's progress.
    * A ``publish_edition`` arq job was enqueued on the *regular* queue
      (``docverse:queue``), not the dedicated ``docverse:sync-queue``,
      so the existing publish-edition worker pool picks it up.
    * The parent run remains ``in_progress`` because the publish child
      is still queued — finalisation cascades through the publish_edition
      worker once it completes.
    """
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

    publish_jobs = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    assert len(publish_jobs) == 1
    publish_payload = publish_jobs[0].kwargs["payload"]
    assert publish_payload["edition_slug"] == "__main"
    assert publish_payload["project_slug"] == "pipelines"
    assert publish_payload["org_id"] == org_id

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
            assert main_edition.publish_status == PublishStatus.pending

            build_store = BuildStore(session=session, logger=_logger())
            build = await build_store.get_by_id(main_edition.current_build_id)
            assert build is not None
            assert build.status == BuildStatus.completed
            assert build.uploader == "keeper-sync"

            history_store = EditionBuildHistoryStore(
                session=session, logger=_logger()
            )
            history = await history_store.get_by_edition_and_build(
                edition_id=main_edition.id, build_id=build.id
            )
            assert history is not None
            assert history.publish_status == PublishStatus.pending

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
            # Zero edition failures stays plain ``completed`` — the
            # ``completed_with_errors`` status is reserved for a genuine
            # partial import, so a clean run is never ambiguous.
            assert qj.status == JobStatus.completed

            publish_qj = await queue_job_store.get_by_backend_job_id(
                publish_jobs[0].id
            )
            assert publish_qj is not None
            assert publish_qj.kind == JobKind.publish_edition
            assert publish_qj.keeper_sync_run_id == run_id
            assert publish_qj.edition_id == main_edition.id
            assert publish_qj.build_id == build.id
            assert publish_qj.org_id == org_id
            assert publish_qj.project_id == project.id

            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            # Publish child is still queued, so the run waits for it.
            assert run.status == KeeperSyncRunStatus.in_progress

    # Build content actually landed in the destination object store.
    assert any(k.endswith("/index.html") for k in object_store.objects)
    assert any(k.endswith("/app.js") for k in object_store.objects)


@pytest.mark.asyncio
async def test_keeper_sync_project_publishes_adopted_edition_under_native_slug(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An adopted edition's publish carries the persisted native slug.

    PRD #409: a native edition already tracks ``tickets/DM-54686`` under
    the slugified ``tickets-DM-54686`` slug; keeper-sync imports LTD's own
    ``DM-54686`` slug on the same ref and adopts the native edition. The
    enqueued ``publish_edition`` job must carry the *persisted* slug
    (``tickets-DM-54686``) — both publish paths resolve the edition via
    ``get_by_slug``, so the keeper-derived ``DM-54686`` would miss the row
    and the freshly-synced build would fail to publish.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )
        # A native auto-created edition already tracks the branch ref under
        # the slugified slug, before keeper-sync ever runs.
        project_store = ProjectStore(session=db_session, logger=_logger())
        seed_project = await project_store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="pipelines",
                title="LSST Science Pipelines",
                source_url="https://example.com/lsst/pipelines",
            ),
        )
        edition_store = EditionStore(session=db_session, logger=_logger())
        native_edition = await edition_store.create(
            project_id=seed_project.id,
            data=EditionCreate(
                slug="tickets-DM-54686",
                title="DM-54686",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "tickets/DM-54686"},
            ),
        )
    native_edition_id = native_edition.id

    # LTD reports the same branch under its own ``DM-54686`` slug.
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200, json={"editions": [f"{LTD_BASE}/editions/2"]}
        )
    )
    branch_edition = _load("edition_branch_git_refs.json")
    branch_edition["slug"] = "DM-54686"
    branch_edition["title"] = "DM-54686"
    branch_edition["tracked_refs"] = ["tickets/DM-54686"]
    branch_edition["build_url"] = f"{LTD_BASE}/builds/43"
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(200, json=branch_edition)
    )
    branch_build = _load("build.json")
    branch_build["self_url"] = f"{LTD_BASE}/builds/43"
    branch_build["bucket_root_dir"] = "pipelines/builds/43"
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=branch_build)
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>branch</html>",
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

    publish_jobs = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    assert len(publish_jobs) == 1
    publish_payload = publish_jobs[0].kwargs["payload"]
    # The publish job must carry the persisted native slug so
    # ``publish_edition``'s ``get_by_slug`` resolves the adopted edition;
    # the keeper-derived ``DM-54686`` slug would miss the row entirely.
    assert publish_payload["edition_slug"] == "tickets-DM-54686"
    assert publish_payload["edition_id"] == native_edition_id

    async for session in db_session_dependency():
        async with session.begin():
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
            assert project is not None

            edition_store = EditionStore(session=session, logger=_logger())
            # The adopted edition got the synced build and a pending publish.
            adopted = await edition_store.get_by_slug(
                project_id=project.id, slug="tickets-DM-54686"
            )
            assert adopted is not None
            assert adopted.id == native_edition_id
            assert adopted.current_build_id is not None
            assert adopted.publish_status == PublishStatus.pending

            # No second row was created under the keeper-derived slug.
            keeper_slugged = await edition_store.get_by_slug(
                project_id=project.id, slug="DM-54686"
            )
            assert keeper_slugged is None


@pytest.mark.asyncio
async def test_keeper_sync_project_short_circuit_skips_publish_enqueue(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A short-circuited sync (LTD ``date_rebuilt`` unchanged) skips publish.

    Runs ``keeper_sync_project`` twice for the same product. The first
    pass populates everything (project / edition / build / state rows
    and a ``publish_edition`` arq job). The second pass observes the
    ``keeper_sync_state`` row's ``date_rebuilt_seen`` matches LTD's
    ``date_rebuilt`` and short-circuits inside ``KeeperSyncService.
    sync_build``. It must NOT enqueue a redundant ``publish_edition``
    arq job — re-publishing on every reconciliation tick would burn
    KV writes without any state change.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        first_qj = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
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

    payload: dict[str, Any] = {
        "org_id": org_id,
        "org_slug": org_slug,
        "run_id": run_id,
        "queue_job_id": first_qj,
        "ltd_slug": "pipelines",
        "ltd_base_url": LTD_BASE,
    }

    first_result = await keeper_sync_project(ctx, payload)
    assert first_result == "completed"
    publish_after_first = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    assert len(publish_after_first) == 1

    # Second pass on the same LTD state — must short-circuit.
    async with db_session.begin():
        second_qj = await _seed_project_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="test-arq-project-2",
        )
    payload["queue_job_id"] = second_qj
    second_result = await keeper_sync_project(ctx, payload)
    await ctx["http_client"].aclose()
    assert second_result == "completed"

    publish_after_second = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    # Still exactly one publish job — the second pass short-circuited.
    assert len(publish_after_second) == 1


@pytest.mark.asyncio
async def test_keeper_sync_project_self_heals_unpublished_short_circuit(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Short-circuited sync re-publishes an edition that was never published.

    Simulates the staging shape from the first sync runs that landed
    before the publish-enqueue path existed: an edition has its
    ``current_build_id`` set, the build is ``completed``, and a
    ``keeper_sync_state`` row matches LTD's ``date_rebuilt`` — but no
    publish was ever enqueued against this build, so neither the
    edition's ``publish_status`` nor an ``edition_build_history`` row for
    the pair exists. On the next sync run the build sync still
    short-circuits (no LTD-side change), but the worker must observe
    the unpublished edition and enqueue a catch-up
    ``publish_edition`` job so KV + dashboard come into sync.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        first_qj = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
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

    payload: dict[str, Any] = {
        "org_id": org_id,
        "org_slug": org_slug,
        "run_id": run_id,
        "queue_job_id": first_qj,
        "ltd_slug": "pipelines",
        "ltd_base_url": LTD_BASE,
    }

    first_result = await keeper_sync_project(ctx, payload)
    assert first_result == "completed"
    publish_after_first = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    assert len(publish_after_first) == 1

    # Erase every trace of the publish to mimic data that landed before
    # the publish-enqueue path existed.
    await _clear_publish_traces(org_id=org_id, edition_slug="__main")

    async with db_session.begin():
        second_qj = await _seed_project_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="test-arq-project-2",
        )
    payload["queue_job_id"] = second_qj
    second_result = await keeper_sync_project(ctx, payload)
    await ctx["http_client"].aclose()
    assert second_result == "completed"

    publish_after_second = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    # The second pass short-circuited but observed the unpublished edition,
    # so a catch-up publish was enqueued.
    assert len(publish_after_second) == 2

    second_payload = publish_after_second[1].kwargs["payload"]
    assert second_payload["edition_slug"] == "__main"
    assert second_payload["project_slug"] == "pipelines"
    assert second_payload["org_id"] == org_id

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
            assert project is not None
            edition = await edition_store.get_by_slug(
                project_id=project.id, slug="__main"
            )
            assert edition is not None
            assert edition.publish_status == PublishStatus.pending

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            self_heal_qj = await queue_job_store.get_by_backend_job_id(
                publish_after_second[1].id
            )
            assert self_heal_qj is not None
            assert self_heal_qj.kind == JobKind.publish_edition
            assert self_heal_qj.keeper_sync_run_id == run_id


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


@pytest.mark.asyncio
async def test_keeper_sync_project_skips_reaped_queue_job(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A run child a reaper already failed is skipped, not raised on.

    The late-delivery guard from PRD #538. LTD is deliberately left
    unstubbed: if the guard let the job body run, the sync would raise
    instead of returning cleanly.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )
        # Stand in for the abandoned sweep having failed this child
        # (and rolled the run up) before arq delivered it.
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        await queue_job_store.fail(
            queue_job_id,
            errors={
                "message": "Abandoned keeper_sync_project",
                "type": "AbandonedQueueJob",
            },
        )

    object_store = MockObjectStore()
    _patch_factory_io(
        monkeypatch, object_store=object_store, source_objects={}
    )

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    with capture_logs() as captured:
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

    assert result == "skipped"
    warnings = [
        event
        for event in captured
        if event.get("log_level") == "warning"
        and event.get("queue_job_status") == JobStatus.failed.value
    ]
    assert len(warnings) == 1
    assert warnings[0]["queue_job_kind"] == JobKind.keeper_sync_project.value

    # No job body ran: nothing copied, the reaped row untouched, and the
    # parent run left exactly as the reaper left it.
    assert object_store.objects == {}
    async for session in db_session_dependency():
        async with session.begin():
            store = QueueJobStore(session=session, logger=_logger())
            qj = await store.get(queue_job_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            assert qj.date_started is None

            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.in_progress


@pytest.mark.asyncio
async def test_keeper_sync_project_publishes_run_completed(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finalising a run publishes one ``keeper_sync_run_completed`` event."""
    _manager, events = await build_event_manager(Configuration())

    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    # LTD 404 → the run's single attributed child fails, so the run
    # finalises as partial_failure (success=False).
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
    ctx = make_worker_ctx(
        http_client=http_client, arq_queue=mock_arq, events=events
    )

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

    publisher = events.keeper_sync_run_completed
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == org_slug
    # A keeper-sync run is org-scoped, so the event carries no project.
    assert event.project is None
    assert event.success is False
    assert event.total_count == 1
    assert event.succeeded_count == 0
    assert event.failed_count == 1
    assert event.elapsed >= timedelta(0)


@pytest.mark.asyncio
async def test_keeper_sync_project_failure_captures_to_sentry(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The worker's explicit ``capture_exception`` reaches Sentry on failure.

    Locks the worker-side analogue of the FastAPI exception handler from
    PRD #338 (user stories 2, 3, 23, 24): when ``keeper_sync_project``
    catches an exception in its outer ``except`` block, transitions the
    queue-job to ``failed``, and re-raises, the explicit
    ``sentry_sdk.capture_exception(exc)`` produces exactly one Sentry
    envelope tagged with the worker-keeper-sync component and the
    package ``release``. The structured-log breadcrumb
    (``logger.exception``) and the queue-job ``failed`` transition both
    stay intact — Sentry is additive, never a replacement.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    # Force the LTD product fetch to 404 so ``LtdNotFoundError`` propagates
    # out of ``KeeperSyncService.sync_project`` and hits the worker's
    # outer ``except``.
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(404)
    )
    object_store = MockObjectStore()
    _patch_factory_io(
        monkeypatch, object_store=object_store, source_objects={}
    )

    # Sentry test transport + DSN gating must be in place *before*
    # ``initialize_sentry`` runs — otherwise the wrapper's
    # ``should_enable_sentry`` early-return leaves the SDK uninitialised
    # and the explicit capture never reaches a transport.
    monkeypatch.setenv("SENTRY_DSN", "https://test@example.com/1")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "test")
    real_init = sentry_sdk.init

    def _init_with_test_transport(*args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("transport", TestTransport())
        return real_init(*args, **kwargs)

    monkeypatch.setattr(sentry_sdk, "init", _init_with_test_transport)

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    with sentry_init_fixture():
        initialize_sentry(component="worker-keeper-sync")
        captured = capture_events_fixture(monkeypatch)()

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

        assert len(captured.errors) == 1
        event = captured.errors[0]
        assert event["release"] == pkg_version("docverse-server")
        assert event["tags"]["service"] == "docverse"
        assert event["tags"]["component"] == "worker-keeper-sync"
        exc_values = event["exception"]["values"]
        assert any(exc["type"] == "LtdNotFoundError" for exc in exc_values)
    await ctx["http_client"].aclose()

    # The failure transitions the queue-job + run row are still in place
    # — Sentry is additive to the existing finalisation contract.
    async for session in db_session_dependency():
        async with session.begin():
            queue_job_store = QueueJobStore(session=session, logger=_logger())
            qj = await queue_job_store.get(queue_job_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.partial_failure


@pytest.mark.asyncio
async def test_keeper_sync_project_objectstore_failure_leaves_no_open_txn(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Objectstore resolution raising mid-copy leaks no open transaction.

    Drives the failure deeper than the LTD-404 path: the service has
    started copying and invokes the factory's copier closure, which
    calls ``create_objectstore_for_org``. We make that raise *after* the
    factory has entered the autobegun transaction region. The service's
    per-edition boundary absorbs it and the loop reaches its end — but
    this product has exactly one edition, so *every* attempted edition
    failed and the end-of-run check turns that into a
    ``KeeperSyncSystemicFailureError``: a run that imported nothing is
    an outage, not a partial success, on a project of any size. Marking
    the job ``failed`` still needs a session clean enough to open the
    worker's except-branch transaction; a leaked transaction reproduces
    the original bug (``InvalidRequestError: A transaction is already
    begun on this Session``).
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_ltd(mock_discovery)

    async def _create_objectstore_for_org_raises(
        self: Factory, *, org_id: int, service_label: str
    ) -> MockObjectStore:
        msg = f"Service {service_label!r} not found"
        raise RuntimeError(msg)

    def _create_ltd_s3_source(
        self: Factory, *, bucket: str = "lsst-the-docs"
    ) -> _FakeLtdSource:
        return _FakeLtdSource({})

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _create_objectstore_for_org_raises,
    )
    monkeypatch.setattr(Factory, "create_ltd_s3_source", _create_ltd_s3_source)

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    with pytest.raises(KeeperSyncSystemicFailureError):
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
            assert qj.errors["type"] == "KeeperSyncSystemicFailureError"
            # The edition's own error is chained onto the systemic
            # abort, so the underlying fault stays triageable from the
            # recorded traceback.
            assert "Service 'mock-store' not found" in qj.errors["traceback"]

            # The project's only edition failed, so the job is the run's
            # only child. ``aggregate_activity`` buckets the failed job
            # into ``failed_count``, so the run rolls up
            # ``partial_failure`` — the errors reach the run status
            # without inventing a new one.
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.partial_failure


@pytest.mark.asyncio
async def test_keeper_sync_project_missing_publishing_store_label(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No ``publishing_store_label`` → worker fails fast with clear error."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, publishing_store_label=None
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    object_store = MockObjectStore()
    _patch_factory_io(
        monkeypatch, object_store=object_store, source_objects={}
    )

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    with pytest.raises(RuntimeError, match="publishing_store_label"):
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
            assert "publishing_store_label" in qj.errors["message"]

            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.partial_failure

    # Nothing copied since the worker bailed before constructing the service.
    assert object_store.objects == {}


def _seed_two_edition_ltd(mock_discovery: respx.Router) -> None:
    """Stub LTD with the main edition + one ticket-branch edition."""
    branch_edition = _load("edition_branch_git_refs.json")
    branch_build = _load("build.json")
    branch_build["self_url"] = f"{LTD_BASE}/builds/43"
    branch_build["bucket_root_dir"] = "pipelines/builds/43"
    branch_edition["build_url"] = f"{LTD_BASE}/builds/43"
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200,
            json={
                "editions": [
                    f"{LTD_BASE}/editions/1",
                    f"{LTD_BASE}/editions/2",
                ]
            },
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(
            200, json=_load("edition_main_git_refs.json")
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(200, json=branch_edition)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/42").mock(
        return_value=httpx.Response(200, json=_load("build.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=branch_build)
    )


@pytest.mark.asyncio
async def test_keeper_sync_project_publishes_each_edition_per_iteration(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Full-success multi-edition: per-edition callback fires N times.

    Locks the new contract: every freshly-synced edition gets a
    publish_edition enqueued via the on_edition_synced callback. The
    tail-end self-heal pass observes ``publish_status=pending`` on
    every edition and enqueues nothing extra — guarding against
    double-publish.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_two_edition_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>branch</html>",
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

    publish_jobs = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    # Exactly N=2 publish_edition jobs, one per edition; self-heal
    # found nothing to do at the tail end.
    assert len(publish_jobs) == 2
    publish_slugs = sorted(
        job.kwargs["payload"]["edition_slug"] for job in publish_jobs
    )
    assert publish_slugs == ["__main", "u-jsick-feature"]

    async for session in db_session_dependency():
        async with session.begin():
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
            assert project is not None
            edition_store = EditionStore(session=session, logger=_logger())
            for slug in ("__main", "u-jsick-feature"):
                edition = await edition_store.get_by_slug(
                    project_id=project.id, slug=slug
                )
                assert edition is not None
                assert edition.publish_status == PublishStatus.pending

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            for arq_job in publish_jobs:
                qj = await queue_job_store.get_by_backend_job_id(arq_job.id)
                assert qj is not None
                assert qj.kind == JobKind.publish_edition
                assert qj.keeper_sync_run_id == run_id


def _seed_release_edition_ltd(mock_discovery: respx.Router) -> None:
    """Stub LTD with the main edition + one ``15.2.1`` release edition."""
    release_edition = _load("edition_branch_git_refs.json")
    release_edition["slug"] = "15.2.1"
    release_edition["title"] = "15.2.1"
    release_edition["tracked_refs"] = ["15.2.1"]
    release_build = _load("build.json")
    release_build["self_url"] = f"{LTD_BASE}/builds/43"
    release_build["bucket_root_dir"] = "pipelines/builds/43"
    release_edition["build_url"] = f"{LTD_BASE}/builds/43"
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200,
            json={
                "editions": [
                    f"{LTD_BASE}/editions/1",
                    f"{LTD_BASE}/editions/2",
                ]
            },
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(
            200, json=_load("edition_main_git_refs.json")
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(200, json=release_edition)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/42").mock(
        return_value=httpx.Response(200, json=_load("build.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=release_build)
    )


@pytest.mark.asyncio
async def test_keeper_sync_project_publishes_backfilled_aggregates(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ``15`` / ``15.2`` aggregates get published, not just created.

    The dashboard lists any edition with a current build, so an
    aggregate keeper-sync creates but never publishes would render as a
    link to a 404.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_release_edition_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>release</html>",
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

    publish_jobs = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    publish_slugs = sorted(
        job.kwargs["payload"]["edition_slug"] for job in publish_jobs
    )
    assert publish_slugs == ["15", "15.2", "15.2.1", "__main"]

    async for session in db_session_dependency():
        async with session.begin():
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
            assert project is not None
            edition_store = EditionStore(session=session, logger=_logger())
            for slug in ("15", "15.2"):
                aggregate = await edition_store.get_by_slug(
                    project_id=project.id, slug=slug
                )
                assert aggregate is not None
                assert aggregate.publish_status == PublishStatus.pending


@pytest.mark.asyncio
async def test_keeper_sync_project_locks_every_edition_pointer_write(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every pointer this worker moves is written under its own lock.

    The factory has to hand ``KeeperSyncService`` a real ``LockService``,
    because a project mid-migration publishes natively — through
    ``build_processing`` and ``publish_edition``, both of which take
    ``EDITION_UPDATE`` on the same key — while keeper-sync still polls.

    Also pins the *shape* of the holds, which is what keeps keeper-sync
    out of a deadlock with those workers: keeper-sync takes only
    ``EDITION_UPDATE`` (never ``BUILD_PROCESSING``, so there is no
    ordering to invert against the native path's outer-to-inner
    nesting), and it releases each hold before taking the next, so no
    two editions are ever held at once.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_release_edition_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>release</html>",
    }
    _patch_factory_io(
        monkeypatch,
        object_store=object_store,
        source_objects=source_objects,
    )
    events = install_recording_lock_service(monkeypatch)

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

    project_id = 0
    edition_ids: dict[str, int] = {}
    async for session in db_session_dependency():
        async with session.begin():
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
            assert project is not None
            project_id = project.id
            edition_store = EditionStore(session=session, logger=_logger())
            for slug in ("15.2.1", "15", "15.2"):
                edition = await edition_store.get_by_slug(
                    project_id=project_id, slug=slug
                )
                assert edition is not None
                edition_ids[slug] = edition.id

    def _key(slug: str) -> LockKey:
        return LockKey.for_edition_update(
            org_id=org_id,
            project_id=project_id,
            edition_id=edition_ids[slug],
        )

    # Only EDITION_UPDATE: keeper-sync never nests under BUILD_PROCESSING.
    assert {e.lock_key.lock_class for e in events} == {
        LockClass.EDITION_UPDATE
    }
    enters = [e.lock_key for e in events if e.event == "enter"]
    # The imported release and both backfilled aggregates were each
    # repointed under their own key.
    assert set(enters) >= {_key("15.2.1"), _key("15"), _key("15.2")}
    # Aggregates in ``semver_aggregate_specs`` order: major, then minor.
    assert enters.index(_key("15")) < enters.index(_key("15.2"))
    # Strictly serial: each hold is released before the next is taken.
    assert [e.event for e in events] == ["enter", "exit"] * len(enters)


@pytest.mark.asyncio
async def test_keeper_sync_project_dedups_dashboard_build_cascade(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """N-edition keeper-sync produces exactly one ``dashboard_build`` row.

    The cascade in ``publish_edition`` calls
    ``try_enqueue_dashboard_build_by_id`` after every successful publish.
    Without dedup, an N-edition keeper-sync project would produce N
    ``dashboard_build`` rows for the same project — one per publish
    cascade. The per-project gate keyed on ``(org_id, project_id)``
    collapses the burst to one. This test runs the real
    ``keeper_sync_project`` worker against a 2-edition fixture and then
    drives the cascade by calling ``try_enqueue_dashboard_build_by_id``
    once per publish_edition arq job that the worker enqueued — the
    same call ``publish_edition`` makes at the end of its success path.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_two_edition_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>branch</html>",
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

    publish_jobs = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    assert len(publish_jobs) == 2

    # Simulate the per-publish cascade: every publish_edition success
    # path runs ``try_enqueue_dashboard_build_by_id`` once. The dedup
    # gate at the service level collapses the burst of N attempts into
    # exactly one ``dashboard_build`` row.
    async for session in db_session_dependency():
        async with session.begin():
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
            assert project is not None
            project_id = project.id

        enqueuer = DashboardBuildEnqueuer(
            org_store=OrganizationStore(session=session, logger=_logger()),
            project_store=ProjectStore(session=session, logger=_logger()),
            queue_backend=ArqQueueBackend(
                arq_queue=mock_arq, default_queue_name="docverse:queue"
            ),
            queue_job_store=QueueJobStore(session=session, logger=_logger()),
            logger=_logger(),
        )

        for _ in range(len(publish_jobs)):
            async with session.begin():
                await enqueuer.enqueue_for_project(
                    org_id=org_id, project_id=project_id
                )
                await session.commit()

        async with session.begin():
            rows = await session.execute(
                select(SqlQueueJob).where(
                    SqlQueueJob.kind == JobKind.dashboard_build.value,
                    SqlQueueJob.org_id == org_id,
                    SqlQueueJob.project_id == project_id,
                )
            )
            dashboard_rows = list(rows.scalars().all())

    assert len(dashboard_rows) == 1


@pytest.mark.asyncio
async def test_keeper_sync_project_partial_failure_publishes_succeeded_only(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Partial-failure mid-sync: editions that succeeded still publish.

    Edition 1 (``__main``, build 42) syncs cleanly and the per-edition
    callback enqueues a publish. Edition 2 (the branch edition, build
    43) raises mid-``sync_edition`` because its LTD build reports
    ``uploaded=False``. The service's per-edition boundary absorbs that
    failure, so the job reaches its end rather than failing outright (a
    permanently-broken LTD build must not abort the project's whole
    import) — but it finishes ``completed_with_errors``, not plain
    ``completed``, so a partial import is never indistinguishable from
    a clean one. The ``queue_jobs`` row's ``progress`` carries the
    per-edition detail. Locks two contracts at once: the editions that
    already succeeded end up fully published rather than stranded on
    ``publish_status IS NULL`` (the issue #320 regression), and the
    failure is visible in both the status and the job record.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    # Build 42 succeeds; build 43 reports uploaded=False so sync_edition
    # raises RuntimeError mid-iteration.
    branch_edition = _load("edition_branch_git_refs.json")
    branch_edition["build_url"] = f"{LTD_BASE}/builds/43"
    half_uploaded = _load("build.json")
    half_uploaded["self_url"] = f"{LTD_BASE}/builds/43"
    half_uploaded["bucket_root_dir"] = "pipelines/builds/43"
    half_uploaded["uploaded"] = False
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200,
            json={
                "editions": [
                    f"{LTD_BASE}/editions/1",
                    f"{LTD_BASE}/editions/2",
                ]
            },
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(
            200, json=_load("edition_main_git_refs.json")
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(200, json=branch_edition)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/42").mock(
        return_value=httpx.Response(200, json=_load("build.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=half_uploaded)
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
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
    assert result == "completed_with_errors"

    publish_jobs = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    # Exactly M-1 = 1 publish_edition arq job: the per-edition
    # callback fired for the first edition; the second edition's
    # failure was isolated and skipped.
    assert len(publish_jobs) == 1
    assert publish_jobs[0].kwargs["payload"]["edition_slug"] == "__main"

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
            assert main_edition.publish_status == PublishStatus.pending

            # The branch edition's row exists (the ensure-edition
            # transaction committed before sync_build raised) but its
            # publish_status is still NULL because the callback never
            # ran for it.
            branch = await edition_store.get_by_slug(
                project_id=project.id, slug="u-jsick-feature"
            )
            assert branch is not None
            assert branch.publish_status is None

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            qj = await queue_job_store.get(queue_job_id)
            assert qj is not None
            assert qj.status == JobStatus.completed_with_errors
            assert qj.progress is not None
            assert qj.progress["edition_failure_count"] == 1
            recorded = qj.progress["edition_failures"]
            assert len(recorded) == 1
            assert recorded[0]["ltd_edition_slug"] == "u-jsick-feature"
            assert recorded[0]["error_type"] == "RuntimeError"
            assert "uploaded=False" in recorded[0]["error_message"]

            publish_qj = await queue_job_store.get_by_backend_job_id(
                publish_jobs[0].id
            )
            assert publish_qj is not None
            assert publish_qj.kind == JobKind.publish_edition
            assert publish_qj.keeper_sync_run_id == run_id
            assert publish_qj.edition_id == main_edition.id

            # The isolated per-edition failure does not paint the run
            # red: the publish child is still pending, so the run stays
            # in_progress rather than finalising to partial_failure.
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.in_progress


@pytest.mark.asyncio
async def test_keeper_sync_project_permanent_denial_completes_with_errors(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A zero-import run of permanent faults completes rather than fails.

    The roundtable-dev shape behind this fix: every other edition is a
    ``lifecycle_preemptive`` tombstone (neutral for ``contacted_ltd``)
    and the one edition that reaches LTD is denied at both
    ``builds/<slug>/`` and ``v/<slug>/``. Nothing is imported, so the
    end-of-run systemic check used to fail the job — and the 5-minute
    tier cron then replayed an identically-failing job forever while
    Sentry fired each time.

    A denial is permanent by construction, so the run belongs on the
    partial-success path: the job finishes ``completed_with_errors``
    with the edition named in ``progress``, and the parent run rolls up
    off that rather than off a raised exception.

    This product's only edition is ``main``, which LTD serves from the
    product root — there is no ``v/`` sibling to recover from — so the
    denial survives the edition-prefix fallback.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_ltd(mock_discovery)

    object_store = MockObjectStore()
    _patch_factory_io(
        monkeypatch,
        object_store=object_store,
        source_objects={
            "pipelines/builds/42/index.html": b"<html>denied</html>"
        },
        denied_prefixes=frozenset({"pipelines/builds/42/"}),
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
    assert result == "completed_with_errors"

    # Nothing was imported, so nothing was published either.
    assert not get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    assert object_store.objects == {}

    async for session in db_session_dependency():
        async with session.begin():
            queue_job_store = QueueJobStore(session=session, logger=_logger())
            qj = await queue_job_store.get(queue_job_id)
            assert qj is not None
            assert qj.status == JobStatus.completed_with_errors
            assert qj.errors is None
            assert qj.progress is not None
            assert qj.progress["edition_failure_count"] == 1
            recorded = qj.progress["edition_failures"]
            assert recorded[0]["ltd_edition_slug"] == "main"
            assert recorded[0]["error_type"] == "LtdSourceAccessDeniedError"


@pytest.mark.asyncio
async def test_keeper_sync_project_self_heals_all_short_circuited_editions(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Re-run with N short-circuits + N unpublished builds → N self-heals.

    Locks the tail-end self-heal pass: when every edition's build
    short-circuits but no edition's current build has a publish on
    record (e.g. their builds pre-date the publish enqueue path), the
    second pass enqueues N publishes via
    :func:`_self_heal_unpublished_editions` since the per-edition
    callback skips short-circuited builds.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        first_qj = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_two_edition_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>branch</html>",
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

    payload: dict[str, Any] = {
        "org_id": org_id,
        "org_slug": org_slug,
        "run_id": run_id,
        "queue_job_id": first_qj,
        "ltd_slug": "pipelines",
        "ltd_base_url": LTD_BASE,
    }
    first_result = await keeper_sync_project(ctx, payload)
    assert first_result == "completed"
    publish_after_first = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    assert len(publish_after_first) == 2

    # Erase the publish traces on both editions so a re-run that
    # short-circuits has work to do at the tail end.
    for slug in ("__main", "u-jsick-feature"):
        await _clear_publish_traces(org_id=org_id, edition_slug=slug)

    async with db_session.begin():
        second_qj = await _seed_project_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="test-arq-project-2",
        )
    payload["queue_job_id"] = second_qj
    second_result = await keeper_sync_project(ctx, payload)
    await ctx["http_client"].aclose()
    assert second_result == "completed"

    publish_after_second = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    # Two more publishes — one per short-circuited edition — enqueued
    # by the tail-end self-heal pass.
    assert len(publish_after_second) == 4
    self_heal_slugs = sorted(
        job.kwargs["payload"]["edition_slug"]
        for job in publish_after_second[2:]
    )
    assert self_heal_slugs == ["__main", "u-jsick-feature"]

    async for session in db_session_dependency():
        async with session.begin():
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
            assert project is not None
            edition_store = EditionStore(session=session, logger=_logger())
            for slug in ("__main", "u-jsick-feature"):
                edition = await edition_store.get_by_slug(
                    project_id=project.id, slug=slug
                )
                assert edition is not None
                assert edition.publish_status == PublishStatus.pending


async def _clear_publish_traces(
    *, org_id: int, edition_slug: str
) -> tuple[int, int]:
    """Erase every trace that a publish was enqueued for one edition.

    Simulates the lost-enqueue shapes described on
    ``_self_heal_unpublished_aggregates``: the aggregate row still points
    at the release build, but the edition's ``publish_status``, its
    ``edition_build_history`` row for that build, and the
    ``publish_edition`` queue job all never happened. Returns the
    ``(project_id, edition_id)`` pair for follow-up assertions.
    """
    async for session in db_session_dependency():
        async with session.begin():
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
            assert project is not None
            edition_store = EditionStore(session=session, logger=_logger())
            edition = await edition_store.get_by_slug(
                project_id=project.id, slug=edition_slug
            )
            assert edition is not None
            await session.execute(
                update(SqlEdition)
                .where(SqlEdition.id == edition.id)
                .values(publish_status=None)
            )
            await session.execute(
                delete(SqlEditionBuildHistory).where(
                    SqlEditionBuildHistory.edition_id == edition.id
                )
            )
            await session.execute(
                delete(SqlQueueJob).where(
                    SqlQueueJob.edition_id == edition.id,
                    SqlQueueJob.kind == JobKind.publish_edition.value,
                )
            )
            return project.id, edition.id
    msg = "No database session available"
    raise RuntimeError(msg)


@pytest.mark.asyncio
async def test_keeper_sync_project_self_heals_unpublished_aggregate(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A semver aggregate whose publish enqueue was lost is re-enqueued.

    The ``15`` / ``15.2`` aggregates are enqueued exactly once, from the
    ``on_edition_synced`` callback, and ``sync_project`` deliberately
    swallows callback failures — so a failed enqueue (or a worker death
    between the backfill's commit and the enqueue) leaves the aggregate
    pointing at the release build with no KV pointer ever written. A
    re-sync of the unchanged edition skips the backfill entirely (its
    ``aggregates_backfilled_build_id`` marker already names this build),
    so nothing re-enqueues it via the outcome path; the tail-end
    self-heal pass must recover it from persistent state instead.

    The recovery run has to carry a signal that an aggregate could have
    moved, or the pass is gated off (see
    ``_run_may_have_moved_aggregates``) — here LTD rebuilds ``main`` onto
    a new build, which is both the cheapest such signal and the shape a
    real project hits, since the tiers keep polling a project whose
    ``main`` moves long after its releases have frozen. The rebuilt
    edition is deliberately *not* the release one: a fresh release build
    would re-run the backfill and re-enqueue the aggregates through the
    outcome path, which is the path this test is proving unnecessary.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        first_qj = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_release_edition_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>release</html>",
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

    payload: dict[str, Any] = {
        "org_id": org_id,
        "org_slug": org_slug,
        "run_id": run_id,
        "queue_job_id": first_qj,
        "ltd_slug": "pipelines",
        "ltd_base_url": LTD_BASE,
    }
    assert await keeper_sync_project(ctx, payload) == "completed"

    # Rewind the ``15.2`` aggregate to the lost-enqueue shape.
    project_id, aggregate_id = await _clear_publish_traces(
        org_id=org_id, edition_slug="15.2"
    )
    # ``main`` rebuilds, so the recovery run is not an idle poll.
    _repoint_ltd_main_edition(mock_discovery, build_slug="44")
    source_objects["pipelines/builds/44/index.html"] = b"<html>main v2</html>"
    publish_before = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )

    async with db_session.begin():
        second_qj = await _seed_project_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="test-arq-project-2",
        )
    payload["queue_job_id"] = second_qj
    assert await keeper_sync_project(ctx, payload) == "completed"
    await ctx["http_client"].aclose()

    publish_after = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    healed = publish_after[len(publish_before) :]
    healed_slugs = [job.kwargs["payload"]["edition_slug"] for job in healed]
    # ``__main`` is the rebuilt edition's own publish, enqueued by the
    # per-edition callback; ``15.2`` is the aggregate the tail-end pass
    # recovered.
    assert healed_slugs == ["__main", "15.2"]
    aggregate_job = healed[1]

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            aggregate = await edition_store.get_by_slug(
                project_id=project_id, slug="15.2"
            )
            assert aggregate is not None
            assert aggregate.id == aggregate_id
            assert aggregate.publish_status == PublishStatus.pending

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            healed_qj = await queue_job_store.get_by_backend_job_id(
                aggregate_job.id
            )
            assert healed_qj is not None
            assert healed_qj.kind == JobKind.publish_edition
            assert healed_qj.keeper_sync_run_id == run_id


@pytest.mark.asyncio
async def test_keeper_sync_project_self_heal_skips_published_aggregates(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Aggregates with a publish already in flight are not re-enqueued.

    The first sync leaves ``15`` / ``15.2`` with a ``pending``
    ``edition_build_history`` row for the build they point at. The
    aggregate self-heal must read that as "the publish for this pair was
    already enqueued" and leave the pair alone, so a steady-state
    re-sync does not enqueue a duplicate ``publish_edition`` on every
    reconciliation tick.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        first_qj = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_release_edition_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>release</html>",
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

    payload: dict[str, Any] = {
        "org_id": org_id,
        "org_slug": org_slug,
        "run_id": run_id,
        "queue_job_id": first_qj,
        "ltd_slug": "pipelines",
        "ltd_base_url": LTD_BASE,
    }
    assert await keeper_sync_project(ctx, payload) == "completed"
    publish_after_first = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    assert sorted(
        job.kwargs["payload"]["edition_slug"] for job in publish_after_first
    ) == ["15", "15.2", "15.2.1", "__main"]

    async with db_session.begin():
        second_qj = await _seed_project_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="test-arq-project-2",
        )
    payload["queue_job_id"] = second_qj
    assert await keeper_sync_project(ctx, payload) == "completed"
    await ctx["http_client"].aclose()

    publish_after_second = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    assert len(publish_after_second) == len(publish_after_first)


async def _project_id_for(*, org_id: int) -> int:
    """Return the Docverse id of the org's synced ``pipelines`` project."""
    async for session in db_session_dependency():
        async with session.begin():
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
            assert project is not None
            return project.id
    msg = "No database session available"
    raise RuntimeError(msg)


def _spy_aggregate_scan(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Record every project-wide edition scan the worker performs.

    ``EditionStore.list_all_by_project`` has exactly one caller on the
    ``keeper_sync_project`` path — the aggregate self-heal pass — so the
    returned list is a direct measure of how often that pass ran.
    """
    project_scans: list[int] = []
    original_scan = EditionStore.list_all_by_project

    async def _scan(self: EditionStore, project_id: int) -> list[Edition]:
        project_scans.append(project_id)
        return await original_scan(self, project_id)

    monkeypatch.setattr(EditionStore, "list_all_by_project", _scan)
    return project_scans


@pytest.mark.asyncio
async def test_keeper_sync_project_skips_aggregate_self_heal_when_idle(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A steady-state re-sync does not scan the project for aggregates.

    Nothing in a run where every edition short-circuited and no
    aggregate outcome was emitted can have moved an aggregate, so the
    self-heal pass must not pay a full-project edition scan (plus a
    history lookup per aggregate) on every five-minute poll, forever,
    for every synced project.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        first_qj = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_release_edition_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>release</html>",
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

    payload: dict[str, Any] = {
        "org_id": org_id,
        "org_slug": org_slug,
        "run_id": run_id,
        "queue_job_id": first_qj,
        "ltd_slug": "pipelines",
        "ltd_base_url": LTD_BASE,
    }
    assert await keeper_sync_project(ctx, payload) == "completed"

    # Spy only over the second, steady-state run.
    project_scans = _spy_aggregate_scan(monkeypatch)

    async with db_session.begin():
        second_qj = await _seed_project_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="test-arq-project-2",
        )
    payload["queue_job_id"] = second_qj
    assert await keeper_sync_project(ctx, payload) == "completed"
    await ctx["http_client"].aclose()

    assert project_scans == []


@pytest.mark.asyncio
async def test_keeper_sync_project_scans_aggregates_after_a_build_moved(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A run that imported a fresh build still scans for aggregates.

    The mirror of
    ``test_keeper_sync_project_skips_aggregate_self_heal_when_idle``:
    identical setup, except LTD rebuilds ``main`` before the second
    sync. A non-short-circuited build is the signal that the backfill
    could have run — including the paths where it committed an aggregate
    and then lost its outcome — so the pass must not be gated off.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        first_qj = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_release_edition_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>release</html>",
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

    payload: dict[str, Any] = {
        "org_id": org_id,
        "org_slug": org_slug,
        "run_id": run_id,
        "queue_job_id": first_qj,
        "ltd_slug": "pipelines",
        "ltd_base_url": LTD_BASE,
    }
    assert await keeper_sync_project(ctx, payload) == "completed"

    project_id = await _project_id_for(org_id=org_id)

    _repoint_ltd_main_edition(mock_discovery, build_slug="44")
    source_objects["pipelines/builds/44/index.html"] = b"<html>main v2</html>"

    # Spy only over the second run.
    project_scans = _spy_aggregate_scan(monkeypatch)

    async with db_session.begin():
        second_qj = await _seed_project_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="test-arq-project-2",
        )
    payload["queue_job_id"] = second_qj
    assert await keeper_sync_project(ctx, payload) == "completed"
    await ctx["http_client"].aclose()

    assert project_scans == [project_id]


async def _current_build_pairs(
    *, project_id: int, slugs: Sequence[str]
) -> list[tuple[int, int]]:
    """Return each named edition's ``(edition_id, current_build_id)``."""
    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            pairs: list[tuple[int, int]] = []
            for slug in slugs:
                edition = await edition_store.get_by_slug(
                    project_id=project_id, slug=slug
                )
                assert edition is not None
                assert edition.current_build_id is not None
                pairs.append((edition.id, edition.current_build_id))
            return pairs
    msg = "No database session available"
    raise RuntimeError(msg)


def _spy_history_lookups(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[list[tuple[int, int]]], list[tuple[int, int]]]:
    """Record batched and per-pair ``edition_build_history`` lookups.

    Returns ``(batched, single)``: the pair list handed to every
    ``list_by_edition_build_pairs`` call, and the pair every
    ``get_by_edition_and_build`` call asked about.
    """
    batched: list[list[tuple[int, int]]] = []
    single: list[tuple[int, int]] = []
    original_batched = EditionBuildHistoryStore.list_by_edition_build_pairs
    original_single = EditionBuildHistoryStore.get_by_edition_and_build

    async def _batched(
        self: EditionBuildHistoryStore, pairs: Sequence[tuple[int, int]]
    ) -> list[EditionBuildHistory]:
        batched.append(list(pairs))
        return await original_batched(self, pairs)

    async def _single(
        self: EditionBuildHistoryStore, *, edition_id: int, build_id: int
    ) -> EditionBuildHistory | None:
        single.append((edition_id, build_id))
        return await original_single(
            self, edition_id=edition_id, build_id=build_id
        )

    monkeypatch.setattr(
        EditionBuildHistoryStore, "list_by_edition_build_pairs", _batched
    )
    monkeypatch.setattr(
        EditionBuildHistoryStore, "get_by_edition_and_build", _single
    )
    return batched, single


@pytest.mark.asyncio
async def test_keeper_sync_project_batches_aggregate_history_lookups(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The aggregate pass reads every history row in one query.

    A migrated project carries an ``N`` / ``N.M`` row per release
    series, and resolving each one's publish state through
    ``get_by_edition_and_build`` cost a BEGIN/SELECT/COMMIT apiece. One
    ``IN`` query over the whole pair set replaces them.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        first_qj = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_release_edition_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>release</html>",
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

    payload: dict[str, Any] = {
        "org_id": org_id,
        "org_slug": org_slug,
        "run_id": run_id,
        "queue_job_id": first_qj,
        "ltd_slug": "pipelines",
        "ltd_base_url": LTD_BASE,
    }
    assert await keeper_sync_project(ctx, payload) == "completed"

    project_id = await _project_id_for(org_id=org_id)
    aggregate_pairs = await _current_build_pairs(
        project_id=project_id, slugs=("15", "15.2")
    )

    # Rebuild ``main`` so the second run opens the aggregate gate.
    _repoint_ltd_main_edition(mock_discovery, build_slug="44")
    source_objects["pipelines/builds/44/index.html"] = b"<html>main v2</html>"

    batched, single = _spy_history_lookups(monkeypatch)

    async with db_session.begin():
        second_qj = await _seed_project_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="test-arq-project-2",
        )
    payload["queue_job_id"] = second_qj
    assert await keeper_sync_project(ctx, payload) == "completed"
    await ctx["http_client"].aclose()

    assert len(batched) == 1
    assert sorted(batched[0]) == sorted(aggregate_pairs)
    # No aggregate fell back to the per-pair lookup (the LTD leg still
    # uses it for its own short-circuited editions).
    assert not set(aggregate_pairs) & set(single)


def _repoint_ltd_main_edition(
    mock_discovery: respx.Router, *, build_slug: str = "43"
) -> None:
    """Advance LTD's ``main`` edition onto a second, newer build.

    Replaces the ``/editions/1`` and ``/builds/<build_slug>`` routes
    seeded by :func:`_seed_ltd` so a follow-up sync sees a fresh
    ``date_rebuilt`` pointing at that LTD build. ``respx`` replaces
    routes with an identical pattern, so the later ``.mock()`` wins.

    ``build_slug`` defaults to the next build after :func:`_seed_ltd`'s
    single ``42``; fixtures that already use ``43`` for a second edition
    (:func:`_seed_release_edition_ltd`) pass a free slug instead.
    """
    edition = _load("edition_main_git_refs.json")
    edition["build_url"] = f"{LTD_BASE}/builds/{build_slug}"
    edition["date_rebuilt"] = "2026-05-02T18:30:00.000000+00:00"
    build = _load("build.json")
    build["self_url"] = f"{LTD_BASE}/builds/{build_slug}"
    build["slug"] = build_slug
    build["bucket_root_dir"] = f"pipelines/builds/{build_slug}"
    build["date_created"] = "2026-05-02T18:25:00.000000+00:00"
    build["published_url"] = f"https://pipelines.lsst.io/builds/{build_slug}"
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(200, json=edition)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/{build_slug}").mock(
        return_value=httpx.Response(200, json=build)
    )


async def _lose_repoint_publish_enqueue(
    *, org_id: int, edition_slug: str, surviving_status: PublishStatus
) -> tuple[int, int, int]:
    """Erase the publish traces for an edition's *current* build only.

    The LTD-leg counterpart to :func:`_clear_publish_traces`: rather than
    the pre-enqueue-era shape (no publish ever ran for this edition), this
    models the repoint shape. The edition was published against an
    earlier build — so its edition-level ``publish_status`` is still set,
    a single slot ``set_current_build`` never clears — and then advanced
    onto a new build whose publish enqueue was lost, leaving the new
    ``(edition, build)`` pair with no ``edition_build_history`` row and no
    ``publish_edition`` queue job. The earlier build's history row is left
    intact, exactly as a real repoint leaves it.

    Returns ``(project_id, edition_id, current_build_id)``.
    """
    async for session in db_session_dependency():
        async with session.begin():
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
            assert project is not None
            edition_store = EditionStore(session=session, logger=_logger())
            edition = await edition_store.get_by_slug(
                project_id=project.id, slug=edition_slug
            )
            assert edition is not None
            assert edition.current_build_id is not None
            build_id = edition.current_build_id
            await session.execute(
                update(SqlEdition)
                .where(SqlEdition.id == edition.id)
                .values(publish_status=surviving_status.value)
            )
            await session.execute(
                delete(SqlEditionBuildHistory).where(
                    SqlEditionBuildHistory.edition_id == edition.id,
                    SqlEditionBuildHistory.build_id == build_id,
                )
            )
            await session.execute(
                delete(SqlQueueJob).where(
                    SqlQueueJob.edition_id == edition.id,
                    SqlQueueJob.build_id == build_id,
                    SqlQueueJob.kind == JobKind.publish_edition.value,
                )
            )
            return project.id, edition.id, build_id
    msg = "No database session available"
    raise RuntimeError(msg)


@pytest.mark.asyncio
async def test_keeper_sync_project_self_heals_repointed_edition(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A repointed LTD edition whose publish enqueue was lost is healed.

    The edition-level ``publish_status`` is a single slot that survives a
    repoint — ``set_current_build`` never clears it — so an edition that
    published for build ``42`` and then advanced onto build ``43`` with a
    lost enqueue still reads ``published``. Deciding "unpublished" from
    that column leaves the new build unpublished forever, because the
    LTD leg is the only path that can publish a short-circuited build.
    The self-heal must therefore decide per ``(edition, current_build)``
    pair via the ``edition_build_history`` row.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        first_qj = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
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

    payload: dict[str, Any] = {
        "org_id": org_id,
        "org_slug": org_slug,
        "run_id": run_id,
        "queue_job_id": first_qj,
        "ltd_slug": "pipelines",
        "ltd_base_url": LTD_BASE,
    }
    assert await keeper_sync_project(ctx, payload) == "completed"

    # LTD rebuilds ``main`` onto a second build with different content,
    # so the second pass imports a new Docverse build and repoints.
    _repoint_ltd_main_edition(mock_discovery)
    source_objects["pipelines/builds/43/index.html"] = b"<html>v2</html>"
    async with db_session.begin():
        second_qj = await _seed_project_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="test-arq-project-2",
        )
    payload["queue_job_id"] = second_qj
    assert await keeper_sync_project(ctx, payload) == "completed"

    # Rewind the repoint to the lost-enqueue shape: the edition still
    # reads ``published`` from build 42's publish, but the new pair has
    # no history row and no queue job.
    (
        project_id,
        edition_id,
        repointed_build_id,
    ) = await _lose_repoint_publish_enqueue(
        org_id=org_id,
        edition_slug="__main",
        surviving_status=PublishStatus.published,
    )
    publish_before = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )

    async with db_session.begin():
        third_qj = await _seed_project_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="test-arq-project-3",
        )
    payload["queue_job_id"] = third_qj
    assert await keeper_sync_project(ctx, payload) == "completed"
    await ctx["http_client"].aclose()

    publish_after = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    healed = publish_after[len(publish_before) :]
    assert [job.kwargs["payload"]["edition_slug"] for job in healed] == [
        "__main"
    ]
    assert healed[0].kwargs["payload"]["build_id"] == repointed_build_id

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            edition = await edition_store.get_by_slug(
                project_id=project_id, slug="__main"
            )
            assert edition is not None
            assert edition.id == edition_id
            assert edition.publish_status == PublishStatus.pending

            history_store = EditionBuildHistoryStore(
                session=session, logger=_logger()
            )
            history = await history_store.get_by_edition_and_build(
                edition_id=edition_id, build_id=repointed_build_id
            )
            assert history is not None
            assert history.publish_status == PublishStatus.pending

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            healed_qj = await queue_job_store.get_by_backend_job_id(
                healed[0].id
            )
            assert healed_qj is not None
            assert healed_qj.kind == JobKind.publish_edition
            assert healed_qj.keeper_sync_run_id == run_id


async def _settle_publish(
    *, org_id: int, edition_slug: str, status: PublishStatus
) -> None:
    """Drive one edition's current-build publish to a terminal status.

    Stands in for the ``publish_edition`` worker's write-back: it sets
    both the edition-level ``publish_status`` and the
    ``edition_build_history`` row for the edition's current build, which
    is the shape a steady-state re-sync finds.
    """
    async for session in db_session_dependency():
        async with session.begin():
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
            assert project is not None
            edition_store = EditionStore(session=session, logger=_logger())
            edition = await edition_store.get_by_slug(
                project_id=project.id, slug=edition_slug
            )
            assert edition is not None
            assert edition.current_build_id is not None
            await session.execute(
                update(SqlEdition)
                .where(SqlEdition.id == edition.id)
                .values(publish_status=status.value)
            )
            await session.execute(
                update(SqlEditionBuildHistory)
                .where(
                    SqlEditionBuildHistory.edition_id == edition.id,
                    SqlEditionBuildHistory.build_id
                    == edition.current_build_id,
                )
                .values(publish_status=status.value)
            )
            return
    msg = "No database session available"
    raise RuntimeError(msg)


@pytest.mark.parametrize(
    "settled_status", [PublishStatus.published, PublishStatus.failed]
)
@pytest.mark.asyncio
async def test_keeper_sync_project_self_heal_skips_settled_editions(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
    settled_status: PublishStatus,
) -> None:
    """A terminal publish for the current pair is not re-enqueued.

    The per-pair rule has to carry the dedup the edition-level column
    used to supply: once the ``(edition, current_build)`` pair carries a
    ``published`` or ``failed`` publish, a steady-state re-sync must
    leave it alone rather than burn a KV write on every reconciliation
    tick. (``pending`` — a publish still in flight — is covered by
    ``test_keeper_sync_project_short_circuit_skips_publish_enqueue``.)
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        first_qj = await _seed_project_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _seed_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
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

    payload: dict[str, Any] = {
        "org_id": org_id,
        "org_slug": org_slug,
        "run_id": run_id,
        "queue_job_id": first_qj,
        "ltd_slug": "pipelines",
        "ltd_base_url": LTD_BASE,
    }
    assert await keeper_sync_project(ctx, payload) == "completed"
    publish_after_first = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    assert len(publish_after_first) == 1

    await _settle_publish(
        org_id=org_id, edition_slug="__main", status=settled_status
    )

    async with db_session.begin():
        second_qj = await _seed_project_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="test-arq-project-2",
        )
    payload["queue_job_id"] = second_qj
    assert await keeper_sync_project(ctx, payload) == "completed"
    await ctx["http_client"].aclose()

    publish_after_second = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    assert len(publish_after_second) == len(publish_after_first)
