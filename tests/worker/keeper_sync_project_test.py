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
from safir.testing.sentry import (
    TestTransport,
    capture_events_fixture,
    sentry_init_fixture,
)
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    BuildStatus,
    JobKind,
    KeeperSyncConfig,
    KeeperSyncRunStatus,
    OrganizationCreate,
)
from docverse.client.models.queue_enums import PublishStatus
from docverse.dbschema.edition import SqlEdition
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.dbschema.organization import SqlOrganization
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.queue import JobStatus
from docverse.factory import Factory
from docverse.sentry import initialize_sentry
from docverse.services.dashboard.enqueue import DashboardBuildEnqueuer
from docverse.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.keeper_sync import KeeperSyncStateStore, ResourceType
from docverse.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse.storage.ltd import LtdNotFoundError
from docverse.storage.objectstore import MockObjectStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_backend import ArqQueueBackend
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.keeper_sync import keeper_sync_project
from tests.support.arq_testing import get_jobs_by_name, register_queue
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
        org_id=org_id, kind="backfill", status="in_progress"
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
async def test_keeper_sync_project_runs_service_and_enqueues_publish(  # noqa: PLR0915
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
async def test_keeper_sync_project_self_heals_unpublished_short_circuit(  # noqa: PLR0915
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Short-circuited sync re-publishes an edition that was never published.

    Simulates the staging shape from the first sync runs that landed
    before the publish-enqueue path existed: an edition has its
    ``current_build_id`` set, the build is ``completed``, and a
    ``keeper_sync_state`` row matches LTD's ``date_rebuilt`` — but
    ``publish_status`` is ``NULL`` because no publish was ever enqueued
    against this build. On the next sync run the build sync still
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

    # Reset the edition's publish_status to NULL to mimic data that
    # landed before the publish-enqueue path existed.
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
            await session.execute(
                update(SqlEdition)
                .where(SqlEdition.id == main_edition.id)
                .values(publish_status=None)
            )

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

    try:
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
            assert event["release"] == pkg_version("docverse")
            assert event["tags"]["service"] == "docverse"
            assert event["tags"]["component"] == "worker-keeper-sync"
            exc_values = event["exception"]["values"]
            assert any(exc["type"] == "LtdNotFoundError" for exc in exc_values)
    finally:
        # ``initialize_sentry`` writes ``service`` and ``component`` to
        # the global scope; strip them so this test does not bleed tags
        # into any later test that asserts their absence.
        scope = sentry_sdk.get_global_scope()
        scope.remove_tag("service")
        scope.remove_tag("component")
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
async def test_keeper_sync_project_objectstore_failure_marks_queue_job_failed(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Objectstore resolution raising → queue job ``failed``, no leaked txn.

    Drives the failure deeper than the LTD-404 path: the service has
    started copying and invokes the factory's copier closure, which calls
    ``create_objectstore_for_org``. We make that raise, exercising the
    worker's except branch after the factory has entered the autobegun
    transaction region. Without the fix this reproduces
    ``InvalidRequestError: A transaction is already begun on this Session``.
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

    with pytest.raises(RuntimeError, match="Service 'mock-store' not found"):
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
            assert "Service 'mock-store' not found" in qj.errors["message"]

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
    """Partial-failure mid-sync: editions 1..M-1 publish; M..N untouched.

    Edition 1 (``__main``, build 42) syncs cleanly and the per-edition
    callback enqueues a publish. Edition 2 (the branch edition, build
    43) raises mid-``sync_edition`` because its LTD build reports
    ``uploaded=False`` — the exception propagates out of sync_project
    and the worker marks the queue_jobs row failed. Locks the new
    contract: a partial mid-sync failure leaves the editions that
    already succeeded fully published, instead of stranding all of
    them on ``publish_status IS NULL`` until the next reconciliation
    tick (the issue #320 regression).
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

    with pytest.raises(RuntimeError, match="uploaded=False"):
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

    publish_jobs = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    # Exactly M-1 = 1 publish_edition arq job: the per-edition
    # callback fired for the first edition before sync_project raised
    # on the second.
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
            assert qj.status == JobStatus.failed

            publish_qj = await queue_job_store.get_by_backend_job_id(
                publish_jobs[0].id
            )
            assert publish_qj is not None
            assert publish_qj.kind == JobKind.publish_edition
            assert publish_qj.keeper_sync_run_id == run_id
            assert publish_qj.edition_id == main_edition.id


@pytest.mark.asyncio
async def test_keeper_sync_project_self_heals_all_short_circuited_editions(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Re-run with N short-circuits + N null publish_status → N self-heals.

    Locks the tail-end self-heal pass: when every edition's build
    short-circuits but every edition is sitting on
    ``publish_status IS NULL`` (e.g. their builds pre-date the publish
    enqueue path), the second pass enqueues N publishes via
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

    # Null out publish_status on both editions so a re-run that
    # short-circuits has work to do at the tail end.
    async for session in db_session_dependency():
        async with session.begin():
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
            assert project is not None
            await session.execute(
                update(SqlEdition)
                .where(SqlEdition.project_id == project.id)
                .values(publish_status=None)
            )

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
