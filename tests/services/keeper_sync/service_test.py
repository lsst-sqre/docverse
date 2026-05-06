"""Integration tests for ``KeeperSyncService``.

Wires real ``ProjectService`` / ``BuildStore`` / ``EditionStore`` against
the alembic test DB, stubs LTD HTTP via ``respx``, and uses a real
:class:`BuildContentCopier` with an in-memory LTD source plus
:class:`MockObjectStore` so the destination assertions can verify both
state-store rows and copied object bytes.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
import respx
import structlog
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    BuildCreate,
    BuildStatus,
    EditionKind,
    OrganizationCreate,
    TrackingMode,
)
from docverse.dbschema.build import SqlBuild
from docverse.services.keeper_sync.copier import BuildContentCopier
from docverse.services.keeper_sync.service import (
    KeeperSyncContext,
    KeeperSyncService,
    _now,
)
from docverse.services.project import ProjectService
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.keeper_sync import KeeperSyncStateStore, ResourceType
from docverse.storage.ltd import LtdClient, LtdSourceProtocol
from docverse.storage.objectstore import MockObjectStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

FIXTURES_DIR = (
    Path(__file__).parent.parent.parent / "storage" / "ltd" / "fixtures"
)
LTD_BASE = "https://keeper.lsst.codes"


def _load(name: str) -> dict[str, object]:
    payload: dict[str, object] = json.loads((FIXTURES_DIR / name).read_text())
    return payload


class _FakeLtdSource(LtdSourceProtocol):
    """In-memory LTD source for service-level integration tests."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self._objects = objects

    async def list_keys(self, *, prefix: str) -> list[str]:
        return [k for k in self._objects if k.startswith(prefix)]

    async def download_object(self, *, key: str) -> bytes:
        return self._objects[key]


@pytest_asyncio.fixture
async def http_client() -> AsyncGenerator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


async def _seed_org(session: AsyncSession, *, slug: str = "ks-svc") -> int:
    logger = structlog.get_logger("test")
    store = OrganizationStore(session=session, logger=logger)
    org = await store.create(
        OrganizationCreate(
            slug=slug,
            title="ks-svc",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id


def _build_service(
    session: AsyncSession,
    http_client: httpx.AsyncClient,
    object_store: MockObjectStore,
    source_objects: dict[str, bytes],
) -> KeeperSyncService:
    """Construct a real ``KeeperSyncService`` against the test DB."""
    logger = structlog.get_logger("test")
    org_store = OrganizationStore(session=session, logger=logger)
    project_store = ProjectStore(session=session, logger=logger)
    edition_store = EditionStore(session=session, logger=logger)
    build_store = BuildStore(session=session, logger=logger)
    state_store = KeeperSyncStateStore(session=session, logger=logger)
    project_service = ProjectService(
        store=project_store,
        org_store=org_store,
        edition_store=edition_store,
        logger=logger,
    )
    ltd_client = LtdClient(
        http_client=http_client,
        base_url=LTD_BASE,
        logger=logger,
        base_backoff_seconds=0.0,
    )
    source = _FakeLtdSource(source_objects)
    copier = BuildContentCopier(
        source=source, destination=object_store, logger=logger
    )

    async def copy_callable(source_prefix: str, dest_prefix: str) -> object:
        return await copier.copy_build(
            source_prefix=source_prefix, dest_prefix=dest_prefix
        )

    context = KeeperSyncContext(
        org_store=org_store,
        project_store=project_store,
        project_service=project_service,
        edition_store=edition_store,
        build_store=build_store,
        state_store=state_store,
    )
    return KeeperSyncService(
        session=session,
        context=context,
        ltd_client=ltd_client,
        copy_callable=copy_callable,  # type: ignore[arg-type]
        logger=logger,
    )


def _seed_ltd(
    mock_discovery: respx.Router,
    *,
    editions_payload: list[dict] | None = None,  # type: ignore[type-arg]
    edition_main: dict | None = None,  # type: ignore[type-arg]
    build_payload: dict | None = None,  # type: ignore[type-arg]
) -> None:
    """Stub the canonical LTD endpoints for the pipelines product."""
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200,
            json={
                "editions": [
                    f"{LTD_BASE}/editions/1",
                ]
            }
            if editions_payload is None
            else {"editions": [e["self_url"] for e in editions_payload]},
        )
    )
    edition_main_payload = (
        edition_main
        if edition_main is not None
        else _load("edition_main_git_refs.json")
    )
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(200, json=edition_main_payload)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/42").mock(
        return_value=httpx.Response(
            200,
            json=build_payload
            if build_payload is not None
            else _load("build.json"),
        )
    )


@pytest.mark.asyncio
async def test_sync_project_creates_project_edition_and_build(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Brand-new project: project + ``__main`` edition + build all created."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    _seed_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
        "pipelines/builds/42/assets/app.js": b"console.log(1)",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    build_store = BuildStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    state_store = KeeperSyncStateStore(
        session=db_session, logger=structlog.get_logger("test")
    )

    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        assert result.docverse_project_id == project.id

        # ``main`` LTD edition must update Docverse's auto-created
        # ``__main`` edition rather than create a duplicate.
        main_edition = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert main_edition is not None
        assert main_edition.kind == EditionKind.main
        assert main_edition.tracking_mode == TrackingMode.git_ref
        assert main_edition.tracking_params == {"git_ref": "main"}
        assert main_edition.current_build_id is not None

        build = await build_store.get_by_id(main_edition.current_build_id)
        assert build is not None
        assert build.status == BuildStatus.completed
        assert build.content_hash.startswith("sha256:")
        assert build.content_hash != "sha256:" + "0" * 64
        assert build.object_count == 2
        assert build.git_ref == "main"
        assert build.uploader == "keeper-sync"

        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.build,
            ltd_id=42,
        )
        assert state is not None
        assert state.docverse_id == build.id
        assert state.content_hash == build.content_hash

    # Build content landed in the destination object store.
    assert any(k.endswith("/index.html") for k in object_store.objects)
    assert any(k.endswith("/app.js") for k in object_store.objects)


@pytest.mark.asyncio
async def test_sync_short_circuits_when_state_matches_ltd(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A second sync run with unchanged LTD state must not copy content."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    _seed_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
    }

    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    keys_after_first = set(object_store.objects.keys())
    assert keys_after_first

    # Second run with identical LTD state.
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    edition_outcomes = result.edition_outcomes
    assert any(
        o.build_outcome and o.build_outcome.short_circuited
        for o in edition_outcomes
    )

    # Object store unchanged: no re-copy.
    assert set(object_store.objects.keys()) == keys_after_first


@pytest.mark.asyncio
async def test_sync_does_not_short_circuit_when_state_missing_docverse_id(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """State with matching date but null docverse_id must not short-circuit.

    Guards the case where a placeholder state upsert succeeded but build
    creation crashed before the docverse build id was recorded.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    _seed_ltd(mock_discovery)

    edition_payload = _load("edition_main_git_refs.json")
    ltd_date_rebuilt = datetime.fromisoformat(
        str(edition_payload["date_rebuilt"])
    )

    state_store = KeeperSyncStateStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.build,
            ltd_id=42,
            ltd_slug="42",
            date_last_synced=_now(),
            date_rebuilt_seen=ltd_date_rebuilt,
        )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    main_outcome = result.edition_outcomes[0]
    assert main_outcome.build_outcome is not None
    assert main_outcome.build_outcome.short_circuited is False
    assert main_outcome.build_outcome.docverse_build_id is not None

    async with db_session.begin():
        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.build,
            ltd_id=42,
        )
        assert state is not None
        assert (
            state.docverse_id == main_outcome.build_outcome.docverse_build_id
        )

    assert any(k.endswith("/index.html") for k in object_store.objects)


@pytest.mark.asyncio
async def test_resync_detects_new_build_via_date_rebuilt(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Newer LTD ``date_rebuilt`` triggers a fresh build sync."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    _seed_ltd(mock_discovery)
    object_store = MockObjectStore()
    source_objects_v1 = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects_v1
    )
    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # LTD now reports a newer date_rebuilt and a different build URL.
    edition_v2 = _load("edition_main_git_refs.json")
    edition_v2["date_rebuilt"] = "2026-05-04T12:00:00.000000+00:00"
    edition_v2["build_url"] = f"{LTD_BASE}/builds/43"
    build_v2 = _load("build.json")
    build_v2["self_url"] = f"{LTD_BASE}/builds/43"
    build_v2["bucket_root_dir"] = "pipelines/builds/43"

    mock_discovery.reset()
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200, json={"editions": [f"{LTD_BASE}/editions/1"]}
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(200, json=edition_v2)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=build_v2)
    )

    source_objects_v2 = {
        "pipelines/builds/43/index.html": b"<html>v2</html>",
        "pipelines/builds/43/new.txt": b"new",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects_v2
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    main_outcome = result.edition_outcomes[0]
    assert main_outcome.build_outcome is not None
    assert main_outcome.build_outcome.short_circuited is False
    assert main_outcome.build_outcome.object_count == 2

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    build_store = BuildStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition is not None
        assert edition.current_build_id is not None
        assert (
            edition.current_build_id
            == main_outcome.build_outcome.docverse_build_id
        )
        build = await build_store.get_by_id(edition.current_build_id)
        assert build is not None
        assert build.object_count == 2


@pytest.mark.asyncio
async def test_branch_edition_creates_new_draft_edition(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A non-main LTD edition becomes a Docverse ``draft`` edition."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)

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
            200, json={"editions": [f"{LTD_BASE}/editions/2"]}
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(200, json=branch_edition)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=branch_build)
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>branch</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    assert len(result.edition_outcomes) == 1
    outcome = result.edition_outcomes[0]
    assert outcome.docverse_slug == "u-jsick-feature"

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        # New draft edition exists.
        draft = await edition_store.get_by_slug(
            project_id=project.id, slug="u-jsick-feature"
        )
        assert draft is not None
        assert draft.kind == EditionKind.draft
        assert draft.tracking_mode == TrackingMode.git_ref
        assert draft.tracking_params == {"git_ref": "u/jsick/feature"}
        # The auto-created __main edition is unchanged (still has no build).
        main = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert main is not None
        assert main.current_build_id is None


@pytest.mark.asyncio
async def test_sync_build_refuses_half_uploaded_build(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """LTD build with ``uploaded=False`` must raise rather than sync."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    half_uploaded_build = _load("build.json")
    half_uploaded_build["uploaded"] = False
    _seed_ltd(mock_discovery, build_payload=half_uploaded_build)

    object_store = MockObjectStore()
    service = _build_service(db_session, http_client, object_store, {})

    with pytest.raises(RuntimeError, match="uploaded=False"):
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # Nothing was copied to the destination.
    assert not object_store.objects


@pytest.mark.asyncio
async def test_unsupported_ltd_mode_raises_not_implemented(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """LTD modes other than ``git_refs`` cleanly raise NotImplementedError."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    edition = _load("edition_main_git_refs.json")
    edition["mode"] = "lsst_doc"
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200, json={"editions": [f"{LTD_BASE}/editions/1"]}
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(200, json=edition)
    )

    service = _build_service(db_session, http_client, MockObjectStore(), {})
    with pytest.raises(NotImplementedError, match="#289"):
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")


@pytest.mark.asyncio
async def test_sync_build_reclaims_orphaned_pending_placeholder(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Stale orphans get reclaimed before the next placeholder is created.

    Mimics a crashed prior run: a ``pending`` placeholder for the same
    ``(project_id, git_ref)`` exists, backdated past the reclaim
    window. The next sync must transition it to ``failed`` and still
    complete its own build normally.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    _seed_ltd(mock_discovery)
    object_store = MockObjectStore()
    source_objects_v1 = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects_v1
    )
    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    build_store = BuildStore(
        session=db_session, logger=structlog.get_logger("test")
    )

    # Mimic a crashed prior run: a placeholder build was created for
    # the same project/git_ref under the keeper-sync uploader, then
    # the worker died before the finalize transaction. We backdate
    # ``date_created`` so the default reclaim window catches it.
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        orphan = await build_store.create(
            project_id=project.id,
            project_slug=project.slug,
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:" + "0" * 64,
            ),
            uploader="keeper-sync",
        )
        await db_session.execute(
            update(SqlBuild)
            .where(SqlBuild.id == orphan.id)
            .values(date_created=_now() - timedelta(hours=2))
        )
    orphan_id = orphan.id

    # LTD now reports a fresh build → sync re-runs the active path,
    # which must reclaim the orphan before creating its placeholder.
    edition_v2 = _load("edition_main_git_refs.json")
    edition_v2["date_rebuilt"] = "2026-05-04T12:00:00.000000+00:00"
    edition_v2["build_url"] = f"{LTD_BASE}/builds/43"
    build_v2 = _load("build.json")
    build_v2["self_url"] = f"{LTD_BASE}/builds/43"
    build_v2["bucket_root_dir"] = "pipelines/builds/43"

    mock_discovery.reset()
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200, json={"editions": [f"{LTD_BASE}/editions/1"]}
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(200, json=edition_v2)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=build_v2)
    )

    source_objects_v2 = {
        "pipelines/builds/43/index.html": b"<html>v2</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects_v2
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    main_outcome = result.edition_outcomes[0]
    assert main_outcome.build_outcome is not None
    assert main_outcome.build_outcome.short_circuited is False
    new_build_id = main_outcome.build_outcome.docverse_build_id
    assert new_build_id is not None
    assert new_build_id != orphan_id

    async with db_session.begin():
        orphan_after = await build_store.get_by_id(orphan_id)
        assert orphan_after is not None
        assert orphan_after.status == BuildStatus.failed
        assert orphan_after.date_completed is not None

        new_build = await build_store.get_by_id(new_build_id)
        assert new_build is not None
        assert new_build.status == BuildStatus.completed


@pytest.mark.asyncio
async def test_sync_build_does_not_reclaim_recent_pending_placeholders(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Recent placeholders stay ``pending`` — they may belong to a peer.

    A keeper-sync placeholder younger than the reclaim window must be
    left alone; otherwise two concurrent syncs of the same edition
    would cannibalize each other.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    _seed_ltd(mock_discovery)
    object_store = MockObjectStore()
    source_objects_v1 = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects_v1
    )
    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    build_store = BuildStore(
        session=db_session, logger=structlog.get_logger("test")
    )

    # A keeper-sync placeholder created moments ago — younger than the
    # default one-hour reclaim window.
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        recent_orphan = await build_store.create(
            project_id=project.id,
            project_slug=project.slug,
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:" + "0" * 64,
            ),
            uploader="keeper-sync",
        )

    edition_v2 = _load("edition_main_git_refs.json")
    edition_v2["date_rebuilt"] = "2026-05-04T12:00:00.000000+00:00"
    edition_v2["build_url"] = f"{LTD_BASE}/builds/43"
    build_v2 = _load("build.json")
    build_v2["self_url"] = f"{LTD_BASE}/builds/43"
    build_v2["bucket_root_dir"] = "pipelines/builds/43"

    mock_discovery.reset()
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200, json={"editions": [f"{LTD_BASE}/editions/1"]}
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(200, json=edition_v2)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=build_v2)
    )

    source_objects_v2 = {
        "pipelines/builds/43/index.html": b"<html>v2</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects_v2
    )
    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    async with db_session.begin():
        recent = await build_store.get_by_id(recent_orphan.id)
        assert recent is not None
        assert recent.status == BuildStatus.pending


@pytest.mark.asyncio
async def test_now_helper_returns_aware_datetime() -> None:
    """Sanity check: state-row timestamps must be timezone-aware."""
    value = _now()
    assert isinstance(value, datetime)
    assert value.tzinfo == UTC
