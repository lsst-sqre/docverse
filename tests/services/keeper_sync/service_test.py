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
from safir.github import GitHubAppClientFactory
from sqlalchemy import update
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
from docverse.dbschema.build import SqlBuild
from docverse.domain.lifecycle import (
    BuildHistoryOrphanRule,
    DraftInactivityRule,
    LifecycleRuleSet,
    RefDeletedRule,
)
from docverse.services.keeper_sync.copier import BuildContentCopier
from docverse.services.keeper_sync.service import (
    KeeperSyncContext,
    KeeperSyncService,
    _now,
)
from docverse.services.keeper_sync_tombstone import KeeperSyncTombstoneService
from docverse.services.project import ProjectService
from docverse.services.project_github_binding import (
    ProjectGitHubBindingResolver,
)
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.github import (
    GITHUB_API_BASE_URL,
    GitHubAppClient,
    GitHubRefSetFetcher,
)
from docverse.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse.storage.ltd import LtdClient, LtdSourceProtocol
from docverse.storage.objectstore import MockObjectStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from tests.support.github_mock import DEFAULT_APP_NAME, GitHubMock

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


async def _seed_org(
    session: AsyncSession,
    *,
    slug: str = "ks-svc",
    lifecycle_rules: LifecycleRuleSet | None = None,
) -> int:
    logger = structlog.get_logger("test")
    store = OrganizationStore(session=session, logger=logger)
    org = await store.create(
        OrganizationCreate(
            slug=slug,
            title="ks-svc",
            base_domain=f"{slug}.example.com",
            lifecycle_rules=lifecycle_rules,
        )
    )
    return org.id


def _build_service(
    session: AsyncSession,
    http_client: httpx.AsyncClient,
    object_store: MockObjectStore,
    source_objects: dict[str, bytes],
    *,
    binding_resolver: ProjectGitHubBindingResolver | None = None,
    ref_set_fetcher: GitHubRefSetFetcher | None = None,
    tombstone_service: KeeperSyncTombstoneService | None = None,
) -> KeeperSyncService:
    """Construct a real ``KeeperSyncService`` against the test DB.

    Optional ``binding_resolver`` / ``ref_set_fetcher`` /
    ``tombstone_service`` wire the proactive-lifecycle path; tests
    that don't care about that path leave them unset and get the
    pre-PRD-#332 behavior (proactive pass is a no-op).
    """
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

    async def manifest_callable(source_prefix: str) -> str:
        return await copier.compute_manifest_hash(source_prefix=source_prefix)

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
        manifest_callable=manifest_callable,
        logger=logger,
        tombstone_service=tombstone_service,
        binding_resolver=binding_resolver,
        ref_set_fetcher=ref_set_fetcher,
    )


def _make_proactive_deps(
    *,
    session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_github: GitHubMock,
) -> tuple[
    ProjectGitHubBindingResolver,
    GitHubRefSetFetcher,
    KeeperSyncTombstoneService,
]:
    """Build the three proactive-lifecycle deps for a sync_project test.

    Mirrors the production factory wiring at the unit-test layer: the
    resolver, ref fetcher, and tombstone service are the same concrete
    classes the production ``Factory.create_keeper_sync_service`` would
    construct. Tests pass these straight into ``_build_service`` to
    exercise the proactive path.
    """
    logger = structlog.get_logger("test")
    project_store = ProjectStore(session=session, logger=logger)
    state_store = KeeperSyncStateStore(session=session, logger=logger)
    safir_factory = GitHubAppClientFactory(
        id=mock_github.app_id,
        key=mock_github.private_key_pem,
        name=DEFAULT_APP_NAME,
        http_client=http_client,
    )
    app_client = GitHubAppClient(
        factory=safir_factory,
        http_client=http_client,
        logger=logger,
    )
    resolver = ProjectGitHubBindingResolver(
        session=session,
        project_store=project_store,
        app_client=app_client,
        logger=logger,
    )
    fetcher = GitHubRefSetFetcher(http_client=http_client)
    tombstone_service = KeeperSyncTombstoneService(
        session=session, state_store=state_store, logger=logger
    )
    return resolver, fetcher, tombstone_service


def _ref_entry(ref: str) -> dict[str, object]:
    return {
        "ref": ref,
        "node_id": f"node-{ref}",
        "url": f"https://api.github.com/{ref}",
        "object": {"sha": "deadbeef", "type": "commit"},
    }


def _seed_github_refs(
    router: respx.Router,
    *,
    owner: str,
    repo: str,
    branches: list[str] | None = None,
    tags: list[str] | None = None,
) -> tuple[respx.Route, respx.Route]:
    """Seed the matching-refs endpoints for one repo; return the routes.

    Returning the routes lets the caller pin call counts on the
    heads/tags endpoints (the "fetched once per sync_project" assertion).
    """
    branches = branches if branches is not None else []
    tags = tags if tags is not None else []
    heads_route = router.get(
        f"{GITHUB_API_BASE_URL}/repos/{owner}/{repo}/git/matching-refs/heads"
    ).mock(
        return_value=httpx.Response(
            200, json=[_ref_entry(f"refs/heads/{n}") for n in branches]
        )
    )
    tags_route = router.get(
        f"{GITHUB_API_BASE_URL}/repos/{owner}/{repo}/git/matching-refs/tags"
    ).mock(
        return_value=httpx.Response(
            200, json=[_ref_entry(f"refs/tags/{n}") for n in tags]
        )
    )
    return heads_route, tags_route


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
async def test_keeper_sync_adopts_native_git_ref_edition(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """keeper-sync adopts a differently-slugged native edition on one ref.

    PRD #409: native auto-creation slugifies ``tickets/DM-54686`` to
    ``tickets-DM-54686`` while keeper-sync imports LTD's own ``DM-54686``
    slug. Both track the same ``git_ref``. After a ``get_by_slug`` miss,
    keeper-sync must consult the shared git_ref lookup, adopt the
    existing native edition (refresh its tracking, keep its slug), and
    create no second row; the ``keeper_sync_state`` for the imported
    edition points at the adopted edition's id.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    logger = structlog.get_logger("test")
    project_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)

    # A native auto-created edition already tracks ``tickets/DM-54686``
    # under the slugified slug, before keeper-sync ever runs.
    async with db_session.begin():
        project = await project_store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="pipelines",
                title="LSST Science Pipelines",
                source_url="https://example.com/lsst/pipelines",
            ),
        )
        native_edition = await edition_store.create(
            project_id=project.id,
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
    branch_edition = _load("edition_branch_git_refs.json")
    branch_edition["slug"] = "DM-54686"
    branch_edition["title"] = "DM-54686"
    branch_edition["tracked_refs"] = ["tickets/DM-54686"]
    branch_edition["build_url"] = f"{LTD_BASE}/builds/43"
    branch_build = _load("build.json")
    branch_build["self_url"] = f"{LTD_BASE}/builds/43"
    branch_build["bucket_root_dir"] = "pipelines/builds/43"

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

    # The adopted edition's id is returned even though the keeper slug
    # differs from the native one.
    assert len(result.edition_outcomes) == 1
    assert result.edition_outcomes[0].docverse_edition_id == native_edition_id

    state_store = KeeperSyncStateStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        # No second edition under the keeper-derived slug was created.
        keeper_slugged = await edition_store.get_by_slug(
            project_id=project.id, slug="DM-54686"
        )
        assert keeper_slugged is None

        # The native edition survives, keeps its slug, and its tracking
        # was refreshed to the imported git_ref.
        adopted = await edition_store.get_by_slug(
            project_id=project.id, slug="tickets-DM-54686"
        )
        assert adopted is not None
        assert adopted.id == native_edition_id
        assert adopted.tracking_mode == TrackingMode.git_ref
        assert adopted.tracking_params == {"git_ref": "tickets/DM-54686"}

        # Exactly one edition tracks the ref — no duplicate row.
        all_editions = await edition_store.list_all_by_project(project.id)
        on_ref = [
            e
            for e in all_editions
            if (e.tracking_params or {}).get("git_ref") == "tickets/DM-54686"
        ]
        assert len(on_ref) == 1

        # keeper_sync_state for the imported edition (ltd_id=2) points at
        # the adopted edition's id.
        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
        )
        assert state is not None
        assert state.docverse_id == native_edition_id


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
@pytest.mark.parametrize(
    ("ltd_mode", "expected_tracking_mode"),
    [
        ("lsst_doc", TrackingMode.lsst_doc),
        ("eups_major_release", TrackingMode.eups_major_release),
        ("eups_weekly_release", TrackingMode.eups_weekly_release),
        ("eups_daily_release", TrackingMode.eups_daily_release),
    ],
)
async def test_sync_edition_round_trips_version_modes(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    ltd_mode: str,
    expected_tracking_mode: TrackingMode,
) -> None:
    """Same-named LTD/Docverse version modes round-trip with no params.

    ``lsst_doc`` and the three ``eups_*_release`` modes use Docverse
    version parsers (no ``tracking_params`` needed), so the imported
    edition row carries an empty params dict and the recorded LTD mode
    survives in the state row's ``annotations``.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    edition_payload = _load("edition_main_git_refs.json")
    edition_payload["mode"] = ltd_mode
    _seed_ltd(mock_discovery, edition_main=edition_payload)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    project_store = ProjectStore(
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
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition is not None
        assert edition.tracking_mode == expected_tracking_mode
        assert edition.tracking_params == {}

        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
        )
        assert state is not None
        assert state.annotations is not None
        assert state.annotations["ltd_mode"] == ltd_mode


@pytest.mark.asyncio
async def test_sync_edition_imports_lsst_doc_branch_edition(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A non-main ``lsst_doc`` edition imports as a Docverse draft."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    branch_edition = _load("edition_branch_git_refs.json")
    branch_edition["mode"] = "lsst_doc"
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
        draft = await edition_store.get_by_slug(
            project_id=project.id, slug="u-jsick-feature"
        )
        assert draft is not None
        assert draft.kind == EditionKind.draft
        assert draft.tracking_mode == TrackingMode.lsst_doc
        assert draft.tracking_params == {}

    outcome = result.edition_outcomes[0]
    assert outcome.docverse_slug == "u-jsick-feature"


@pytest.mark.asyncio
async def test_sync_edition_imports_manual_as_pinned_git_ref(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """``manual`` LTD editions become ``git_ref`` pinned to the build's ref.

    Docverse has no ``manual`` tracking mode (PRD #275 "Out of scope"),
    so the importer collapses ``manual`` onto a pinned ``git_ref`` while
    preserving the original LTD ``manual`` label in
    ``keeper_sync_state.annotations`` for later reversibility.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    edition_payload = _load("edition_main_git_refs.json")
    edition_payload["mode"] = "manual"
    # Keep ``tracked_refs`` set so the existing ``_create_synced_build``
    # path remains happy; the assertion on tracking_params below proves
    # the mapper still pins to the BUILD's git_refs[0] rather than the
    # edition's tracked_refs[0]. Hardening
    # ``_create_synced_build`` to derive the Docverse build's git_ref
    # from ``LtdBuild.git_refs[0]`` (so manual editions with
    # ``tracked_refs is None`` round-trip cleanly) is tracked as
    # follow-up — out of scope for this mapper-only slice.
    edition_payload["tracked_refs"] = ["main"]
    build_payload = _load("build.json")
    build_payload["git_refs"] = ["v22_0_0", "main"]
    _seed_ltd(
        mock_discovery,
        edition_main=edition_payload,
        build_payload=build_payload,
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>v22</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    project_store = ProjectStore(
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
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition is not None
        assert edition.tracking_mode == TrackingMode.git_ref
        # The build's first git_ref is pinned, NOT the edition's
        # tracked_refs (which manual editions need not maintain).
        assert edition.tracking_params == {"git_ref": "v22_0_0"}

        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
        )
        assert state is not None
        assert state.annotations is not None
        # Original LTD mode preserved for reversibility.
        assert state.annotations["ltd_mode"] == "manual"


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
async def test_dual_upload_convergence_links_existing_build_and_skips_copy(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Skip the copy and link state when content matches an existing build.

    Models the dual-upload scenario from PRD #275 user story 12: a
    project has cut over to direct Docverse uploads, so its current
    Docverse build holds the canonical content; LTD CI is still pushing
    the same content to LTD as well. Re-copying that content into a new
    Docverse build row would have the two upload paths fight each other.
    """
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
    first = await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    existing_build_id = first.edition_outcomes[
        0
    ].build_outcome.docverse_build_id  # type: ignore[union-attr]
    assert existing_build_id is not None
    keys_after_first = set(object_store.objects.keys())
    assert keys_after_first

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    project_store = ProjectStore(
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
        edition_before = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition_before is not None
        edition_date_updated_before = edition_before.date_updated

    # Now LTD reports a *new* build (id 43) at a new bucket prefix, but
    # the source content under that prefix is byte-identical to what's
    # already in Docverse. The keeper-sync state has no row for ltd_id=43,
    # so the existing date-based short-circuit cannot fire.
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

    # Same files under a different LTD bucket prefix → same manifest
    # hash (the manifest is over relative key + content).
    source_objects_v2 = {
        "pipelines/builds/43/index.html": b"<html>v1</html>",
        "pipelines/builds/43/assets/app.js": b"console.log(1)",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects_v2
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    main_outcome = result.edition_outcomes[0]
    assert main_outcome.build_outcome is not None
    assert main_outcome.build_outcome.short_circuited is True
    assert main_outcome.build_outcome.docverse_build_id == existing_build_id
    assert main_outcome.build_outcome.docverse_build_public_id is not None
    # Object store unchanged: convergence skipped the upload entirely.
    assert set(object_store.objects.keys()) == keys_after_first

    async with db_session.begin():
        # No new build row was created — only the original survives.
        builds = await build_store.list_by_project(project.id, limit=10)
        assert len(builds.entries) == 1
        assert builds.entries[0].id == existing_build_id

        # State for the new ltd_id points at the existing Docverse build.
        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.build,
            ltd_id=43,
        )
        assert state is not None
        assert state.docverse_id == existing_build_id
        assert state.content_hash is not None
        assert state.content_hash.startswith("sha256:")

        # Edition still points at the existing build and was not touched
        # (date_updated unchanged).
        edition_after = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition_after is not None
        assert edition_after.current_build_id == existing_build_id
        assert edition_after.date_updated == edition_date_updated_before


@pytest.mark.asyncio
async def test_dual_upload_convergence_repoints_edition_when_pointer_differs(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Convergence updates the edition pointer when it points elsewhere.

    The edition currently points at a different build (e.g. a stale
    keeper-sync row from an earlier resync). Discovering the matching-
    hash build means the sync must atomically re-point the edition to
    that build inside the same transaction that links the state row.
    """
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
    first = await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    matching_build_id = first.edition_outcomes[
        0
    ].build_outcome.docverse_build_id  # type: ignore[union-attr]
    assert matching_build_id is not None

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    build_store = BuildStore(
        session=db_session, logger=structlog.get_logger("test")
    )

    # Create a second Docverse build with a different content hash, then
    # point the edition at it. Convergence should detect the matching-
    # hash build above and re-point back.
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        other_build = await build_store.create(
            project_id=project.id,
            project_slug=project.slug,
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:" + "1" * 64,
            ),
            uploader="direct-upload",
        )
        # Walk it through to ``completed`` so set_current_build's stale
        # guard sees a real ``date_created`` on the row.
        await build_store.transition_status(
            build_id=other_build.id, new_status=BuildStatus.processing
        )
        await build_store.transition_status(
            build_id=other_build.id, new_status=BuildStatus.completed
        )
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition is not None
        await edition_store.set_current_build(
            edition_id=edition.id,
            build_id=other_build.id,
            skip_date_guard=True,
        )

    # LTD now reports a new build (id 43) whose content matches build_42
    # (and thus the original Docverse build).
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

    keys_before = set(object_store.objects.keys())
    source_objects_v2 = {
        "pipelines/builds/43/index.html": b"<html>v1</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects_v2
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    main_outcome = result.edition_outcomes[0]
    assert main_outcome.build_outcome is not None
    assert main_outcome.build_outcome.docverse_build_id == matching_build_id
    assert main_outcome.build_outcome.docverse_build_public_id is not None
    # Skipped the copy.
    assert set(object_store.objects.keys()) == keys_before

    async with db_session.begin():
        edition_after = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition_after is not None
        assert edition_after.current_build_id == matching_build_id


@pytest.mark.asyncio
async def test_now_helper_returns_aware_datetime() -> None:
    """Sanity check: state-row timestamps must be timezone-aware."""
    value = _now()
    assert isinstance(value, datetime)
    assert value.tzinfo == UTC


@pytest.mark.asyncio
async def test_sync_project_invokes_on_edition_synced_per_outcome(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """The callback fires once per edition with the same outcome objects.

    Locks the contract worker code relies on: ``on_edition_synced``
    sees every successful ``sync_edition`` outcome before
    ``sync_project`` returns, and each outcome's
    ``docverse_project_id`` / ``docverse_project_slug`` is populated
    so the publish-enqueue closure has the project context the helper
    needs.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    main_edition_payload = _load("edition_main_git_refs.json")
    branch_edition_payload = _load("edition_branch_git_refs.json")
    branch_edition_payload["build_url"] = f"{LTD_BASE}/builds/43"
    branch_build_payload = _load("build.json")
    branch_build_payload["self_url"] = f"{LTD_BASE}/builds/43"
    branch_build_payload["bucket_root_dir"] = "pipelines/builds/43"

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
        return_value=httpx.Response(200, json=main_edition_payload)
    )
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(200, json=branch_edition_payload)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/42").mock(
        return_value=httpx.Response(200, json=_load("build.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=branch_build_payload)
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>branch</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    seen: list[tuple[str, int, str]] = []

    async def on_edition_synced(outcome: object) -> None:
        # Cast for type checker convenience; the runtime object is an
        # EditionSyncOutcome.
        seen.append(
            (
                outcome.docverse_slug,  # type: ignore[attr-defined]
                outcome.docverse_project_id,  # type: ignore[attr-defined]
                outcome.docverse_project_slug,  # type: ignore[attr-defined]
            )
        )

    result = await service.sync_project(
        org_id=org_id,
        ltd_slug="pipelines",
        on_edition_synced=on_edition_synced,
    )

    assert len(seen) == 2
    callback_slugs = [s for s, _, _ in seen]
    assert "__main" in callback_slugs
    assert "u-jsick-feature" in callback_slugs
    for _, project_id, project_slug in seen:
        assert project_id == result.docverse_project_id
        assert project_slug == result.docverse_project_slug


@pytest.mark.asyncio
async def test_sync_project_continues_when_callback_raises(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A raising callback on edition N must not stop edition N+1's sync.

    The worker's tail-end self-heal pass picks up any edition the
    callback failed on, but only if ``sync_project`` keeps walking the
    edition list past the failure. Lock that contract here.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    main_edition_payload = _load("edition_main_git_refs.json")
    branch_edition_payload = _load("edition_branch_git_refs.json")
    branch_edition_payload["build_url"] = f"{LTD_BASE}/builds/43"
    branch_build_payload = _load("build.json")
    branch_build_payload["self_url"] = f"{LTD_BASE}/builds/43"
    branch_build_payload["bucket_root_dir"] = "pipelines/builds/43"

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
        return_value=httpx.Response(200, json=main_edition_payload)
    )
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(200, json=branch_edition_payload)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/42").mock(
        return_value=httpx.Response(200, json=_load("build.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=branch_build_payload)
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>branch</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    invocations: list[str] = []

    async def on_edition_synced(outcome: object) -> None:
        slug: str = outcome.docverse_slug  # type: ignore[attr-defined]
        invocations.append(slug)
        if slug == "__main":
            msg = "boom"
            raise RuntimeError(msg)

    result = await service.sync_project(
        org_id=org_id,
        ltd_slug="pipelines",
        on_edition_synced=on_edition_synced,
    )

    # Both editions were walked and both produced outcomes; the
    # callback ran for both even though the first raised.
    assert invocations == ["__main", "u-jsick-feature"]
    assert len(result.edition_outcomes) == 2
    outcome_slugs = [o.docverse_slug for o in result.edition_outcomes]
    assert "__main" in outcome_slugs
    assert "u-jsick-feature" in outcome_slugs


@pytest.mark.asyncio
async def test_sync_edition_short_circuits_when_tombstoned(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A tombstoned edition state row short-circuits ``sync_edition``.

    The build copy must not run, the build store must not be touched,
    and ``date_last_synced`` on the tombstoned state row must remain
    untouched (proves ``_ensure_edition`` / state ``upsert`` were
    skipped).
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-edition")

    _seed_ltd(mock_discovery)
    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    # First sync imports the edition + build normally.
    first = await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    assert first.edition_outcomes
    keys_after_first = set(object_store.objects.keys())
    assert keys_after_first

    state_store = KeeperSyncStateStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    tombstone_service = KeeperSyncTombstoneService(
        session=db_session,
        state_store=state_store,
        logger=structlog.get_logger("test"),
    )

    # Tombstone the edition state row + snapshot ``date_last_synced``
    # so the post-sync assertion can prove the upsert was skipped.
    async with db_session.begin():
        await tombstone_service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            reason=TombstoneReason.lifecycle_delete,
        )
        state_before = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            include_tombstoned=True,
        )
    assert state_before is not None
    assert state_before.date_tombstoned is not None
    date_last_synced_before = state_before.date_last_synced

    # LTD pretends a fresh build arrived — without the short-circuit
    # the sync would copy bucket content into Docverse.
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
    builds_43_route = mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=build_v2)
    )

    source_objects_v2 = {
        "pipelines/builds/43/index.html": b"<html>v2</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects_v2
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # The edition outcome is present but carries no build_outcome — the
    # short-circuit fired before ``sync_build`` had a chance to run.
    assert len(result.edition_outcomes) == 1
    outcome = result.edition_outcomes[0]
    assert outcome.docverse_slug == "__main"
    assert outcome.build_outcome is None

    # No new bytes landed in the destination object store, and the
    # build endpoint was never fetched — short-circuit fired before
    # any sync_build work could begin.
    assert set(object_store.objects.keys()) == keys_after_first
    assert builds_43_route.call_count == 0

    # The tombstoned state row is unchanged — no upsert ran inside
    # the short-circuited sync_edition.
    async with db_session.begin():
        state_after = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            include_tombstoned=True,
        )
    assert state_after is not None
    assert state_after.date_tombstoned is not None
    assert state_after.date_last_synced == date_last_synced_before


@pytest.mark.asyncio
async def test_sync_project_short_circuits_when_tombstoned(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A tombstoned project state row short-circuits ``sync_project``.

    The LTD product fetch must not run and ``edition_outcomes`` must
    be empty.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-project")

    _seed_ltd(mock_discovery)
    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    # First sync imports the project + writes the project state row.
    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    state_store = KeeperSyncStateStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    tombstone_service = KeeperSyncTombstoneService(
        session=db_session,
        state_store=state_store,
        logger=structlog.get_logger("test"),
    )
    async with db_session.begin():
        await tombstone_service.record(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
            reason=TombstoneReason.manual_delete,
        )

    # Clear LTD call stats — the short-circuit must issue zero LTD
    # calls, so any post-tombstone delta would indicate the sync
    # still walked LTD. ``reset()`` keeps the routes registered so a
    # missed short-circuit still returns the stubbed responses (no
    # passthrough surprises) while the call counter remains pinnable.
    mock_discovery.reset()

    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert result.edition_outcomes == []
    assert mock_discovery.calls.call_count == 0


@pytest.mark.asyncio
async def test_sync_edition_does_not_crash_on_soft_deleted_tombstoned_row(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Regression: tombstone short-circuit defuses the slug-clash crash.

    Without the short-circuit, a soft-deleted Docverse edition row
    whose LTD source still exists makes ``_ensure_edition`` /
    ``EditionStore.create_internal`` hit a ``uq_editions_project_lower_slug``
    conflict against the soft-deleted row, ``ON CONFLICT DO NOTHING``
    short-circuits the insert, and the follow-up ``get_by_slug``
    (which filters ``date_deleted IS NULL``) returns ``None`` —
    raising ``"create_internal lost ON CONFLICT race"``. The tombstone
    must defuse that crash by short-circuiting ``sync_edition`` before
    ``_ensure_edition`` runs.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-soft-deleted")

    _seed_ltd(mock_discovery)
    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>v1</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    state_store = KeeperSyncStateStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    tombstone_service = KeeperSyncTombstoneService(
        session=db_session,
        state_store=state_store,
        logger=structlog.get_logger("test"),
    )

    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        soft_deleted = await edition_store.soft_delete(
            org_id=org_id,
            project_id=project.id,
            slug="__main",
            reason=TombstoneReason.manual_delete,
        )
        assert soft_deleted is True
        await tombstone_service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            reason=TombstoneReason.manual_delete,
        )

    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    # Without the short-circuit this raises "lost ON CONFLICT race".
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert len(result.edition_outcomes) == 1
    outcome = result.edition_outcomes[0]
    assert outcome.docverse_slug == "__main"
    assert outcome.build_outcome is None


# ---------------------------------------------------------------------------
# Proactive lifecycle evaluation (PRD #332 / DM-54914) — sync_project
# pre-filters LTD editions that ``draft_inactivity`` or ``ref_deleted``
# would immediately tombstone, so the migration does not spend bandwidth
# copying build assets the lifecycle pass would delete seconds later.
# ---------------------------------------------------------------------------


def _seed_two_editions_main_and_draft(
    mock_discovery: respx.Router,
    *,
    draft_payload: dict | None = None,  # type: ignore[type-arg]
    draft_build_payload: dict | None = None,  # type: ignore[type-arg]
) -> None:
    """Stub LTD's view of a project with main + one draft edition.

    Two edition resources at ``/editions/1`` (main, build 42) and
    ``/editions/2`` (the supplied draft, build 43). The draft's
    ``tracked_refs`` and ``date_rebuilt`` are driven by the caller via
    ``draft_payload`` so individual tests can express their scenario
    (ref present / ref deleted / stale / fresh) directly.
    """
    if draft_payload is None:
        draft_payload = _load("edition_branch_git_refs.json")
    if draft_build_payload is None:
        draft_build_payload = _load("build.json")
        draft_build_payload["self_url"] = f"{LTD_BASE}/builds/43"
        draft_build_payload["bucket_root_dir"] = "pipelines/builds/43"
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
    mock_discovery.get(f"{LTD_BASE}/builds/42").mock(
        return_value=httpx.Response(200, json=_load("build.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(200, json=draft_payload)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=draft_build_payload)
    )


# The ``pipelines`` LTD product's ``doc_repo`` parses to
# (lsst, pipelines_lsst_io); every proactive test seeds GitHub refs for
# that coordinate. Keep the constant here so the wiring is obvious at
# the test call sites.
_LSST_OWNER = "lsst"
_LSST_REPO = "pipelines_lsst_io"


@pytest.mark.asyncio
async def test_proactive_ref_deleted_tombstones_and_skips_sync_edition(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    mock_github: GitHubMock,
) -> None:
    """A draft tracking a missing GitHub branch is tombstoned pre-import.

    Seeds an org with ``RefDeletedRule`` configured, an LTD product
    whose ``doc_repo`` is a public GitHub repo, and a draft edition
    tracking a branch that no longer exists on GitHub. The proactive
    evaluator must tombstone the draft ``lifecycle_preemptive``, skip
    its ``sync_edition`` call entirely (no Docverse edition row, no
    LTD build endpoint hit, no bytes in the destination store), and
    sync the main edition normally.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug="ks-proactive-ref",
            lifecycle_rules=LifecycleRuleSet(root=[RefDeletedRule()]),
        )

    draft_payload = _load("edition_branch_git_refs.json")
    # Track a branch that the GitHub mock will NOT list as live.
    draft_payload["tracked_refs"] = ["tickets/DM-deleted"]
    _seed_two_editions_main_and_draft(
        mock_discovery, draft_payload=draft_payload
    )
    _seed_github_refs(
        mock_github.router,
        owner=_LSST_OWNER,
        repo=_LSST_REPO,
        branches=["main"],
        tags=[],
    )
    # Track the draft build endpoint so we can prove it was not fetched.
    draft_build_route = mock_discovery.get(f"{LTD_BASE}/builds/43")

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>draft</html>",
    }
    resolver, fetcher, tombstone_service = _make_proactive_deps(
        session=db_session,
        http_client=http_client,
        mock_github=mock_github,
    )
    service = _build_service(
        db_session,
        http_client,
        object_store,
        source_objects,
        binding_resolver=resolver,
        ref_set_fetcher=fetcher,
        tombstone_service=tombstone_service,
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # Only the main edition produced an outcome — the draft was
    # tombstoned pre-import and skipped.
    assert len(result.edition_outcomes) == 1
    assert result.edition_outcomes[0].docverse_slug == "__main"

    # The draft's LTD build endpoint was never fetched.
    assert draft_build_route.call_count == 0

    # No draft edition row was created in Docverse.
    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    edition_store = EditionStore(
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
        draft = await edition_store.get_by_slug(
            project_id=project.id, slug="u-jsick-feature"
        )
        assert draft is None

        # ...but the tombstone state row IS present.
        tombstone = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            include_tombstoned=True,
        )
        assert tombstone is not None
        assert tombstone.date_tombstoned is not None
        assert tombstone.tombstone_reason == (
            TombstoneReason.lifecycle_preemptive.value
        )
        # No Docverse row was created — the tombstone is a pure veto.
        assert tombstone.docverse_id is None

    # No draft bytes landed in the destination object store.
    assert not any(
        k.startswith("pipelines/builds/43") for k in object_store.objects
    )


@pytest.mark.asyncio
async def test_proactive_draft_inactivity_tombstones_and_skips(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    mock_github: GitHubMock,
) -> None:
    """A draft past its inactivity threshold is tombstoned pre-import.

    ``draft_inactivity`` does not read ``live_refs`` — the rule fires
    purely on edition kind + age. This test seeds GitHub refs so the
    binding resolves cleanly, then backdates the LTD draft's
    ``date_rebuilt`` / ``date_created`` well past the configured
    inactivity threshold so the proactive evaluator tombstones it.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug="ks-proactive-stale",
            lifecycle_rules=LifecycleRuleSet(
                root=[DraftInactivityRule(max_days_inactive=30)]
            ),
        )

    draft_payload = _load("edition_branch_git_refs.json")
    # Backdate ``date_rebuilt`` and ``date_created`` past the threshold.
    stale_date = (datetime.now(tz=UTC) - timedelta(days=60)).isoformat()
    draft_payload["date_rebuilt"] = stale_date
    draft_payload["date_created"] = stale_date
    _seed_two_editions_main_and_draft(
        mock_discovery, draft_payload=draft_payload
    )
    _seed_github_refs(
        mock_github.router,
        owner=_LSST_OWNER,
        repo=_LSST_REPO,
        branches=["main", "u/jsick/feature"],
        tags=[],
    )
    draft_build_route = mock_discovery.get(f"{LTD_BASE}/builds/43")

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>draft</html>",
    }
    resolver, fetcher, tombstone_service = _make_proactive_deps(
        session=db_session,
        http_client=http_client,
        mock_github=mock_github,
    )
    service = _build_service(
        db_session,
        http_client,
        object_store,
        source_objects,
        binding_resolver=resolver,
        ref_set_fetcher=fetcher,
        tombstone_service=tombstone_service,
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # Only the main edition produced an outcome.
    assert len(result.edition_outcomes) == 1
    assert result.edition_outcomes[0].docverse_slug == "__main"
    assert draft_build_route.call_count == 0

    state_store = KeeperSyncStateStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        tombstone = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            include_tombstoned=True,
        )
        assert tombstone is not None
        assert tombstone.tombstone_reason == (
            TombstoneReason.lifecycle_preemptive.value
        )


@pytest.mark.asyncio
async def test_proactive_keep_proceeds_through_sync_edition(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    mock_github: GitHubMock,
) -> None:
    """A draft whose ref still exists is NOT tombstoned and syncs normally.

    ``RefDeletedRule`` is configured. GitHub lists the draft's branch
    as live, so the proactive evaluator returns KEEP and the regular
    ``sync_edition`` flow imports the edition + build.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug="ks-proactive-keep",
            lifecycle_rules=LifecycleRuleSet(root=[RefDeletedRule()]),
        )

    draft_payload = _load("edition_branch_git_refs.json")
    draft_payload["tracked_refs"] = ["u/jsick/feature"]
    _seed_two_editions_main_and_draft(
        mock_discovery, draft_payload=draft_payload
    )
    _seed_github_refs(
        mock_github.router,
        owner=_LSST_OWNER,
        repo=_LSST_REPO,
        branches=["main", "u/jsick/feature"],
        tags=[],
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>draft</html>",
    }
    resolver, fetcher, tombstone_service = _make_proactive_deps(
        session=db_session,
        http_client=http_client,
        mock_github=mock_github,
    )
    service = _build_service(
        db_session,
        http_client,
        object_store,
        source_objects,
        binding_resolver=resolver,
        ref_set_fetcher=fetcher,
        tombstone_service=tombstone_service,
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # Both editions produced outcomes — the draft was kept and synced.
    assert len(result.edition_outcomes) == 2
    outcome_slugs = {o.docverse_slug for o in result.edition_outcomes}
    assert "__main" in outcome_slugs
    assert "u-jsick-feature" in outcome_slugs

    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    edition_store = EditionStore(
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
        draft = await edition_store.get_by_slug(
            project_id=project.id, slug="u-jsick-feature"
        )
        assert draft is not None
        # No tombstone for the kept edition.
        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            include_tombstoned=True,
        )
        assert state is not None
        assert state.date_tombstoned is None


@pytest.mark.asyncio
async def test_proactive_build_history_orphan_never_fires(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    mock_github: GitHubMock,
) -> None:
    """``build_history_orphan`` is excluded from the proactive filter.

    Even when configured on the org, the rule must NOT fire during
    ``sync_project`` — it matches against the Docverse build-history
    chain that does not exist until import, and is owned by the
    post-import ``lifecycle_eval`` pass. The draft edition syncs
    normally despite the rule being configured.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug="ks-proactive-orphan",
            lifecycle_rules=LifecycleRuleSet(
                root=[BuildHistoryOrphanRule(min_position=1, min_age_days=0)]
            ),
        )

    draft_payload = _load("edition_branch_git_refs.json")
    _seed_two_editions_main_and_draft(
        mock_discovery, draft_payload=draft_payload
    )
    _seed_github_refs(
        mock_github.router,
        owner=_LSST_OWNER,
        repo=_LSST_REPO,
        branches=["main"],
        tags=[],
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>draft</html>",
    }
    resolver, fetcher, tombstone_service = _make_proactive_deps(
        session=db_session,
        http_client=http_client,
        mock_github=mock_github,
    )
    service = _build_service(
        db_session,
        http_client,
        object_store,
        source_objects,
        binding_resolver=resolver,
        ref_set_fetcher=fetcher,
        tombstone_service=tombstone_service,
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # Both editions synced; no tombstone written.
    assert len(result.edition_outcomes) == 2
    state_store = KeeperSyncStateStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        draft_state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            include_tombstoned=True,
        )
        assert draft_state is not None
        assert draft_state.date_tombstoned is None


@pytest.mark.asyncio
async def test_proactive_fetch_failure_disables_ref_deleted_only(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    mock_github: GitHubMock,
) -> None:
    """A 500 from GitHub falls through to KEEP; editions sync normally.

    The proactive evaluator's ``RepositoryRefFetchError`` handling
    leaves ``live_refs`` unset, which disables ``ref_deleted`` for
    this project pass. The draft tracking a ref that would otherwise
    look deleted is therefore kept (and synced); the regular
    ``git_ref_audit`` cron catches up later if needed.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug="ks-proactive-fetch-fail",
            lifecycle_rules=LifecycleRuleSet(root=[RefDeletedRule()]),
        )

    draft_payload = _load("edition_branch_git_refs.json")
    draft_payload["tracked_refs"] = ["tickets/DM-anything"]
    _seed_two_editions_main_and_draft(
        mock_discovery, draft_payload=draft_payload
    )
    # 500 on the heads endpoint → RepositoryRefFetchError → live_refs
    # stays unset → ref_deleted disabled.
    mock_github.router.get(
        f"{GITHUB_API_BASE_URL}/repos/{_LSST_OWNER}/{_LSST_REPO}"
        "/git/matching-refs/heads"
    ).mock(return_value=httpx.Response(500, json={"message": "boom"}))

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>draft</html>",
    }
    resolver, fetcher, tombstone_service = _make_proactive_deps(
        session=db_session,
        http_client=http_client,
        mock_github=mock_github,
    )
    service = _build_service(
        db_session,
        http_client,
        object_store,
        source_objects,
        binding_resolver=resolver,
        ref_set_fetcher=fetcher,
        tombstone_service=tombstone_service,
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # Both editions synced — fetch failure did not abort the pass.
    assert len(result.edition_outcomes) == 2
    state_store = KeeperSyncStateStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        draft_state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            include_tombstoned=True,
        )
        assert draft_state is not None
        assert draft_state.date_tombstoned is None


@pytest.mark.asyncio
async def test_proactive_no_binding_skips_fetch_and_syncs(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    mock_github: GitHubMock,
) -> None:
    """A project with no GitHub binding skips the fetch entirely.

    Substitutes a non-GitHub ``doc_repo`` (parses to ``None`` via
    ``parse_github_url``) so the resolved binding is ``None``. The
    proactive evaluator must not call GitHub for that project; the
    ``ref_deleted`` rule simply does not fire and editions sync
    normally.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug="ks-proactive-no-binding",
            lifecycle_rules=LifecycleRuleSet(root=[RefDeletedRule()]),
        )

    product_payload = _load("product_pipelines.json")
    product_payload["doc_repo"] = "https://gitlab.example.com/lsst/pipelines"
    draft_payload = _load("edition_branch_git_refs.json")
    draft_payload["tracked_refs"] = ["tickets/DM-anything"]
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=product_payload)
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
    mock_discovery.get(f"{LTD_BASE}/builds/42").mock(
        return_value=httpx.Response(200, json=_load("build.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(200, json=draft_payload)
    )
    draft_build = _load("build.json")
    draft_build["self_url"] = f"{LTD_BASE}/builds/43"
    draft_build["bucket_root_dir"] = "pipelines/builds/43"
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=draft_build)
    )
    # Seed the GitHub refs endpoints so we can prove they were NOT
    # called by the proactive evaluator (the project has no GitHub
    # binding, so no resolve / fetch happens).
    heads_route, tags_route = _seed_github_refs(
        mock_github.router,
        owner=_LSST_OWNER,
        repo=_LSST_REPO,
        branches=["main"],
        tags=[],
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>draft</html>",
    }
    resolver, fetcher, tombstone_service = _make_proactive_deps(
        session=db_session,
        http_client=http_client,
        mock_github=mock_github,
    )
    service = _build_service(
        db_session,
        http_client,
        object_store,
        source_objects,
        binding_resolver=resolver,
        ref_set_fetcher=fetcher,
        tombstone_service=tombstone_service,
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert len(result.edition_outcomes) == 2
    # GitHub endpoints were never called.
    assert heads_route.call_count == 0
    assert tags_route.call_count == 0


@pytest.mark.asyncio
async def test_proactive_ref_set_fetched_once_per_sync_project(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    mock_github: GitHubMock,
) -> None:
    """The ref set is fetched exactly once even with many editions.

    Seeds five LTD editions (one main + four drafts); the GitHub
    matching-refs endpoints must each be hit exactly once across the
    whole ``sync_project`` invocation, demonstrating the per-project
    cache the PRD calls out for GitHub API budget reasons.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug="ks-proactive-once",
            lifecycle_rules=LifecycleRuleSet(root=[RefDeletedRule()]),
        )

    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    edition_urls = [f"{LTD_BASE}/editions/{i}" for i in range(1, 6)]
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(200, json={"editions": edition_urls})
    )
    # main → /editions/1, /builds/42
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(
            200, json=_load("edition_main_git_refs.json")
        )
    )
    mock_discovery.get(f"{LTD_BASE}/builds/42").mock(
        return_value=httpx.Response(200, json=_load("build.json"))
    )
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
    }
    # Four drafts, each on its own ref; all four refs are live so they
    # KEEP and proceed to sync_edition.
    for i, ref in enumerate(["feat-a", "feat-b", "feat-c", "feat-d"], start=2):
        draft_payload = _load("edition_branch_git_refs.json")
        draft_payload["self_url"] = f"{LTD_BASE}/editions/{i}"
        draft_payload["slug"] = f"u-jsick-{ref}"
        draft_payload["title"] = f"u/jsick/{ref}"
        draft_payload["tracked_refs"] = [ref]
        build_id = 100 + i
        draft_payload["build_url"] = f"{LTD_BASE}/builds/{build_id}"
        mock_discovery.get(f"{LTD_BASE}/editions/{i}").mock(
            return_value=httpx.Response(200, json=draft_payload)
        )
        draft_build = _load("build.json")
        draft_build["self_url"] = f"{LTD_BASE}/builds/{build_id}"
        draft_build["bucket_root_dir"] = f"pipelines/builds/{build_id}"
        mock_discovery.get(f"{LTD_BASE}/builds/{build_id}").mock(
            return_value=httpx.Response(200, json=draft_build)
        )
        source_objects[f"pipelines/builds/{build_id}/index.html"] = (
            f"<html>{ref}</html>".encode()
        )

    heads_route, tags_route = _seed_github_refs(
        mock_github.router,
        owner=_LSST_OWNER,
        repo=_LSST_REPO,
        branches=["main", "feat-a", "feat-b", "feat-c", "feat-d"],
        tags=[],
    )

    object_store = MockObjectStore()
    resolver, fetcher, tombstone_service = _make_proactive_deps(
        session=db_session,
        http_client=http_client,
        mock_github=mock_github,
    )
    service = _build_service(
        db_session,
        http_client,
        object_store,
        source_objects,
        binding_resolver=resolver,
        ref_set_fetcher=fetcher,
        tombstone_service=tombstone_service,
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # All five editions synced.
    assert len(result.edition_outcomes) == 5
    # GitHub refs endpoints each hit exactly once.
    assert heads_route.call_count == 1
    assert tags_route.call_count == 1
