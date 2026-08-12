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
import sentry_sdk
import structlog
from safir.github import GitHubAppClientFactory
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    EditionUpdate,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.dbschema.project import SqlProject
from docverse_server.domain.edition import Edition
from docverse_server.domain.lifecycle import (
    BuildHistoryOrphanRule,
    DraftInactivityRule,
    LifecycleRuleSet,
    RefDeletedRule,
)
from docverse_server.exceptions import KeeperSyncSystemicFailureError
from docverse_server.services.keeper_sync.copier import BuildContentCopier
from docverse_server.services.keeper_sync.service import (
    MAX_CONSECUTIVE_EDITION_FAILURES,
    EditionSyncOutcome,
    KeeperSyncContext,
    KeeperSyncService,
    _now,
)
from docverse_server.services.keeper_sync_tombstone import (
    KeeperSyncTombstoneService,
)
from docverse_server.services.project import ProjectService
from docverse_server.services.project_github_binding import (
    ProjectGitHubBindingResolver,
)
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.github import (
    GITHUB_API_BASE_URL,
    GitHubAppClient,
    GitHubRefSetFetcher,
)
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse_server.storage.ltd import (
    LtdClient,
    LtdEdition,
    LtdSourceAccessDeniedError,
    LtdSourceProtocol,
)
from docverse_server.storage.objectstore import MockObjectStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from tests.support.github_mock import DEFAULT_APP_NAME, GitHubMock

FIXTURES_DIR = (
    Path(__file__).parent.parent.parent / "storage" / "ltd" / "fixtures"
)
LTD_BASE = "https://keeper.lsst.codes"


def _load(name: str) -> dict[str, object]:
    payload: dict[str, object] = json.loads((FIXTURES_DIR / name).read_text())
    return payload


class _FakeLtdSource(LtdSourceProtocol):
    """In-memory LTD source for service-level integration tests.

    ``denied_prefixes`` reproduces LTD's oldest uploads: the keys are
    listable but every ``GetObject`` under the prefix answers
    ``AccessDenied`` (#516). ``listed_prefixes`` records every prefix the
    service asked about so a test can pin that a readable build prefix
    never causes the edition-prefix fallback to be consulted.
    """

    def __init__(
        self,
        objects: dict[str, bytes],
        *,
        denied_prefixes: frozenset[str] = frozenset(),
    ) -> None:
        self._objects = objects
        self._denied_prefixes = denied_prefixes
        self.listed_prefixes: list[str] = []

    async def list_keys(self, *, prefix: str) -> list[str]:
        self.listed_prefixes.append(prefix)
        return [k for k in self._objects if k.startswith(prefix)]

    async def download_object(self, *, key: str) -> bytes:
        if any(key.startswith(prefix) for prefix in self._denied_prefixes):
            raise LtdSourceAccessDeniedError(
                bucket="lsst-the-docs", key=key, operation="GetObject"
            )
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
    slug_rewrite_rules: list[dict[str, object]] | None = None,
    edition_autocreation: dict[str, object] | None = None,
) -> int:
    logger = structlog.get_logger("test")
    store = OrganizationStore(session=session, logger=logger)
    org = await store.create(
        OrganizationCreate(
            slug=slug,
            title="ks-svc",
            base_domain=f"{slug}.example.com",
            lifecycle_rules=lifecycle_rules,
            slug_rewrite_rules=slug_rewrite_rules,
        )
    )
    # ``edition_autocreation`` is PATCH-only (not on OrganizationCreate),
    # so it goes on with a direct UPDATE — same shape the edition-tracking
    # tests use.
    if edition_autocreation is not None:
        await session.execute(
            update(SqlOrganization)
            .where(SqlOrganization.id == org.id)
            .values(edition_autocreation=edition_autocreation)
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
    source: _FakeLtdSource | None = None,
) -> KeeperSyncService:
    """Construct a real ``KeeperSyncService`` against the test DB.

    Optional ``binding_resolver`` / ``ref_set_fetcher`` /
    ``tombstone_service`` wire the proactive-lifecycle path; tests
    that don't care about that path leave them unset and get the
    pre-PRD-#332 behavior (proactive pass is a no-op).

    ``source`` overrides the LTD source built from ``source_objects``,
    for tests that need to configure denied prefixes or inspect which
    prefixes the sync consulted.
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
    if source is None:
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
    """LTD build with ``uploaded=False`` must not be synced.

    ``sync_build`` raises; ``sync_project``'s per-edition boundary
    turns that into a reported failure rather than a copied build, so
    the assertion is on the reported failure plus an untouched
    destination store.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    half_uploaded_build = _load("build.json")
    half_uploaded_build["uploaded"] = False
    _seed_ltd(mock_discovery, build_payload=half_uploaded_build)

    object_store = MockObjectStore()
    service = _build_service(db_session, http_client, object_store, {})

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert result.edition_outcomes == []
    assert len(result.edition_failures) == 1
    assert result.edition_failures[0].error_type == "RuntimeError"
    assert "uploaded=False" in result.edition_failures[0].error_message

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
    """A non-main ``lsst_doc`` edition imports as a Docverse release.

    The LTD tracking mode alone decides the kind: this fixture's ref is
    an ordinary ``u/jsick/feature`` branch, which the ref heuristic
    would classify as a draft.
    """
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
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="u-jsick-feature"
        )
        assert edition is not None
        assert edition.kind == EditionKind.release
        assert edition.tracking_mode == TrackingMode.lsst_doc
        assert edition.tracking_params == {}

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


def _seed_three_editions(
    mock_discovery: respx.Router, *, middle_uploaded: bool = True
) -> None:
    """Stub a pipelines product with three ``git_refs`` editions.

    Editions 1 / 2 / 3 map to LTD builds 42 / 43 / 44. ``middle_uploaded
    =False`` makes LTD report edition 2's build as half-uploaded, which
    is what makes ``sync_build`` raise for that edition and no other.
    """
    main_edition = _load("edition_main_git_refs.json")
    middle_edition = _load("edition_branch_git_refs.json")
    last_edition = _load("edition_branch_git_refs.json")
    last_edition["self_url"] = f"{LTD_BASE}/editions/3"
    last_edition["build_url"] = f"{LTD_BASE}/builds/44"
    last_edition["slug"] = "u-jsick-other"
    last_edition["title"] = "u/jsick/other"
    last_edition["tracked_refs"] = ["u/jsick/other"]

    middle_build = _load("build.json")
    middle_build["self_url"] = f"{LTD_BASE}/builds/43"
    middle_build["slug"] = "43"
    middle_build["bucket_root_dir"] = "pipelines/builds/43"
    middle_build["uploaded"] = middle_uploaded
    last_build = _load("build.json")
    last_build["self_url"] = f"{LTD_BASE}/builds/44"
    last_build["slug"] = "44"
    last_build["bucket_root_dir"] = "pipelines/builds/44"

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
                    f"{LTD_BASE}/editions/3",
                ]
            },
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(200, json=main_edition)
    )
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(200, json=middle_edition)
    )
    mock_discovery.get(f"{LTD_BASE}/editions/3").mock(
        return_value=httpx.Response(200, json=last_edition)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/42").mock(
        return_value=httpx.Response(200, json=_load("build.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=middle_build)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/44").mock(
        return_value=httpx.Response(200, json=last_build)
    )


_THREE_EDITION_SOURCE_OBJECTS = {
    "pipelines/builds/42/index.html": b"<html>main</html>",
    "pipelines/builds/43/index.html": b"<html>middle</html>",
    "pipelines/builds/44/index.html": b"<html>last</html>",
}


def _seed_n_branch_editions(
    mock_discovery: respx.Router, *, count: int
) -> None:
    """Stub a ``pipelines`` product with *count* ``git_refs`` editions.

    Editions ``1..count`` each track their own branch and carry their
    own build. Used by the systemic-abort tests, which need more
    editions than the consecutive-failure threshold so the loop has
    somewhere to abort *before*.
    """
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200,
            json={
                "editions": [
                    f"{LTD_BASE}/editions/{i}" for i in range(1, count + 1)
                ]
            },
        )
    )
    for i in range(1, count + 1):
        build_id = 200 + i
        payload = _load("edition_branch_git_refs.json")
        payload["self_url"] = f"{LTD_BASE}/editions/{i}"
        payload["slug"] = f"u-jsick-feat-{i}"
        payload["title"] = f"u/jsick/feat-{i}"
        payload["tracked_refs"] = [f"u/jsick/feat-{i}"]
        payload["build_url"] = f"{LTD_BASE}/builds/{build_id}"
        mock_discovery.get(f"{LTD_BASE}/editions/{i}").mock(
            return_value=httpx.Response(200, json=payload)
        )
        build_payload = _load("build.json")
        build_payload["self_url"] = f"{LTD_BASE}/builds/{build_id}"
        build_payload["slug"] = str(build_id)
        build_payload["bucket_root_dir"] = f"pipelines/builds/{build_id}"
        mock_discovery.get(f"{LTD_BASE}/builds/{build_id}").mock(
            return_value=httpx.Response(200, json=build_payload)
        )


@pytest.mark.asyncio
async def test_sync_project_aborts_after_consecutive_edition_failures(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A systemic outage aborts the loop instead of failing every edition.

    Per-edition isolation is right for a single unreadable LTD build and
    wrong for an LTD outage or a dead database: without an abort, every
    remaining edition is marked failed one by one, the loop finishes,
    and the project job (and its parent run) roll up green on a
    3-of-80 import. ``MAX_CONSECUTIVE_EDITION_FAILURES`` in a row is
    the systemic signal, and the raise is what fails the queue job.
    """
    total = MAX_CONSECUTIVE_EDITION_FAILURES + 3
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-systemic-abort")

    _seed_n_branch_editions(mock_discovery, count=total)
    service = _build_service(db_session, http_client, MockObjectStore(), {})

    attempted: list[int] = []

    async def _always_raises(
        *, ltd_edition: LtdEdition, **kwargs: object
    ) -> None:
        attempted.append(ltd_edition.ltd_id)
        msg = "LTD is down"
        raise RuntimeError(msg)

    monkeypatch.setattr(service, "sync_edition", _always_raises)

    with pytest.raises(KeeperSyncSystemicFailureError) as exc_info:
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # The loop stopped at the threshold rather than walking every
    # remaining edition and reporting them all as isolated failures.
    assert len(attempted) == MAX_CONSECUTIVE_EDITION_FAILURES
    assert attempted == list(range(1, MAX_CONSECUTIVE_EDITION_FAILURES + 1))
    # The triggering error stays on the cause chain for Sentry and for
    # the ``queue_jobs.errors`` traceback the worker records.
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert exc_info.value.ltd_slug == "pipelines"
    assert (
        exc_info.value.consecutive_failures == MAX_CONSECUTIVE_EDITION_FAILURES
    )


@pytest.mark.asyncio
async def test_sync_project_success_resets_consecutive_failure_counter(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One success between failure runs keeps per-edition isolation alive.

    Scattered failures — the permanently-unreadable-build case the
    per-edition boundary exists for — must not accumulate into a false
    systemic abort. Fail up to one short of the threshold, succeed once,
    then fail that many again: the counter resets on the success, so the
    project completes with every failure isolated.
    """
    run = MAX_CONSECUTIVE_EDITION_FAILURES - 1
    total = run * 2 + 1
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-systemic-reset")

    _seed_n_branch_editions(mock_discovery, count=total)
    service = _build_service(db_session, http_client, MockObjectStore(), {})

    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )

    async def _fails_except_middle(
        *, ltd_edition: LtdEdition, **kwargs: object
    ) -> EditionSyncOutcome:
        ltd_id = ltd_edition.ltd_id
        if ltd_id != run + 1:
            msg = "LTD is flaky"
            raise RuntimeError(msg)
        async with db_session.begin():
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
        assert project is not None
        return EditionSyncOutcome(
            docverse_edition_id=None,
            docverse_slug=f"u-jsick-feat-{ltd_id}",
            docverse_project_id=project.id,
            docverse_project_slug=project.slug,
            build_outcome=None,
            short_circuited=False,
        )

    monkeypatch.setattr(service, "sync_edition", _fails_except_middle)

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # No abort: every edition was walked and the failures stayed
    # isolated on the result.
    assert len(result.edition_outcomes) == 1
    assert len(result.edition_failures) == total - 1


@pytest.mark.asyncio
async def test_sync_project_short_circuit_does_not_reset_failure_counter(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tombstone short-circuit must not clear the systemic counter.

    The mirror image of the reset test above: the middle edition returns
    a short-circuited outcome instead of a genuine one. A short-circuit
    never contacts LTD — it is a pure ``keeper_sync_state`` read, which
    succeeds *especially* when LTD is down — so it carries no evidence
    that the outage ended and must leave ``consecutive_failures``
    exactly where it was. On documenteer, whose oldest history
    interleaves ``lifecycle_preemptive`` tombstones with its releases,
    resetting on short-circuits produced the alternating fail/reset
    pattern that defeated the breaker outright.
    """
    run = MAX_CONSECUTIVE_EDITION_FAILURES - 1
    total = MAX_CONSECUTIVE_EDITION_FAILURES + 3
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-systemic-tombstone")

    _seed_n_branch_editions(mock_discovery, count=total)
    service = _build_service(db_session, http_client, MockObjectStore(), {})

    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    attempted: list[int] = []

    async def _fails_around_a_short_circuit(
        *, ltd_edition: LtdEdition, **kwargs: object
    ) -> EditionSyncOutcome:
        ltd_id = ltd_edition.ltd_id
        attempted.append(ltd_id)
        if ltd_id != run + 1:
            msg = "LTD is down"
            raise RuntimeError(msg)
        async with db_session.begin():
            project = await project_store.get_by_slug(
                org_id=org_id, slug="pipelines"
            )
        assert project is not None
        return EditionSyncOutcome(
            docverse_edition_id=None,
            docverse_slug=f"u-jsick-feat-{ltd_id}",
            docverse_project_id=project.id,
            docverse_project_slug=project.slug,
            build_outcome=None,
            short_circuited=True,
        )

    monkeypatch.setattr(service, "sync_edition", _fails_around_a_short_circuit)

    with pytest.raises(KeeperSyncSystemicFailureError) as exc_info:
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # The short-circuited edition was walked — it is not in
    # ``skip_ltd_ids`` — but left the counter alone, so the failure
    # right after it is the Nth in a row and aborts. Editions past it
    # were never attempted.
    assert attempted == list(range(1, MAX_CONSECUTIVE_EDITION_FAILURES + 2))
    assert (
        exc_info.value.consecutive_failures == MAX_CONSECUTIVE_EDITION_FAILURES
    )
    # The reported run spans the short-circuit: the slug list holds only
    # the genuinely-failed editions, and the short-circuited one is
    # absent without having broken the run.
    assert exc_info.value.failed_ltd_edition_slugs == [
        *(f"u-jsick-feat-{i}" for i in range(1, run + 1)),
        f"u-jsick-feat-{run + 2}",
    ]


@pytest.mark.asyncio
async def test_sync_project_proactive_skip_is_counter_neutral(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A proactively-skipped edition neither increments nor resets.

    Editions the proactive lifecycle pass tombstoned never reach
    ``sync_edition`` at all: the loop ``continue``s past them. Pin that
    they stay invisible to the breaker in both directions — the skip is
    not a success (it must not reset a failure run) and not a failure
    (it must not push the counter toward the threshold on its own).
    """
    run = MAX_CONSECUTIVE_EDITION_FAILURES - 1
    total = MAX_CONSECUTIVE_EDITION_FAILURES + 3
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-systemic-skip")

    _seed_n_branch_editions(mock_discovery, count=total)
    service = _build_service(db_session, http_client, MockObjectStore(), {})

    # Stub the pass rather than wiring the proactive deps: this test is
    # about the loop's counter accounting for a skipped id, not about
    # which editions the lifecycle evaluator picks.
    async def _skip_middle(**kwargs: object) -> set[int]:
        return {run + 1}

    monkeypatch.setattr(service, "_proactive_lifecycle_pass", _skip_middle)

    attempted: list[int] = []

    async def _always_raises(
        *, ltd_edition: LtdEdition, **kwargs: object
    ) -> None:
        attempted.append(ltd_edition.ltd_id)
        msg = "LTD is down"
        raise RuntimeError(msg)

    monkeypatch.setattr(service, "sync_edition", _always_raises)

    with pytest.raises(KeeperSyncSystemicFailureError) as exc_info:
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # The skipped edition was never attempted, and the failures either
    # side of it still add up to one systemic run.
    assert run + 1 not in attempted
    assert attempted == [*range(1, run + 1), run + 2]
    assert (
        exc_info.value.consecutive_failures == MAX_CONSECUTIVE_EDITION_FAILURES
    )
    assert (
        len(exc_info.value.failed_ltd_edition_slugs)
        == MAX_CONSECUTIVE_EDITION_FAILURES
    )


@pytest.mark.asyncio
async def test_sync_project_isolates_a_failing_edition(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """One raising edition costs one edition, not the rest of the project.

    Three LTD editions where the middle one's build raises: the outer
    two still sync end-to-end and the result reports exactly one
    failure carrying the LTD edition's slug and id. Without the
    per-edition boundary the exception would abandon edition 3
    entirely — the roundtable-dev documenteer case, where LTD build 33
    is permanently unreadable and stalls 76 readable releases behind
    it.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-isolate")

    _seed_three_editions(mock_discovery, middle_uploaded=False)
    object_store = MockObjectStore()
    service = _build_service(
        db_session,
        http_client,
        object_store,
        dict(_THREE_EDITION_SOURCE_OBJECTS),
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    outcome_slugs = [o.docverse_slug for o in result.edition_outcomes]
    assert outcome_slugs == ["__main", "u-jsick-other"]
    assert len(result.edition_failures) == 1
    failure = result.edition_failures[0]
    assert failure.ltd_edition_slug == "u-jsick-feature"
    assert failure.ltd_edition_id == 2
    assert failure.error_type == "RuntimeError"
    assert "uploaded=False" in failure.error_message

    # The edition *after* the failure really was imported, not just
    # reported: its build content landed in the destination store.
    stored = {obj.data for obj in object_store.objects.values()}
    assert b"<html>main</html>" in stored
    assert b"<html>last</html>" in stored


@pytest.mark.asyncio
async def test_sync_project_captures_edition_failure_to_sentry(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each isolated per-edition failure is reported to Sentry.

    Swallowing the exception must not swallow the alert: the boundary
    reports every failure through ``sentry_sdk.capture_exception`` so
    an operator still sees the LTD build that could not be read.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-isolate-sentry")

    _seed_three_editions(mock_discovery, middle_uploaded=False)
    object_store = MockObjectStore()
    service = _build_service(
        db_session,
        http_client,
        object_store,
        dict(_THREE_EDITION_SOURCE_OBJECTS),
    )

    captured: list[BaseException] = []
    monkeypatch.setattr(sentry_sdk, "capture_exception", captured.append)

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert len(result.edition_failures) == 1
    assert len(captured) == 1
    assert isinstance(captured[0], RuntimeError)


@pytest.mark.asyncio
async def test_sync_project_reports_no_failures_when_all_editions_succeed(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """The all-green project is unchanged: every outcome, no failures."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-isolate-green")

    _seed_three_editions(mock_discovery)
    object_store = MockObjectStore()
    service = _build_service(
        db_session,
        http_client,
        object_store,
        dict(_THREE_EDITION_SOURCE_OBJECTS),
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert [o.docverse_slug for o in result.edition_outcomes] == [
        "__main",
        "u-jsick-feature",
        "u-jsick-other",
    ]
    assert result.edition_failures == ()


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
    # A genuine sync is not short-circuited — the flag is the signal
    # ``sync_project`` uses to decide whether the edition did any work.
    assert first.edition_outcomes[0].short_circuited is False
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
    # Flagged short-circuited so ``sync_project`` keeps it out of its
    # consecutive-failure accounting: nothing here proves LTD is up.
    assert outcome.short_circuited is True

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


def _version_edition_payload(
    *, slug: str, git_ref: str, mode: str = "git_refs"
) -> dict:  # type: ignore[type-arg]
    """Build an LTD edition payload at ``/editions/2`` on a given ref."""
    payload = _load("edition_branch_git_refs.json")
    payload["slug"] = slug
    payload["title"] = slug
    payload["tracked_refs"] = [git_ref]
    payload["mode"] = mode
    return payload


@pytest.mark.asyncio
async def test_sync_imports_version_ref_edition_as_release(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A ``git_refs`` edition tracking ``15.2.1`` imports as a release.

    No slug-rewrite rules are configured, so the classification comes
    entirely from the built-in version heuristics reached through
    ``derive_edition_kind``.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-kind-release")

    _seed_two_editions_main_and_draft(
        mock_discovery,
        draft_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>release</html>",
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
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="15.2.1"
        )
        assert edition is not None
        assert edition.kind == EditionKind.release
        assert edition.tracking_mode == TrackingMode.git_ref
        assert edition.tracking_params == {"git_ref": "15.2.1"}


@pytest.mark.asyncio
async def test_sync_kind_honors_org_slug_rewrite_rules(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An org-configured rule beats the built-in version heuristic.

    Keeper-sync resolves the same project-over-org rule set the native
    upload path uses, so an operator who has deliberately re-tagged a
    grammar keeps that decision on imported editions too.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug="ks-kind-rules",
            slug_rewrite_rules=[{"type": "semver", "edition_kind": "draft"}],
        )

    _seed_two_editions_main_and_draft(
        mock_discovery,
        draft_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>release</html>",
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
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="15.2.1"
        )
        assert edition is not None
        assert edition.kind == EditionKind.draft


@pytest.mark.asyncio
async def test_sync_imports_ticket_branch_edition_as_draft(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Ticket-branch editions keep importing as drafts.

    The version heuristics must not sweep up ordinary branches — these
    editions still need to age out under ``draft_inactivity``.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-kind-draft")

    _seed_two_editions_main_and_draft(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>draft</html>",
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
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="u-jsick-feature"
        )
        assert edition is not None
        assert edition.kind == EditionKind.draft


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ltd_mode", "expected_kind", "expected_tracking_mode"),
    [
        ("lsst_doc", EditionKind.release, TrackingMode.lsst_doc),
        (
            "eups_daily_release",
            EditionKind.draft,
            TrackingMode.eups_daily_release,
        ),
    ],
    ids=["lsst-doc-release", "eups-daily-draft"],
)
async def test_sync_imports_version_mode_editions_by_mode(
    *,
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    ltd_mode: str,
    expected_kind: EditionKind,
    expected_tracking_mode: TrackingMode,
) -> None:
    """LTD version tracking modes decide the kind on their own.

    The tracked ref is deliberately ticket-shaped so a mode-derived
    kind cannot be confused with a ref-heuristic one: ``lsst_doc``
    still imports as a release, ``eups_daily_release`` still as a
    draft.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session, slug=f"ks-mode-{ltd_mode.replace('_', '-')}"
        )

    _seed_two_editions_main_and_draft(
        mock_discovery,
        draft_payload=_version_edition_payload(
            slug="current", git_ref="tickets/DM-1", mode=ltd_mode
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>versioned</html>",
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
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="current"
        )
        assert edition is not None
        assert edition.kind == expected_kind
        assert edition.tracking_mode == expected_tracking_mode


# ---------------------------------------------------------------------------
# Promote-only kind refresh on re-sync (PRD #498 / DM-55772). Editions
# imported before the kind derivation was fixed carry ``draft``; every
# subsequent sync recomputes the derived kind and applies it as a
# ``draft`` -> ``release`` promotion only. Demotions never happen, and
# ``main`` / ``major`` / ``minor`` / ``alternate`` editions — including a
# kind an operator set by hand through the editions PATCH API — are never
# rewritten.
# ---------------------------------------------------------------------------


def _seed_ltd_one_edition(
    mock_discovery: respx.Router,
    *,
    edition_payload: dict,  # type: ignore[type-arg]
) -> None:
    """Stub LTD's view of ``pipelines`` with exactly one edition.

    Deliberately omits LTD's ``main`` edition so these tests can create
    the Docverse project through ``ProjectStore`` (no auto-created
    ``__main``) and still keep the assertions on the one edition under
    test.
    """
    build_payload = _load("build.json")
    build_payload["self_url"] = f"{LTD_BASE}/builds/43"
    build_payload["bucket_root_dir"] = "pipelines/builds/43"
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200, json={"editions": [f"{LTD_BASE}/editions/2"]}
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(200, json=edition_payload)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=build_payload)
    )


async def _seed_project_with_edition(
    session: AsyncSession,
    *,
    org_id: int,
    edition_slug: str,
    kind: EditionKind,
    git_ref: str,
) -> int:
    """Create ``pipelines`` plus one already-imported edition on a ref.

    Stands in for a project keeper-sync has already walked at least
    once: the edition row exists with whatever kind that earlier sync
    (or an operator's PATCH) left behind. Returns the edition id so the
    caller can assert on the same row regardless of slug.
    """
    logger = structlog.get_logger("test")
    project_store = ProjectStore(session=session, logger=logger)
    edition_store = EditionStore(session=session, logger=logger)
    async with session.begin():
        project = await project_store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="pipelines",
                title="LSST Science Pipelines",
                source_url="https://example.com/lsst/pipelines",
            ),
        )
        edition = await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug=edition_slug,
                title=edition_slug,
                kind=kind,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": git_ref},
            ),
        )
    return edition.id


@pytest.mark.asyncio
async def test_resync_promotes_draft_edition_to_release(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A keeper-managed ``draft`` on a version ref heals to ``release``.

    The healing case from PRD #498: safir's 15.x editions were imported
    as drafts before the derivation was mode/rule-driven. Re-syncing
    recomputes the kind and promotes them without any operator action.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-promote")

    edition_id = await _seed_project_with_edition(
        db_session,
        org_id=org_id,
        edition_slug="15.2.1",
        kind=EditionKind.draft,
        git_ref="15.2.1",
    )

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>release</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        edition = await edition_store.get_by_id(edition_id)
    assert edition is not None
    assert edition.kind == EditionKind.release


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("existing_kind", "edition_slug", "git_ref"),
    [
        (EditionKind.release, "u-jsick-feature", "u/jsick/feature"),
        (EditionKind.major, "15", "15.2.1"),
        (EditionKind.minor, "15.2", "15.2.1"),
        (EditionKind.alternate, "15.2.1", "15.2.1"),
    ],
    ids=[
        "never-demote-release",
        "major-untouched",
        "minor-untouched",
        "alternate-untouched",
    ],
)
async def test_resync_kind_refresh_is_promote_only(
    *,
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    existing_kind: EditionKind,
    edition_slug: str,
    git_ref: str,
) -> None:
    """Only ``draft`` -> ``release`` is applied; every other kind holds.

    ``never-demote-release`` derives ``draft`` from a ticket branch
    against an edition already marked ``release`` — the exact shape of a
    manual PATCH that must survive. The aggregate and alternate cases
    derive ``release`` from a version ref but must not disturb a kind
    Docverse itself assigned.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session, slug=f"ks-hold-{existing_kind.value}"
        )

    edition_id = await _seed_project_with_edition(
        db_session,
        org_id=org_id,
        edition_slug=edition_slug,
        kind=existing_kind,
        git_ref=git_ref,
    )

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug=edition_slug, git_ref=git_ref
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>held</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        edition = await edition_store.get_by_id(edition_id)
    assert edition is not None
    assert edition.kind == existing_kind


@pytest.mark.asyncio
async def test_resync_respects_manual_demotion_to_draft(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An operator's ``release`` -> ``draft`` PATCH survives every poll.

    The one case the promote-only allow-list cannot protect on its own:
    a demoted edition sits on ``(draft, release)``, exactly the pair the
    policy permits, so before ``kind_manually_set`` the very next tier
    poll silently promoted it back within minutes. The PATCH here goes
    through ``EditionStore.update``, the real editions-API write path,
    so the flag is stamped the way production stamps it.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-manual-demote")

    edition_id = await _seed_project_with_edition(
        db_session,
        org_id=org_id,
        edition_slug="15.2.1",
        kind=EditionKind.release,
        git_ref="15.2.1",
    )

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        seeded = await edition_store.get_by_id(edition_id)
        assert seeded is not None
        demoted = await edition_store.update(
            project_id=seeded.project_id,
            slug="15.2.1",
            data=EditionUpdate(kind=EditionKind.draft),
        )
    assert demoted is not None
    assert demoted.kind == EditionKind.draft

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>demoted</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    async with db_session.begin():
        edition = await edition_store.get_by_id(edition_id)
    assert edition is not None
    assert edition.kind == EditionKind.draft


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("existing_kind", "expected_kind"),
    [
        (EditionKind.draft, EditionKind.release),
        (EditionKind.major, EditionKind.major),
    ],
    ids=["adopted-draft-promoted", "adopted-major-untouched"],
)
async def test_resync_kind_refresh_covers_adopted_editions(
    *,
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    existing_kind: EditionKind,
    expected_kind: EditionKind,
) -> None:
    """The refresh reaches editions adopted by ``git_ref`` too.

    PRD #409 adoption keeps a differently-slugged existing edition
    (``v15.2.1``) rather than inserting the keeper-derived ``15.2.1``.
    That edition is keeper-managed from then on, so the same promote-only
    refresh applies to it.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session, slug=f"ks-adopt-{existing_kind.value}"
        )

    edition_id = await _seed_project_with_edition(
        db_session,
        org_id=org_id,
        edition_slug="v15.2.1",
        kind=existing_kind,
        git_ref="15.2.1",
    )

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>adopted</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        edition = await edition_store.get_by_id(edition_id)
        assert edition is not None
        keeper_slugged = await edition_store.get_by_slug(
            project_id=edition.project_id, slug="15.2.1"
        )
    assert edition.kind == expected_kind
    # Adoption, not a second row under the keeper-derived slug.
    assert keeper_slugged is None


@pytest.mark.asyncio
async def test_resync_realigns_edition_when_create_loses_slug_race(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lost ``create_internal`` race still gets tracking + kind applied.

    ``_ensure_edition`` reads the slug, misses, and inserts — but the
    insert is ``ON CONFLICT DO NOTHING`` on
    ``uq_editions_project_lower_slug``, so a concurrent writer (the
    native ``track_build`` path handling an upload for the same ref)
    that landed the row in the SELECT/INSERT window makes
    ``create_internal`` return *that* row instead. It is an existing
    edition exactly like a ``get_by_slug`` hit, so it must get the same
    tracking realignment and promote-only kind refresh — PRD #498
    promises the kind recompute on *every* sync, and returning the
    concurrent winner verbatim would leave it stale until some later
    sync happened to take the slug-hit path.

    The race window is simulated by making the store's first
    ``get_by_slug`` for this slug miss while the row is really there;
    ``create_internal``'s own re-fetch then returns the winner.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-lost-race")

    # The concurrent winner: same slug, but a stale ref and the draft
    # kind LTD's version ref disagrees with.
    edition_id = await _seed_project_with_edition(
        db_session,
        org_id=org_id,
        edition_slug="15.2.1",
        kind=EditionKind.draft,
        git_ref="u/jsick/stale",
    )

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    original_get_by_slug = EditionStore.get_by_slug
    missed = {"fired": False}

    async def _miss_once(
        self: EditionStore, *, project_id: int, slug: str
    ) -> Edition | None:
        """Miss the first lookup of ``15.2.1`` — the pre-insert SELECT."""
        if slug == "15.2.1" and not missed["fired"]:
            missed["fired"] = True
            return None
        return await original_get_by_slug(
            self, project_id=project_id, slug=slug
        )

    monkeypatch.setattr(EditionStore, "get_by_slug", _miss_once)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>release</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert missed["fired"] is True
    # The sync reports the concurrently-inserted row, not a second one.
    outcome = result.edition_outcomes[0]
    assert outcome.docverse_edition_id == edition_id

    monkeypatch.undo()
    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        edition = await edition_store.get_by_id(edition_id)
    assert edition is not None
    assert edition.kind == EditionKind.release
    assert edition.tracking_mode == TrackingMode.git_ref
    assert edition.tracking_params == {"git_ref": "15.2.1"}


@pytest.mark.asyncio
async def test_sync_fresh_edition_create_needs_no_kind_rewrite(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuinely fresh insert lands its params without a kind write.

    The lost-race fix applies the tracking/kind refresh after *every*
    ``create_internal``, which is only safe because both are no-ops on a
    row that was just inserted with exactly those params: the
    promote-only refresh sees ``(release, release)``, which is not a
    promotion, so ``update_kind`` is never issued.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-fresh-create")
    await _seed_project(db_session, org_id=org_id)

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    original_update_kind = EditionStore.update_kind
    kind_writes: list[EditionKind] = []

    async def _record_update_kind(
        self: EditionStore, *, edition_id: int, kind: EditionKind
    ) -> None:
        kind_writes.append(kind)
        await original_update_kind(self, edition_id=edition_id, kind=kind)

    monkeypatch.setattr(EditionStore, "update_kind", _record_update_kind)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>release</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert kind_writes == []

    monkeypatch.undo()
    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="15.2.1"
        )
    assert edition is not None
    assert edition.kind == EditionKind.release
    assert edition.tracking_mode == TrackingMode.git_ref
    assert edition.tracking_params == {"git_ref": "15.2.1"}


# ---------------------------------------------------------------------------
# Semver aggregate backfill (PRD #498 / DM-55772). Syncing a stable-semver
# release edition creates/points the ``N`` / ``N.M`` aggregates the native
# upload path would have created, so migrated projects render the same
# dashboard groups. Gated on the resolved ``edition_autocreation`` config.
# ---------------------------------------------------------------------------


async def _seed_project(
    session: AsyncSession,
    *,
    org_id: int,
    edition_autocreation: dict[str, object] | None = None,
    slug_rewrite_rules: list[dict[str, object]] | None = None,
) -> int:
    """Create the ``pipelines`` project before sync ever walks it.

    Pairs with :func:`_seed_ltd_one_edition` (which omits LTD's ``main``
    edition), because ``ProjectStore.create`` does not auto-create the
    ``__main`` edition that ``ProjectService.create`` would.
    """
    logger = structlog.get_logger("test")
    project_store = ProjectStore(session=session, logger=logger)
    async with session.begin():
        project = await project_store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="pipelines",
                title="LSST Science Pipelines",
                source_url="https://example.com/lsst/pipelines",
            ),
        )
        # ``slug_rewrite_rules`` goes on with a direct UPDATE (it is
        # PATCH-only, not on ``ProjectCreate``); an explicit ``[]`` is a
        # meaningful value here, so the guard tests for ``None``.
        project_values: dict[str, object] = {}
        if edition_autocreation is not None:
            project_values["edition_autocreation"] = edition_autocreation
        if slug_rewrite_rules is not None:
            project_values["slug_rewrite_rules"] = slug_rewrite_rules
        if project_values:
            await session.execute(
                update(SqlProject)
                .where(SqlProject.id == project.id)
                .values(**project_values)
            )
    return project.id


@pytest.mark.asyncio
async def test_sync_kind_empty_project_rules_opt_out_of_org(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An explicit empty project rule list opts out of the org's rules.

    The mirror of the native edition-tracking path: ``[]`` is a
    deliberate override, not "unset", so the org's ``semver`` kind
    override must not classify this project's editions and the built-in
    heuristic makes ``15.2.1`` a release.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug="ks-kind-optout",
            slug_rewrite_rules=[{"type": "semver", "edition_kind": "draft"}],
        )
    await _seed_project(db_session, org_id=org_id, slug_rewrite_rules=[])

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>release</html>",
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
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="15.2.1"
        )
        assert edition is not None
        assert edition.kind == EditionKind.release


def _seed_ltd_two_releases(
    mock_discovery: respx.Router,
    *,
    first_ref: str,
    second_ref: str,
) -> None:
    """Stub ``pipelines`` with two semver release editions, in order.

    Edition 2 tracks ``first_ref`` (build 43) and edition 3 tracks
    ``second_ref`` (build 44); LTD lists them in that order. The build
    bodies differ so dual-upload convergence does not collapse the two
    onto one Docverse build row.
    """
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200,
            json={
                "editions": [
                    f"{LTD_BASE}/editions/2",
                    f"{LTD_BASE}/editions/3",
                ]
            },
        )
    )
    for ltd_id, build_id, ref in (
        (2, 43, first_ref),
        (3, 44, second_ref),
    ):
        edition_payload = _version_edition_payload(slug=ref, git_ref=ref)
        edition_payload["self_url"] = f"{LTD_BASE}/editions/{ltd_id}"
        edition_payload["build_url"] = f"{LTD_BASE}/builds/{build_id}"
        build_payload = _load("build.json")
        build_payload["self_url"] = f"{LTD_BASE}/builds/{build_id}"
        build_payload["slug"] = str(build_id)
        build_payload["bucket_root_dir"] = f"pipelines/builds/{build_id}"
        mock_discovery.get(f"{LTD_BASE}/editions/{ltd_id}").mock(
            return_value=httpx.Response(200, json=edition_payload)
        )
        mock_discovery.get(f"{LTD_BASE}/builds/{build_id}").mock(
            return_value=httpx.Response(200, json=build_payload)
        )


@pytest.mark.asyncio
async def test_sync_backfills_semver_aggregate_editions(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Syncing ``15.2.1`` creates ``15`` and ``15.2`` on the same build."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-agg-create")

    _seed_two_editions_main_and_draft(
        mock_discovery,
        draft_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>release</html>",
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
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        release = await edition_store.get_by_slug(
            project_id=project.id, slug="15.2.1"
        )
        major = await edition_store.get_by_slug(
            project_id=project.id, slug="15"
        )
        minor = await edition_store.get_by_slug(
            project_id=project.id, slug="15.2"
        )
    assert release is not None
    assert major is not None
    assert major.kind == EditionKind.major
    assert major.tracking_mode == TrackingMode.semver_major
    assert major.tracking_params == {"major_version": 15}
    assert major.current_build_id == release.current_build_id
    assert minor is not None
    assert minor.kind == EditionKind.minor
    assert minor.tracking_mode == TrackingMode.semver_minor
    assert minor.tracking_params == {"major_version": 15, "minor_version": 2}
    assert minor.current_build_id == release.current_build_id


@pytest.mark.asyncio
async def test_sync_aggregates_suppressed_by_project_config(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A project opting out imports the release with no aggregates."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-agg-off-proj")

    project_id = await _seed_project(
        db_session,
        org_id=org_id,
        edition_autocreation={"semver_aggregates": False},
    )

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>release</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        editions = await edition_store.list_all_by_project(project_id)
    assert {e.slug for e in editions} == {"15.2.1"}


@pytest.mark.asyncio
async def test_sync_aggregates_suppressed_by_org_config(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """The org value applies when the project leaves its own null."""
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug="ks-agg-off-org",
            edition_autocreation={"semver_aggregates": False},
        )

    project_id = await _seed_project(db_session, org_id=org_id)

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>release</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        editions = await edition_store.list_all_by_project(project_id)
    assert {e.slug for e in editions} == {"15.2.1"}


@pytest.mark.asyncio
async def test_sync_aggregates_suppressed_by_kind_rule(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A rule declaring semver tags non-releases suppresses aggregates.

    The gate is the *derived* kind, so an operator who re-points the
    ``semver`` rule at ``draft`` gets no ``15`` / ``15.2`` rows. The
    native upload path gates on the same derivation.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug="ks-agg-kind-rule",
            slug_rewrite_rules=[{"type": "semver", "edition_kind": "draft"}],
        )

    project_id = await _seed_project(db_session, org_id=org_id)

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>release</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        editions = await edition_store.list_all_by_project(project_id)
    assert {e.slug for e in editions} == {"15.2.1"}


@pytest.mark.asyncio
async def test_sync_leaves_non_aggregate_edition_on_aggregate_slug(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An operator's own ``15`` edition is never converted or repointed."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-agg-occupied")

    edition_id = await _seed_project_with_edition(
        db_session,
        org_id=org_id,
        edition_slug="15",
        kind=EditionKind.release,
        git_ref="v15",
    )

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>release</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        occupant = await edition_store.get_by_id(edition_id)
    assert occupant is not None
    assert occupant.kind == EditionKind.release
    assert occupant.tracking_mode == TrackingMode.git_ref
    assert occupant.tracking_params == {"git_ref": "v15"}
    assert occupant.current_build_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("edition_slug", "git_ref", "ltd_mode"),
    [
        ("15.2.1-rc.1", "15.2.1-rc.1", "git_refs"),
        ("v1.0", "v1.0", "lsst_doc"),
        ("w_2026_10", "w_2026_10", "git_refs"),
    ],
    ids=["semver-prerelease", "lsst-doc", "eups-weekly"],
)
async def test_sync_creates_no_aggregates_for_non_semver_releases(
    *,
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    edition_slug: str,
    git_ref: str,
    ltd_mode: str,
) -> None:
    """Only stable semver implies aggregates.

    A prerelease is not a release at all; lsstdoc and EUPS releases are,
    but their grammars have no ``N`` / ``N.M`` rollup — the PRD leaves
    EUPS aggregates out of scope.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session, slug=f"ks-no-agg-{ltd_mode.replace('_', '-')}"
        )

    project_id = await _seed_project(db_session, org_id=org_id)

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug=edition_slug, git_ref=git_ref, mode=ltd_mode
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>versioned</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        editions = await edition_store.list_all_by_project(project_id)
    assert {e.slug for e in editions} == {edition_slug}


@pytest.mark.asyncio
async def test_sync_aggregate_never_moves_backwards(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A later-listed older release must not pull ``15`` back.

    LTD lists editions in no version order, so the aggregate needs the
    same version guard the native tracking path applies.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-agg-guard")

    project_id = await _seed_project(db_session, org_id=org_id)

    _seed_ltd_two_releases(
        mock_discovery, first_ref="15.2.1", second_ref="15.0.0"
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>15.2.1</html>",
        "pipelines/builds/44/index.html": b"<html>15.0.0</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        newest = await edition_store.get_by_slug(
            project_id=project_id, slug="15.2.1"
        )
        major = await edition_store.get_by_slug(
            project_id=project_id, slug="15"
        )
    assert newest is not None
    assert major is not None
    assert major.current_build_id == newest.current_build_id


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
async def test_proactive_draft_inactivity_exempts_version_slugged_edition(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    mock_github: GitHubMock,
) -> None:
    """A stale version-slugged LTD edition is imported, not tombstoned.

    This is the regression the PRD was written for: the transient
    edition the proactive pass evaluates used to be hardcoded
    ``draft``, so ``draft_inactivity`` tombstoned every old LTD release
    ``lifecycle_preemptive`` and it never imported. The transient now
    derives ``release`` from the tracked ref, and ``draft_inactivity``
    only matches drafts — so age is irrelevant here.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug="ks-proactive-release",
            lifecycle_rules=LifecycleRuleSet(
                root=[DraftInactivityRule(max_days_inactive=30)]
            ),
        )

    release_payload = _version_edition_payload(slug="15.2.1", git_ref="15.2.1")
    # Far older than the inactivity threshold — under the old
    # derivation this alone was enough to tombstone the edition.
    stale_date = (datetime.now(tz=UTC) - timedelta(days=900)).isoformat()
    release_payload["date_rebuilt"] = stale_date
    release_payload["date_created"] = stale_date
    _seed_two_editions_main_and_draft(
        mock_discovery, draft_payload=release_payload
    )
    _seed_github_refs(
        mock_github.router,
        owner=_LSST_OWNER,
        repo=_LSST_REPO,
        branches=["main"],
        tags=["15.2.1"],
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>release</html>",
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

    # Both editions synced — the release was never vetoed.
    assert len(result.edition_outcomes) == 2
    assert "15.2.1" in {o.docverse_slug for o in result.edition_outcomes}

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
        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            include_tombstoned=True,
        )
        assert state is not None
        assert state.date_tombstoned is None

        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="15.2.1"
        )
        assert edition is not None
        assert edition.kind == EditionKind.release


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


@pytest.mark.asyncio
async def test_sync_edition_isolates_aggregate_backfill_failure(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A raising aggregate backfill must not discard the edition sync.

    The backfill runs *after* ``sync_build`` has committed the build row
    and pointed the edition at it. Letting its exception escape would
    hand the whole edition to ``sync_project``'s per-edition boundary:
    no ``EditionSyncOutcome``, so ``on_edition_synced`` never enqueues
    the publish and the tail-end self-heal (which iterates only
    ``edition_outcomes``) cannot recover it — an edition whose content
    imported completely would be reported failed and never published.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-agg-isolate")

    _seed_two_editions_main_and_draft(
        mock_discovery,
        draft_payload=_version_edition_payload(
            slug="15.2.1", git_ref="15.2.1"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>release</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )

    # Fail *inside* the backfill's own ``session.begin()`` block rather
    # than stubbing the whole method, so the real semver guards still
    # run: ``__main`` is not a release and never reaches this, and only
    # the ``15.2.1`` release edition's backfill blows up — the shape of
    # the transient DB error this boundary exists to absorb.
    async def _raising_ensure_aggregate(**kwargs: object) -> None:
        msg = "aggregate backfill blew up"
        raise RuntimeError(msg)

    monkeypatch.setattr(
        service, "_ensure_aggregate_edition", _raising_ensure_aggregate
    )

    captured: list[BaseException] = []
    monkeypatch.setattr(sentry_sdk, "capture_exception", captured.append)

    published: list[str] = []

    async def on_edition_synced(outcome: object) -> None:
        published.append(outcome.docverse_slug)  # type: ignore[attr-defined]

    result = await service.sync_project(
        org_id=org_id,
        ltd_slug="pipelines",
        on_edition_synced=on_edition_synced,
    )

    # The edition sync is *not* converted into a failure.
    assert result.edition_failures == ()
    outcomes = {o.docverse_slug: o for o in result.edition_outcomes}
    assert set(outcomes) == {"__main", "15.2.1"}
    release_outcome = outcomes["15.2.1"]
    assert release_outcome.build_outcome is not None
    assert release_outcome.build_outcome.docverse_build_id is not None
    # No aggregates moved, because the backfill never got that far.
    assert release_outcome.aggregate_outcomes == ()

    # The publish enqueue still fires for the successfully-synced edition.
    assert published == ["__main", "15.2.1"]

    # The failure is still visible to an operator via Sentry.
    assert len(captured) == 1
    assert isinstance(captured[0], RuntimeError)

    # The edition really did import: it points at its copied build.
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
        release = await edition_store.get_by_slug(
            project_id=project.id, slug="15.2.1"
        )
    assert release is not None
    assert release.current_build_id is not None


async def _manifest_hash_of(source: _FakeLtdSource, *, prefix: str) -> str:
    """Hash ``prefix`` through a throwaway copier, uploading nothing."""
    copier = BuildContentCopier(
        source=source,
        destination=MockObjectStore(),
        logger=structlog.get_logger("test"),
    )
    return await copier.compute_manifest_hash(source_prefix=prefix)


@pytest.mark.asyncio
async def test_denied_build_prefix_recovers_from_edition_prefix(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An ``AccessDenied`` build prefix syncs from ``<product>/v/<slug>/``.

    LTD's earliest build uploads carry no public-read object ACL, but the
    publish step's copy under the edition prefix does — that copy is what
    Fastly serves (#516). The recovered build must carry the *edition*
    prefix's manifest hash, because those are the bytes that were
    uploaded, and the state row must record where they came from.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-denied-recover")

    _seed_two_editions_main_and_draft(
        mock_discovery,
        draft_payload=_version_edition_payload(slug="0.3.0", git_ref="0.3.0"),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        # The un-ACL'd build upload: listable, never readable.
        "pipelines/builds/43/index.html": b"<html>denied</html>",
        # LTD's published copy of the same build, which is public.
        "pipelines/v/0.3.0/index.html": b"<html>release</html>",
        "pipelines/v/0.3.0/assets/app.js": b"console.log(1)",
    }
    source = _FakeLtdSource(
        source_objects, denied_prefixes=frozenset({"pipelines/builds/43/"})
    )
    service = _build_service(
        db_session, http_client, object_store, source_objects, source=source
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    assert result.edition_failures == ()

    edition_prefix_hash = await _manifest_hash_of(
        source, prefix="pipelines/v/0.3.0/"
    )

    logger = structlog.get_logger("test")
    project_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)

    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        release = await edition_store.get_by_slug(
            project_id=project.id, slug="0.3.0"
        )
        assert release is not None
        assert release.current_build_id is not None
        build = await build_store.get_by_id(release.current_build_id)
        assert build is not None
        assert build.status == BuildStatus.completed
        # Hash and copy read the same prefix: the stored hash is the
        # edition prefix's manifest, not the denied build prefix's.
        assert build.content_hash == edition_prefix_hash
        assert build.object_count == 2

        # Provenance: the bytes did not come from ``bucket_root_dir``.
        state = await state_store.get(
            org_id=org_id, resource_type=ResourceType.build, ltd_id=43
        )
        assert state is not None
        assert state.annotations is not None
        assert state.annotations["ltd_source_prefix"] == "pipelines/v/0.3.0/"
        assert state.annotations["ltd_source_prefix_origin"] == "edition"

    # The uploaded bytes are the edition prefix's, so the manifest hash
    # really does describe what landed in the destination.
    uploaded = {
        key: value
        for key, value in object_store.objects.items()
        if key.startswith(build.storage_prefix)
    }
    assert len(uploaded) == 2
    index_key = next(k for k in uploaded if k.endswith("/index.html"))
    assert uploaded[index_key].data == b"<html>release</html>"


@pytest.mark.asyncio
async def test_readable_build_prefix_never_consults_edition_prefix(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A readable build prefix is the only prefix a sync touches.

    The fallback is a recovery path, not a new default: projects whose
    build prefixes read fine must produce the same hash from the same
    prefix as before #516, with no extra bucket traffic.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-readable-build")

    _seed_two_editions_main_and_draft(
        mock_discovery,
        draft_payload=_version_edition_payload(slug="0.3.0", git_ref="0.3.0"),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>release</html>",
        # Present but irrelevant: nothing should read the published copy.
        "pipelines/v/0.3.0/index.html": b"<html>stale</html>",
    }
    source = _FakeLtdSource(source_objects)
    service = _build_service(
        db_session, http_client, object_store, source_objects, source=source
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    assert result.edition_failures == ()

    assert "pipelines/builds/43/" in source.listed_prefixes
    assert not [
        p for p in source.listed_prefixes if p.startswith("pipelines/v/")
    ]

    build_prefix_hash = await _manifest_hash_of(
        source, prefix="pipelines/builds/43/"
    )
    logger = structlog.get_logger("test")
    project_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        release = await edition_store.get_by_slug(
            project_id=project.id, slug="0.3.0"
        )
        assert release is not None
        assert release.current_build_id is not None
        build = await build_store.get_by_id(release.current_build_id)
        assert build is not None
        assert build.content_hash == build_prefix_hash

        state = await state_store.get(
            org_id=org_id, resource_type=ResourceType.build, ltd_id=43
        )
        assert state is not None
        assert state.annotations is not None
        assert state.annotations["ltd_source_prefix"] == "pipelines/builds/43/"
        assert state.annotations["ltd_source_prefix_origin"] == "build"


@pytest.mark.asyncio
async def test_empty_build_prefix_does_not_trigger_edition_fallback(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An empty build prefix is not a denial, so no fallback fires.

    The recovery path is scoped to ``AccessDenied`` specifically: a
    missing or empty build prefix keeps today's behavior (an empty
    manifest), rather than silently importing whatever the edition
    prefix happens to hold.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-empty-build")

    _seed_two_editions_main_and_draft(
        mock_discovery,
        draft_payload=_version_edition_payload(slug="0.3.0", git_ref="0.3.0"),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        # Nothing at all under pipelines/builds/43/.
        "pipelines/v/0.3.0/index.html": b"<html>published</html>",
    }
    source = _FakeLtdSource(source_objects)
    service = _build_service(
        db_session, http_client, object_store, source_objects, source=source
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    assert result.edition_failures == ()
    assert not [
        p for p in source.listed_prefixes if p.startswith("pipelines/v/")
    ]

    logger = structlog.get_logger("test")
    project_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        release = await edition_store.get_by_slug(
            project_id=project.id, slug="0.3.0"
        )
        assert release is not None
        assert release.current_build_id is not None
        build = await build_store.get_by_id(release.current_build_id)
        assert build is not None
        assert build.object_count == 0


@pytest.mark.asyncio
async def test_main_edition_denial_does_not_try_the_edition_prefix(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """LTD serves the default edition from the product root, not ``v/``.

    So a denied ``main`` build has no published sibling to recover from
    and must keep failing its edition rather than importing some other
    prefix's content.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-main-denied")

    _seed_ltd(mock_discovery)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>denied</html>",
        "pipelines/v/main/index.html": b"<html>not the main edition</html>",
    }
    source = _FakeLtdSource(
        source_objects, denied_prefixes=frozenset({"pipelines/builds/42/"})
    )
    service = _build_service(
        db_session, http_client, object_store, source_objects, source=source
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert len(result.edition_failures) == 1
    failure = result.edition_failures[0]
    assert failure.ltd_edition_slug == "main"
    assert failure.error_type == "LtdSourceAccessDeniedError"
    assert not [
        p for p in source.listed_prefixes if p.startswith("pipelines/v/")
    ]
    assert object_store.objects == {}


@pytest.mark.asyncio
async def test_denied_build_prefix_with_empty_edition_prefix_still_fails(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An empty edition prefix is not a recovery; the denial stands.

    Importing a zero-object build would turn a loud, retryable
    ``AccessDenied`` into an edition that looks synced and serves 404s,
    so the fallback only counts when it actually finds content.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-empty-fallback")

    _seed_two_editions_main_and_draft(
        mock_discovery,
        draft_payload=_version_edition_payload(slug="0.3.0", git_ref="0.3.0"),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        "pipelines/builds/43/index.html": b"<html>denied</html>",
        # Nothing was ever published under pipelines/v/0.3.0/.
    }
    source = _FakeLtdSource(
        source_objects, denied_prefixes=frozenset({"pipelines/builds/43/"})
    )
    service = _build_service(
        db_session, http_client, object_store, source_objects, source=source
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert len(result.edition_failures) == 1
    failure = result.edition_failures[0]
    assert failure.ltd_edition_slug == "0.3.0"
    assert failure.error_type == "LtdSourceAccessDeniedError"
    # The fallback was attempted, and found nothing.
    assert "pipelines/v/0.3.0/" in source.listed_prefixes

    logger = structlog.get_logger("test")
    project_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    async with db_session.begin():
        project = await project_store.get_by_slug(
            org_id=org_id, slug="pipelines"
        )
        assert project is not None
        release = await edition_store.get_by_slug(
            project_id=project.id, slug="0.3.0"
        )
    assert release is not None
    assert release.current_build_id is None
