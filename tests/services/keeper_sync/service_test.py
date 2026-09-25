"""Integration tests for ``KeeperSyncService``.

Wires real ``ProjectService`` / ``BuildStore`` / ``EditionStore`` against
the alembic test DB, stubs LTD HTTP via ``respx``, and uses a real
:class:`BuildContentCopier` with an in-memory LTD source plus
:class:`MockObjectStore` so the destination assertions can verify both
state-store rows and copied object bytes.
"""

from __future__ import annotations

import asyncio
import io
import json
import tarfile
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Callable,
    MutableMapping,
    Sequence,
)
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio
import respx
import sentry_sdk
import structlog
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    EndpointConnectionError,
    ReadTimeoutError,
)
from safir.dependencies.db_session import db_session_dependency
from safir.github import GitHubAppClientFactory
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    EditionKindSource,
    EditionUpdate,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.dbschema.project import SqlProject
from docverse_server.domain.content_hash import PLACEHOLDER_CONTENT_HASH
from docverse_server.domain.edition import DEFAULT_EDITION_SLUG, Edition
from docverse_server.domain.lifecycle import (
    BuildHistoryOrphanRule,
    DraftInactivityRule,
    LifecycleRuleSet,
    RefDeletedRule,
)
from docverse_server.exceptions import (
    MAX_REPORTED_EDITION_SLUGS,
    InvalidBuildStateError,
    KeeperSyncSystemicFailureError,
)
from docverse_server.services.keeper_sync import service as service_module
from docverse_server.services.keeper_sync.copier import (
    BuildContentCopier,
    CopyResult,
    CopyTally,
)
from docverse_server.services.keeper_sync.service import (
    DEFAULT_COPY_RETRY_DELAY_SECONDS,
    MAX_CONSECUTIVE_EDITION_FAILURES,
    BuildCopiedCallback,
    BuildCopyReport,
    BuildSyncOutcome,
    CopyCallable,
    EditionSyncOutcome,
    KeeperSyncContext,
    KeeperSyncService,
    _now,
    _retryable_transport_error,
)
from docverse_server.services.keeper_sync_tombstone import (
    KeeperSyncTombstoneService,
)
from docverse_server.services.lock_service import LockKey, LockService
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
    LtdBuild,
    LtdClient,
    LtdEdition,
    LtdSourceAccessDeniedError,
    LtdSourceProtocol,
)
from docverse_server.storage.objectstore import MockObjectStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.worker.functions.build_processing import _process_build
from tests.support.github_mock import DEFAULT_APP_NAME, GitHubMock
from tests.support.lock_service_spy import RecordingLockService
from tests.support.objectstore import ScriptedUploadStore
from tests.support.rowlocks import (
    LOCK_WAIT_TIMEOUT,
    backend_pid,
    wait_until_blocked_or_finished,
)

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
    lock_service: LockService | None = None,
    after_manifest_hash: Callable[[str], None] | None = None,
    wrap_copy: Callable[[CopyCallable], CopyCallable] | None = None,
    copy_retry_delay_seconds: float | None = None,
    on_build_copied: BuildCopiedCallback | None = None,
) -> KeeperSyncService:
    """Construct a real ``KeeperSyncService`` against the test DB.

    Optional ``binding_resolver`` / ``ref_set_fetcher`` /
    ``tombstone_service`` wire the proactive-lifecycle path; tests
    that don't care about that path leave them unset and get the
    pre-PRD-#332 behavior (proactive pass is a no-op).

    ``source`` overrides the LTD source built from ``source_objects``,
    for tests that need to configure denied prefixes or inspect which
    prefixes the sync consulted.

    ``after_manifest_hash`` fires with the resolved prefix immediately
    after a successful manifest hash, i.e. inside the exact window
    ``sync_build`` leaves between deciding on a hash and copying the
    bytes. It is the seam the mid-resolution-republish tests use to
    mutate the LTD source at that instant.

    ``wrap_copy`` wraps the real copy callable before the service gets
    it — the seam the build-level copy-retry tests use to fail a copy
    (see :func:`_flaky_copy`). ``copy_retry_delay_seconds`` overrides
    the service's default wait before re-running a failed copy.
    ``on_build_copied`` receives one report per build copy (see
    :func:`_record_copy_reports`).
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

    async def copy_callable(
        source_prefix: str, dest_prefix: str, tally: CopyTally
    ) -> CopyResult:
        return await copier.copy_build(
            source_prefix=source_prefix, dest_prefix=dest_prefix, tally=tally
        )

    copy: CopyCallable = copy_callable
    if wrap_copy is not None:
        copy = wrap_copy(copy)

    async def manifest_callable(source_prefix: str) -> str:
        manifest_hash = await copier.compute_manifest_hash(
            source_prefix=source_prefix
        )
        if after_manifest_hash is not None:
            after_manifest_hash(source_prefix)
        return manifest_hash

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
        copy_callable=copy,
        manifest_callable=manifest_callable,
        logger=logger,
        tombstone_service=tombstone_service,
        binding_resolver=binding_resolver,
        ref_set_fetcher=ref_set_fetcher,
        lock_service=lock_service,
        copy_retry_delay_seconds=(
            copy_retry_delay_seconds
            if copy_retry_delay_seconds is not None
            else DEFAULT_COPY_RETRY_DELAY_SECONDS
        ),
        on_build_copied=on_build_copied,
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


def _make_client_tarball(files: dict[str, bytes]) -> bytes:
    """Build the gzipped tarball a client upload would stage.

    Members carry the ``./`` prefix that ``tar.add(source, arcname=".")``
    produces in the client's ``docverse._tar.create_tarball``, so the
    worker's key normalization is exercised rather than bypassed.
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name=f"./{name}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


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


async def _read_edition_clock(
    session: AsyncSession, *, project_id: int, slug: str
) -> tuple[datetime, datetime]:
    """Read an edition's ``(date_created, date_updated)`` from the database.

    Column-level, so the identity map cannot answer with an entity
    loaded before the clock stamp's Core ``UPDATE``.
    """
    row = (
        await session.execute(
            select(SqlEdition.date_created, SqlEdition.date_updated).where(
                SqlEdition.project_id == project_id,
                SqlEdition.slug == slug,
            )
        )
    ).one()
    return row.date_created, row.date_updated


@pytest.mark.asyncio
async def test_fresh_import_stamps_the_edition_with_ltd_dates(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An imported edition carries LTD's history, not the import moment.

    PRD #706: the version dashboard renders ``editions.date_updated``,
    and every migrated edition read "updated just now". The repoint
    onto the synced build bumps ``date_updated`` through the ORM
    ``onupdate`` in the same visit, so the stamp has to come after it
    to survive.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    _seed_ltd(mock_discovery)
    ltd_edition = LtdEdition.model_validate(
        _load("edition_main_git_refs.json")
    )
    assert ltd_edition.date_rebuilt is not None

    service = _build_service(
        db_session,
        http_client,
        MockObjectStore(),
        {"pipelines/builds/42/index.html": b"<html>v1</html>"},
    )
    with structlog.testing.capture_logs() as logs:
        result = await service.sync_project(
            org_id=org_id, ltd_slug="pipelines"
        )

    assert len(result.edition_outcomes) == 1
    outcome = result.edition_outcomes[0]
    assert outcome.build_outcome is not None
    assert outcome.build_outcome.short_circuited is False
    assert outcome.dates_restamped is True
    assert result.docverse_project_id is not None
    async with db_session.begin():
        assert await _read_edition_clock(
            db_session,
            project_id=result.docverse_project_id,
            slug=DEFAULT_EDITION_SLUG,
        ) == (ltd_edition.date_created, ltd_edition.date_rebuilt)

    # One ``info`` line per restamped row, naming the edition and the
    # clock it replaced — the repoint's "now", not LTD's value.
    restamps = [
        log
        for log in logs
        if log["event"] == "Restamped edition dates from LTD"
    ]
    assert len(restamps) == 1
    assert restamps[0]["log_level"] == "info"
    assert restamps[0]["edition_id"] == outcome.docverse_edition_id
    assert restamps[0]["date_updated"] == ltd_edition.date_rebuilt.isoformat()
    assert datetime.fromisoformat(
        restamps[0]["previous_date_updated"]
    ) > datetime.fromisoformat(restamps[0]["date_updated"])


@pytest.mark.asyncio
async def test_failed_clock_stamp_still_reports_the_edition(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stamp that raises costs the dates, not the edition's outcome.

    By the clock transaction the edition is imported and repointed, and
    its outcome is what the worker's ``on_edition_synced`` callback
    enqueues the publish from. Letting the stamp's error escape would
    report a live edition as failed and leave its CDN pointer
    unpublished; the next visit re-asserts the dates instead.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)
    _seed_ltd(mock_discovery)

    async def failing_stamp(*args: object, **kwargs: object) -> bool:
        raise RuntimeError("stamp exploded")

    monkeypatch.setattr(EditionStore, "set_sync_dates", failing_stamp)
    service = _build_service(
        db_session,
        http_client,
        MockObjectStore(),
        {"pipelines/builds/42/index.html": b"<html>v1</html>"},
    )
    with structlog.testing.capture_logs() as logs:
        result = await service.sync_project(
            org_id=org_id, ltd_slug="pipelines"
        )

    assert result.edition_failures == ()
    assert [o.dates_restamped for o in result.edition_outcomes] == [False]
    assert result.edition_outcomes[0].build_outcome is not None
    failures = [
        log
        for log in logs
        if log["event"]
        == "Edition clock stamp failed; edition sync still succeeded"
    ]
    assert len(failures) == 1
    assert failures[0]["log_level"] == "error"


@pytest.mark.asyncio
async def test_fresh_import_stamp_leaves_the_project_clock(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """The edition stamp never writes ``projects.date_updated``.

    Imports a branch edition into an existing project: its repoint is
    not ``__main``'s, so nothing in the visit is entitled to move the
    project clock that Ook's ``updated_since`` poll reads (PRD #634),
    and the pinned value must come through untouched.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        project = await ProjectStore(
            session=db_session, logger=structlog.get_logger("test")
        ).create(
            org_id=org_id,
            data=ProjectCreate(
                slug="pipelines",
                title="LSST Science Pipelines",
                source_url="https://example.com/lsst/pipelines",
            ),
        )
        pinned = datetime(2020, 1, 2, 3, 4, 5, tzinfo=UTC)
        await db_session.execute(
            update(SqlProject)
            .where(SqlProject.id == project.id)
            .values(date_updated=pinned)
        )

    branch_edition = _load("edition_branch_git_refs.json")
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
    ltd_edition = LtdEdition.model_validate(branch_edition)

    service = _build_service(
        db_session,
        http_client,
        MockObjectStore(),
        {"pipelines/builds/43/index.html": b"<html>branch</html>"},
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert [o.dates_restamped for o in result.edition_outcomes] == [True]
    async with db_session.begin():
        assert await _read_edition_clock(
            db_session, project_id=project.id, slug="u-jsick-feature"
        ) == (ltd_edition.date_created, ltd_edition.date_rebuilt)
        project_clock = (
            await db_session.execute(
                select(SqlProject.date_updated).where(
                    SqlProject.id == project.id
                )
            )
        ).scalar_one()
    assert project_clock == pinned


async def _read_build_clock(
    session: AsyncSession, build_id: int
) -> tuple[datetime, datetime | None]:
    """Read a build's ``(date_created, date_completed)`` from the database.

    Column-level for the same reason as :func:`_read_edition_clock`.
    """
    row = (
        await session.execute(
            select(SqlBuild.date_created, SqlBuild.date_completed).where(
                SqlBuild.id == build_id
            )
        )
    ).one()
    return row.date_created, row.date_completed


@pytest.mark.asyncio
async def test_fresh_import_stamps_the_build_with_ltd_dates(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A synced build carries LTD's build date, not the import moment.

    PRD #706: both the ``date_created`` server default and the
    completion stamp ``_finalize_synced_build`` writes record the copy,
    so a migrated project's builds all read as built during the sync.
    LTD built the content once, at its build's ``date_created``, so
    that is the value of both columns.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)
    _seed_ltd(mock_discovery)
    ltd_build = LtdBuild.model_validate(_load("build.json"))

    service = _build_service(
        db_session,
        http_client,
        MockObjectStore(),
        {"pipelines/builds/42/index.html": b"<html>v1</html>"},
    )
    with structlog.testing.capture_logs() as logs:
        result = await service.sync_project(
            org_id=org_id, ltd_slug="pipelines"
        )

    outcome = result.edition_outcomes[0]
    assert outcome.build_outcome is not None
    assert outcome.build_outcome.short_circuited is False
    build_id = outcome.build_outcome.docverse_build_id
    assert build_id is not None
    assert outcome.dates_restamped is True
    async with db_session.begin():
        assert await _read_build_clock(db_session, build_id) == (
            ltd_build.date_created,
            ltd_build.date_created,
        )

    # One ``info`` line for the build row, beside the edition's.
    restamps = [
        log for log in logs if log["event"] == "Restamped build dates from LTD"
    ]
    assert len(restamps) == 1
    assert restamps[0]["log_level"] == "info"
    assert restamps[0]["build_id"] == build_id
    assert restamps[0]["edition_id"] == outcome.docverse_edition_id
    assert restamps[0]["date_created"] == ltd_build.date_created.isoformat()
    assert datetime.fromisoformat(
        restamps[0]["previous_date_created"]
    ) > datetime.fromisoformat(restamps[0]["date_created"])


@pytest.mark.asyncio
async def test_short_circuited_visit_restamps_a_sync_time_build(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A "state matches LTD" visit still moves the build's clock to LTD's.

    This is the backfill path: a build imported before PRD #706 carries
    the copy's timestamps, and its LTD build never changes again, so the
    only visits it will ever get are short circuits. The date comes back
    from the LTD build ``sync_build`` already fetched before deciding
    to short-circuit — no second request — and the edition row, already
    on LTD's clock, is not what makes the visit report a restamp.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)
    _seed_ltd(mock_discovery)
    ltd_build = LtdBuild.model_validate(_load("build.json"))
    source_objects = {"pipelines/builds/42/index.html": b"<html>v1</html>"}
    first = await _build_service(
        db_session, http_client, MockObjectStore(), source_objects
    ).sync_project(org_id=org_id, ltd_slug="pipelines")
    assert first.edition_outcomes[0].build_outcome is not None
    build_id = first.edition_outcomes[0].build_outcome.docverse_build_id
    assert build_id is not None

    # Put the build back on the clock a pre-PRD import left it with.
    sync_time = datetime(2026, 9, 20, 8, 0, tzinfo=UTC)
    async with db_session.begin():
        await db_session.execute(
            update(SqlBuild)
            .where(SqlBuild.id == build_id)
            .values(date_created=sync_time, date_completed=sync_time)
        )

    build_fetches: list[str] = []
    real_get_build_by_url = LtdClient.get_build_by_url

    async def counting_get_build_by_url(self: LtdClient, url: str) -> LtdBuild:
        build_fetches.append(url)
        return await real_get_build_by_url(self, url)

    monkeypatch.setattr(
        LtdClient, "get_build_by_url", counting_get_build_by_url
    )
    second = await _build_service(
        db_session, http_client, MockObjectStore(), source_objects
    ).sync_project(org_id=org_id, ltd_slug="pipelines")

    outcome = second.edition_outcomes[0]
    assert outcome.build_outcome is not None
    assert outcome.build_outcome.short_circuited is True
    assert outcome.build_outcome.docverse_build_id == build_id
    assert outcome.dates_restamped is True
    assert build_fetches == [f"{LTD_BASE}/builds/42"]
    async with db_session.begin():
        assert await _read_build_clock(db_session, build_id) == (
            ltd_build.date_created,
            ltd_build.date_created,
        )


@pytest.mark.asyncio
async def test_shared_build_keeps_the_earliest_ltd_date(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Two LTD builds of one content share one clock: the earlier one.

    ``sync_build`` converges an LTD build whose bytes a Docverse build
    already holds onto that build, so two LTD editions whose builds
    carry identical content (``main`` and a branch built from the same
    commit) end up on one Docverse row. Stamped verbatim, each visit
    would overwrite the other's date and every poll would report a
    restamp. The content existed from the earlier LTD build on, so that
    date holds and a repeat pass writes nothing.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    main_build = _load("build.json")
    branch_build = _load("build.json")
    branch_build["self_url"] = f"{LTD_BASE}/builds/43"
    branch_build["slug"] = "43"
    branch_build["bucket_root_dir"] = "pipelines/builds/43"
    branch_build["git_refs"] = ["u/jsick/feature"]
    branch_build["date_created"] = "2026-05-02T07:00:00.000000+00:00"
    main_date = LtdBuild.model_validate(main_build).date_created
    assert LtdBuild.model_validate(branch_build).date_created > main_date
    branch_edition = _load("edition_branch_git_refs.json")
    _seed_ltd(
        mock_discovery,
        editions_payload=[
            _load("edition_main_git_refs.json"),
            branch_edition,
        ],
    )
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(200, json=branch_edition)
    )
    mock_discovery.get(f"{LTD_BASE}/builds/43").mock(
        return_value=httpx.Response(200, json=branch_build)
    )
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>same</html>",
        "pipelines/builds/43/index.html": b"<html>same</html>",
    }

    first = await _build_service(
        db_session, http_client, MockObjectStore(), source_objects
    ).sync_project(org_id=org_id, ltd_slug="pipelines")

    build_ids = {
        o.build_outcome.docverse_build_id
        for o in first.edition_outcomes
        if o.build_outcome is not None
    }
    assert len(first.edition_outcomes) == 2
    assert len(build_ids) == 1
    (build_id,) = build_ids
    assert build_id is not None
    async with db_session.begin():
        assert await _read_build_clock(db_session, build_id) == (
            main_date,
            main_date,
        )

    second = await _build_service(
        db_session, http_client, MockObjectStore(), source_objects
    ).sync_project(org_id=org_id, ltd_slug="pipelines")

    assert [o.dates_restamped for o in second.edition_outcomes] == [
        False,
        False,
    ]
    async with db_session.begin():
        assert await _read_build_clock(db_session, build_id) == (
            main_date,
            main_date,
        )


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

    ``sync_build`` raises and ``sync_project``'s per-edition boundary
    catches it rather than copying a half-uploaded build. This product
    has a single edition, so that failure is also the whole run: nothing
    was imported, which the end-of-run check reads as systemic and
    raises. The half-uploaded build's own error stays on the cause
    chain, and the destination store is untouched either way.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    half_uploaded_build = _load("build.json")
    half_uploaded_build["uploaded"] = False
    _seed_ltd(mock_discovery, build_payload=half_uploaded_build)

    object_store = MockObjectStore()
    service = _build_service(db_session, http_client, object_store, {})

    with pytest.raises(KeeperSyncSystemicFailureError) as exc_info:
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert exc_info.value.failed_ltd_edition_slugs == ["main"]
    cause = exc_info.value.__cause__
    assert isinstance(cause, RuntimeError)
    assert "uploaded=False" in str(cause)

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
    """A branch-tracking ``lsst_doc`` edition imports as a draft.

    The LTD mode narrows but does not decide: this fixture tracks an
    ordinary ``u/jsick/feature`` branch. Trusting the mode alone gave it
    a ``release`` kind — the long CDN cache profile, the Releases bucket
    on the dashboard, and permanent exemption from lifecycle reaping —
    with no path back, since nothing ever demoted it.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    branch_edition = _load("edition_branch_lsst_doc.json")
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
        assert edition.kind == EditionKind.draft
        assert edition.kind_source == EditionKindSource.derived
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
    # LTD leaves ``tracked_refs`` null on a ``manual`` edition; both the
    # tracking pair and the synced build's git_ref come from the BUILD's
    # git_refs[0] (#682).
    edition_payload["tracked_refs"] = None
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
    build_store = BuildStore(
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
        assert edition.current_build_id is not None
        build = await build_store.get_by_id(edition.current_build_id)
        assert build is not None
        assert build.git_ref == "v22_0_0"

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
async def test_sync_edition_imports_lsst_doc_main_without_tracked_refs(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An ``lsst_doc`` main edition with ``tracked_refs: null`` imports.

    The shape of every older LDM/DMTN-era product (#682, found on
    ``sqr-028`` in the first production wave): LTD's ``lsst_doc`` mode
    follows the newest semver tag with a ``main`` / ``master`` fallback
    and never fills ``tracked_refs``. The synced build takes its
    ``git_ref`` from the published build's ``git_refs`` instead, and
    the edition keeps ``lsst_doc`` tracking so post-cutover uploads
    follow the same rule rather than pinning to whichever ref happened
    to be published at sync time.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    build_payload = _load("build.json")
    build_payload["git_refs"] = ["master"]
    _seed_ltd(
        mock_discovery,
        edition_main=_load("edition_main_lsst_doc.json"),
        build_payload=build_payload,
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>sqr-028</html>",
    }
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert result.edition_failures == ()
    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    build_store = BuildStore(
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
        assert edition.tracking_mode == TrackingMode.lsst_doc
        assert edition.tracking_params == {}
        assert edition.current_build_id is not None

        build = await build_store.get_by_id(edition.current_build_id)
        assert build is not None
        assert build.status == BuildStatus.completed
        assert build.git_ref == "master"


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

    Both builds here are copier-produced: the baseline comes from a
    first ``sync_project`` run, and the inbound one is LTD republishing
    byte-identical content under a new build prefix. That is the
    re-sync half of the convergence contract from PRD #275 user story
    12 — an unchanged rebuild must not fork a second Docverse row — and
    it pins the branch's bookkeeping rather than the agreement between
    independent producers, which
    ``test_client_upload_and_ltd_sync_converge_on_same_content`` covers
    with a worker-extracted baseline.
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

        # Edition still points at the existing build. Its clock is LTD's
        # (PRD #706): it follows the republish's ``date_rebuilt`` rather
        # than the convergence visit's own time.
        edition_after = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition_after is not None
        assert edition_after.current_build_id == existing_build_id
        assert edition_after.date_updated == datetime(
            2026, 5, 4, 12, tzinfo=UTC
        )


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
async def test_client_upload_and_ltd_sync_converge_on_same_content(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Convergence fires between two genuinely independent producers.

    The baseline build is produced by the worker extraction path — a
    real client tarball unpacked by
    :func:`~docverse_server.worker.functions.build_processing._process_build`,
    which computes and stores the content identity at completion — and
    the inbound build by keeper-sync's copier over the same files in
    LTD's bucket. Neither producer ever sees the other's hash, so the
    match is evidence that the two implementations agree, which is
    exactly what PRD #569 set out to guarantee: while the client's
    gzip-stream digest was the stored identity, a direct upload could
    never match an inbound LTD build and every sync re-copied content
    Docverse already held.

    The LTD side carries the zero-byte directory markers LTD's uploader
    writes alongside a build's real files, because that — not a flat
    listing — is the layout of every Sphinx build in the bucket. Hashing
    them left the two producers permanently apart for any build with a
    subdirectory (#576).
    """
    logger = structlog.get_logger("test")
    content = {
        "index.html": b"<html>v1</html>",
        "assets/app.js": b"console.log(1)",
    }

    async with db_session.begin():
        org_id = await _seed_org(db_session)
        project_service = ProjectService(
            store=ProjectStore(session=db_session, logger=logger),
            org_store=OrganizationStore(session=db_session, logger=logger),
            edition_store=EditionStore(session=db_session, logger=logger),
            logger=logger,
        )
        # Created the way the API creates a project, and carrying the
        # LTD product slug so the sync adopts it rather than creating a
        # second one.
        _, project, _ = await project_service.create(
            org_slug="ks-svc",
            data=ProjectCreate(
                slug="pipelines",
                title="LSST Science Pipelines",
                source_url="https://example.com/lsst/pipelines_lsst_io",
            ),
        )

    object_store = MockObjectStore()
    build_store = BuildStore(session=db_session, logger=logger)

    # Producer 1: the client upload path. The build is created without
    # a content hash (the transport digest is deprecated and optional),
    # so the row holds the placeholder until the worker hashes what it
    # actually extracted.
    async with db_session.begin():
        uploaded = await build_store.create(
            project_id=project.id,
            project_slug=project.slug,
            data=BuildCreate(git_ref="main"),
            uploader="ci-bot",
        )
        assert uploaded.content_hash == PLACEHOLDER_CONTENT_HASH
        await build_store.transition_status(
            build_id=uploaded.id, new_status=BuildStatus.processing
        )
        staged = await build_store.get_by_id(uploaded.id)
        assert staged is not None

    await object_store.upload_object(
        key=staged.staging_key,
        data=_make_client_tarball(content),
        content_type="application/gzip",
    )
    async with db_session.begin():
        await _process_build(
            object_store=object_store,
            build=staged,
            build_store=build_store,
            org_slug="ks-svc",
            project_slug=project.slug,
            logger=logger,
        )

    async with db_session.begin():
        completed = await build_store.get_by_id(uploaded.id)
        assert completed is not None
        assert completed.status == BuildStatus.completed
        assert completed.content_hash != PLACEHOLDER_CONTENT_HASH
    keys_after_upload = set(object_store.objects.keys())
    assert keys_after_upload

    # Producer 2: keeper-sync's copier over the same files, published to
    # LTD under its own bucket prefix.
    _seed_ltd(mock_discovery)
    source_objects = {
        f"pipelines/builds/42/{name}": data for name, data in content.items()
    }
    # LTD's uploader writes a zero-byte "directory marker" object for
    # the build prefix and for every subdirectory under it, so this is
    # the layout every real synced build actually presents. A tarball
    # built from a directory cannot carry them — a file and a directory
    # cannot share a name — so hashing them is what kept the two
    # producers from ever converging on a build with subdirectories
    # (#576).
    source_objects["pipelines/builds/42/"] = b""
    source_objects["pipelines/builds/42/assets"] = b""
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    main_outcome = result.edition_outcomes[0]
    assert main_outcome.build_outcome is not None
    # The copier's manifest hash matched the worker's, so the sync
    # short-circuited onto the uploaded build.
    assert main_outcome.build_outcome.content_hash == completed.content_hash
    assert main_outcome.build_outcome.short_circuited is True
    assert main_outcome.build_outcome.docverse_build_id == uploaded.id
    # Nothing was copied: the destination holds only what the worker
    # uploaded.
    assert set(object_store.objects.keys()) == keys_after_upload

    async with db_session.begin():
        # No second build row for the same content.
        builds = await build_store.list_by_project(project.id, limit=10)
        assert len(builds.entries) == 1
        assert builds.entries[0].id == uploaded.id

        state_store = KeeperSyncStateStore(session=db_session, logger=logger)
        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.build,
            ltd_id=42,
        )
        assert state is not None
        assert state.docverse_id == uploaded.id
        assert state.content_hash == completed.content_hash

        # The LTD edition serves the uploaded build directly.
        edition_store = EditionStore(session=db_session, logger=logger)
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition is not None
        assert edition.current_build_id == uploaded.id


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
    # The message is bounded even though the run recorded 75 slugs: it
    # is copied into ``queue_jobs.errors`` and the Sentry issue title.
    rendered = str(exc_info.value)
    assert (
        f"+{MAX_CONSECUTIVE_EDITION_FAILURES - MAX_REPORTED_EDITION_SLUGS}"
        " more" in rendered
    )
    assert len(rendered) < 1000


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

    The success in the middle carries a build outcome, because that is
    what "success" means to the breaker: the edition reached LTD. It is
    also what keeps the run off the end-of-loop total-failure abort.
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
            build_outcome=BuildSyncOutcome(
                docverse_build_id=1,
                docverse_build_public_id="BUILD1",
                short_circuited=False,
                content_hash=None,
                object_count=None,
                total_size_bytes=None,
                ltd_date_created=datetime(2026, 4, 30, tzinfo=UTC),
            ),
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
async def test_sync_project_buildless_edition_is_counter_neutral(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An LTD edition with no build must not clear the systemic counter.

    A build-less edition does database-only work: it never resolves an
    LTD build, never reads the LTD bucket, and returns a genuine
    (non-short-circuited) outcome. Like a tombstone short-circuit, it
    succeeds *especially* during an outage, so it carries no evidence
    that LTD or the object store recovered. Counting it as a success
    let any product whose edition list interleaves a build-less edition
    at least once per ``MAX_CONSECUTIVE_EDITION_FAILURES`` positions
    defeat the breaker outright — the same masking the tombstone
    exemption exists to prevent.
    """
    run = MAX_CONSECUTIVE_EDITION_FAILURES - 1
    total = MAX_CONSECUTIVE_EDITION_FAILURES + 3
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-systemic-buildless")

    _seed_n_branch_editions(mock_discovery, count=total)
    service = _build_service(db_session, http_client, MockObjectStore(), {})

    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    attempted: list[int] = []

    async def _fails_around_a_buildless_edition(
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
        # A real sync of a build-less LTD edition: the edition row and
        # its state row were written, but ``sync_build`` never ran, so
        # there is no build outcome and nothing touched LTD.
        return EditionSyncOutcome(
            docverse_edition_id=1,
            docverse_slug=f"u-jsick-feat-{ltd_id}",
            docverse_project_id=project.id,
            docverse_project_slug=project.slug,
            build_outcome=None,
            short_circuited=False,
        )

    monkeypatch.setattr(
        service, "sync_edition", _fails_around_a_buildless_edition
    )

    with pytest.raises(KeeperSyncSystemicFailureError) as exc_info:
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # The build-less edition was walked but left the counter alone, so
    # the failure right after it is the Nth in a row and aborts.
    assert attempted == list(range(1, MAX_CONSECUTIVE_EDITION_FAILURES + 2))
    assert (
        exc_info.value.consecutive_failures == MAX_CONSECUTIVE_EDITION_FAILURES
    )
    # The build-less edition is absent from the reported run without
    # having broken it.
    assert exc_info.value.failed_ltd_edition_slugs == [
        *(f"u-jsick-feat-{i}" for i in range(1, run + 1)),
        f"u-jsick-feat-{run + 2}",
    ]


@pytest.mark.asyncio
async def test_sync_project_fails_when_every_attempted_edition_fails(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A total outage fails the run even below the breaker threshold.

    Per-edition isolation means the consecutive-failure breaker is the
    only path back to a failed job, and a typical migrated project has
    ~20 editions — far fewer than ``MAX_CONSECUTIVE_EDITION_FAILURES``.
    Expired object-store credentials or a dead database would therefore
    fail every edition and still roll the project up
    ``completed_with_errors`` / ``partial_failure`` with zero editions
    imported. A run where every attempted edition failed is systemic by
    construction, whatever the edition count.
    """
    total = 3
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-systemic-total")

    _seed_n_branch_editions(mock_discovery, count=total)
    service = _build_service(db_session, http_client, MockObjectStore(), {})

    attempted: list[int] = []

    async def _always_raises(
        *, ltd_edition: LtdEdition, **kwargs: object
    ) -> None:
        attempted.append(ltd_edition.ltd_id)
        msg = "The object store credentials expired"
        raise RuntimeError(msg)

    monkeypatch.setattr(service, "sync_edition", _always_raises)

    with pytest.raises(KeeperSyncSystemicFailureError) as exc_info:
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    # Nothing short-circuited the loop: every edition was attempted, and
    # the abort fires at the end of the run rather than mid-list.
    assert attempted == list(range(1, total + 1))
    # The last edition's exception stays on the cause chain for Sentry
    # and the ``queue_jobs.errors`` traceback.
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert exc_info.value.ltd_slug == "pipelines"
    assert exc_info.value.consecutive_failures == total
    assert exc_info.value.failed_ltd_edition_slugs == [
        f"u-jsick-feat-{i}" for i in range(1, total + 1)
    ]


@pytest.mark.asyncio
async def test_sync_project_permanent_denials_alone_are_not_systemic(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A permanently-denied edition must not fail the whole project.

    The roundtable-dev shape this guard broke on: a migrated product
    whose history is mostly ``lifecycle_preemptive`` tombstones plus one
    edition whose LTD build is ``AccessDenied`` at both
    ``builds/<slug>/`` and ``v/<slug>/``. Tombstone short-circuits are
    neutral for ``contacted_ltd``, so the run reaches the end with
    ``ltd_successes == 0`` and one failure — and the end-of-run check
    read that as an outage, failing the job, firing Sentry, and handing
    the 5-minute tier cron a job it replays identically forever.

    A denial is permanent by construction (the object carries no
    public-read ACL and the anonymous principal has no credentials to
    acquire one), so it is exactly what the ``completed_with_errors`` /
    ``edition_failures`` partial-success path exists for.
    """
    tombstoned = 19
    total = tombstoned + 1
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-permanent-only")

    _seed_n_branch_editions(mock_discovery, count=total)
    service = _build_service(db_session, http_client, MockObjectStore(), {})

    project_store = ProjectStore(
        session=db_session, logger=structlog.get_logger("test")
    )

    async def _tombstones_then_a_denial(
        *, ltd_edition: LtdEdition, **kwargs: object
    ) -> EditionSyncOutcome:
        ltd_id = ltd_edition.ltd_id
        if ltd_id > tombstoned:
            raise LtdSourceAccessDeniedError(
                bucket="lsst-the-docs",
                key=f"pipelines/builds/{200 + ltd_id}/index.html",
                operation="GetObject",
            )
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

    monkeypatch.setattr(service, "sync_edition", _tombstones_then_a_denial)

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert len(result.edition_failures) == 1
    failure = result.edition_failures[0]
    assert failure.ltd_edition_slug == f"u-jsick-feat-{total}"
    assert failure.error_type == "LtdSourceAccessDeniedError"


@pytest.mark.asyncio
async def test_sync_project_unresolvable_git_ref_is_not_systemic(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An edition with no derivable git ref is a permanent per-edition fault.

    The #682 shape, one step worse: an ``lsst_doc`` main edition with
    ``tracked_refs: null`` whose published build also reports no
    ``git_refs``. That is deterministic for the (edition, build) pair —
    a build's ``git_refs`` is fixed at upload — so failing the job
    would hand the tier cron a replay that fails identically forever,
    and the cause would be visible only in the job traceback. Reporting
    it under ``edition_failures`` puts the reason on the API.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-no-git-ref")

    build_payload = _load("build.json")
    build_payload["git_refs"] = None
    _seed_ltd(
        mock_discovery,
        edition_main=_load("edition_main_lsst_doc.json"),
        build_payload=build_payload,
    )
    service = _build_service(
        db_session,
        http_client,
        MockObjectStore(),
        {"pipelines/builds/42/index.html": b"<html>no ref</html>"},
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert result.edition_outcomes == []
    assert len(result.edition_failures) == 1
    failure = result.edition_failures[0]
    assert failure.ltd_edition_slug == "main"
    assert failure.error_type == "KeeperSyncGitRefUnresolvableError"
    assert "42" in failure.error_message


@pytest.mark.asyncio
async def test_sync_project_manual_edition_without_git_refs_is_not_systemic(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A ``manual`` edition whose build names no ref fails per-edition.

    The same permanent (edition, build) fault as the ``lsst_doc`` case
    above, surfacing one step earlier: ``manual`` needs the published
    build's ``git_refs`` to *map* its tracking pair, so it fails in
    ``map_edition_tracking`` before ``sync_build`` runs. For a product
    whose only edition has that shape, counting it as a systemic
    candidate would fail the job — and the tier cron's replay — forever.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-manual-no-git-ref")

    edition_payload = _load("edition_main_git_refs.json")
    edition_payload["mode"] = "manual"
    edition_payload["tracked_refs"] = None
    build_payload = _load("build.json")
    build_payload["git_refs"] = None
    _seed_ltd(
        mock_discovery,
        edition_main=edition_payload,
        build_payload=build_payload,
    )
    service = _build_service(
        db_session,
        http_client,
        MockObjectStore(),
        {"pipelines/builds/42/index.html": b"<html>no ref</html>"},
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert result.edition_outcomes == []
    assert len(result.edition_failures) == 1
    failure = result.edition_failures[0]
    assert failure.ltd_edition_slug == "main"
    assert failure.error_type == "KeeperSyncGitRefUnresolvableError"
    assert "manual" in failure.error_message
    assert "42" in failure.error_message


@pytest.mark.asyncio
async def test_sync_project_transport_failure_amid_denials_still_aborts(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Discounting permanent faults must not disarm the systemic check.

    Same zero-import run as the test above, but one of the failures is a
    transport error rather than a denial. A transport error *can* be an
    outage, so the run is still systemic and still fails the job — and
    the transport error, not the incidental denial next to it, is what
    lands on the cause chain for triage.
    """
    denied = 2
    total = denied + 1
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-mixed-systemic")

    _seed_n_branch_editions(mock_discovery, count=total)
    service = _build_service(db_session, http_client, MockObjectStore(), {})

    async def _denials_then_a_transport_error(
        *, ltd_edition: LtdEdition, **kwargs: object
    ) -> None:
        if ltd_edition.ltd_id > denied:
            raise httpx.ConnectError("LTD is unreachable")
        raise LtdSourceAccessDeniedError(
            bucket="lsst-the-docs",
            key=f"pipelines/builds/{200 + ltd_edition.ltd_id}/index.html",
            operation="GetObject",
        )

    monkeypatch.setattr(
        service, "sync_edition", _denials_then_a_transport_error
    )

    with pytest.raises(KeeperSyncSystemicFailureError) as exc_info:
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert isinstance(exc_info.value.__cause__, httpx.ConnectError)
    # The count is of everything that failed, not just the systemic
    # evidence — nothing was imported either way.
    assert exc_info.value.consecutive_failures == total


@pytest.mark.asyncio
async def test_sync_project_systemic_message_caps_the_slug_list(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The end-of-run raise names a bounded number of edition slugs.

    ``str(exc)`` lands verbatim in ``queue_jobs.errors['message']`` and
    in the Sentry issue title, so an all-failed documenteer run (279
    editions) must not write the whole slug list into either. The
    exception's attributes still carry every slug.
    """
    total = MAX_REPORTED_EDITION_SLUGS + 5
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-systemic-message")

    _seed_n_branch_editions(mock_discovery, count=total)
    service = _build_service(db_session, http_client, MockObjectStore(), {})

    async def _always_raises(
        *, ltd_edition: LtdEdition, **kwargs: object
    ) -> None:
        msg = "The object store credentials expired"
        raise RuntimeError(msg)

    monkeypatch.setattr(service, "sync_edition", _always_raises)

    with pytest.raises(KeeperSyncSystemicFailureError) as exc_info:
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    rendered = str(exc_info.value)
    assert f"u-jsick-feat-{MAX_REPORTED_EDITION_SLUGS}" in rendered
    assert f"u-jsick-feat-{MAX_REPORTED_EDITION_SLUGS + 1}" not in rendered
    assert "+5 more" in rendered
    assert len(rendered) < 1000
    # The exact count is preserved even though the list is not.
    assert f"all {total} attempted editions failed" in rendered
    assert len(exc_info.value.failed_ltd_edition_slugs) == total


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


def _flaky_copy(
    failures: dict[str, list[BaseException]],
    calls: list[tuple[str, str]],
) -> Callable[[CopyCallable], CopyCallable]:
    """Wrap a copy callable so chosen build copies fail first.

    ``failures`` maps an LTD build prefix (no trailing slash) to the
    exceptions its next copies raise, one per call and in order; once
    a prefix's queue is empty its copies run for real. Every call is
    appended to ``calls`` as ``(source_prefix, dest_prefix)``.
    """

    def wrap(copy: CopyCallable) -> CopyCallable:
        async def flaky(
            source_prefix: str, dest_prefix: str, tally: CopyTally
        ) -> CopyResult:
            calls.append((source_prefix, dest_prefix))
            queued = failures.get(source_prefix.rstrip("/"))
            if queued:
                raise queued.pop(0)
            return await copy(source_prefix, dest_prefix, tally)

        return flaky

    return wrap


def _record_copy_retry_sleeps(
    monkeypatch: pytest.MonkeyPatch, session: AsyncSession
) -> list[tuple[float, bool]]:
    """Stub the service's copy-retry wait and record each one.

    Each entry is ``(delay, in_transaction)``: the second half pins that
    the wait never holds a database transaction open.
    """
    sleeps: list[tuple[float, bool]] = []

    async def _fake_sleep(delay: float) -> None:
        sleeps.append((delay, session.in_transaction()))

    monkeypatch.setattr(service_module, "_sleep", _fake_sleep)
    return sleeps


def _copy_retry_logs(
    logs: Sequence[MutableMapping[str, Any]],
) -> list[MutableMapping[str, Any]]:
    """Pick the build-level copy-retry warnings out of captured logs."""
    return [
        entry
        for entry in logs
        if entry["event"] == "Retrying build copy after transport error"
    ]


@pytest.mark.asyncio
async def test_sync_build_reruns_copy_once_after_transport_failure(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A copy that dies on an R2 connect timeout is re-run, and lands.

    The 2026-09-22 ``sqr-`` campaign lost 38 of 208 jobs to a ~40 s R2
    connect outage that outlasted every per-object retry. The copy is
    content-hashed and idempotent and holds no transaction, so
    ``sync_build`` waits the configured delay and runs it once more into
    the same placeholder build rather than failing the edition.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-copy-retry")

    _seed_ltd(mock_discovery)
    calls: list[tuple[str, str]] = []
    service = _build_service(
        db_session,
        http_client,
        MockObjectStore(),
        {"pipelines/builds/42/index.html": b"<html>v1</html>"},
        wrap_copy=_flaky_copy(
            {"pipelines/builds/42": [httpx.ConnectTimeout("")]}, calls
        ),
        copy_retry_delay_seconds=7.0,
    )
    sleeps = _record_copy_retry_sleeps(monkeypatch, db_session)

    with structlog.testing.capture_logs() as logs:
        result = await service.sync_project(
            org_id=org_id, ltd_slug="pipelines"
        )

    assert result.edition_failures == ()
    outcome = result.edition_outcomes[0].build_outcome
    assert outcome is not None
    assert outcome.short_circuited is False
    assert outcome.object_count == 1
    # Both runs copied into the one placeholder build.
    assert len(calls) == 2
    assert calls[0] == calls[1]
    assert sleeps == [(7.0, False)]
    retries = _copy_retry_logs(logs)
    assert len(retries) == 1
    assert retries[0]["log_level"] == "warning"
    assert retries[0]["error_type"] == "ConnectTimeout"
    assert retries[0]["retry_delay"] == 7.0


@pytest.mark.asyncio
async def test_sync_build_propagates_second_copy_transport_failure(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The copy is re-run once, not until it succeeds.

    A second transport failure fails the edition exactly as a first one
    used to: ``sync_project``'s per-edition boundary records it and
    reports the *second* exception, unchained from the first, while the
    editions either side still sync.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-copy-retry-twice")

    _seed_three_editions(mock_discovery)
    first = httpx.ConnectTimeout("first")
    second = httpx.ConnectTimeout("second")
    calls: list[tuple[str, str]] = []
    service = _build_service(
        db_session,
        http_client,
        MockObjectStore(),
        dict(_THREE_EDITION_SOURCE_OBJECTS),
        wrap_copy=_flaky_copy({"pipelines/builds/43": [first, second]}, calls),
        copy_retry_delay_seconds=7.0,
    )
    sleeps = _record_copy_retry_sleeps(monkeypatch, db_session)
    captured: list[BaseException] = []
    monkeypatch.setattr(sentry_sdk, "capture_exception", captured.append)

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    middle_calls = [c for c in calls if c[0].startswith("pipelines/builds/43")]
    assert len(middle_calls) == 2
    assert sleeps == [(7.0, False)]
    assert [o.docverse_slug for o in result.edition_outcomes] == [
        "__main",
        "u-jsick-other",
    ]
    assert len(result.edition_failures) == 1
    failure = result.edition_failures[0]
    assert failure.ltd_edition_slug == "u-jsick-feature"
    assert failure.error_type == "ConnectTimeout"
    assert len(captured) == 1
    assert captured[0] is second
    assert second.__context__ is None


@pytest.mark.asyncio
async def test_sync_build_does_not_rerun_copy_after_access_denied(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-transport copy failure fails the edition on the first try.

    An LTD ``AccessDenied`` is permanent, so re-running the copy would
    only spend the retry delay on a certain second failure.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-copy-retry-denied")

    _seed_three_editions(mock_discovery)
    calls: list[tuple[str, str]] = []
    service = _build_service(
        db_session,
        http_client,
        MockObjectStore(),
        dict(_THREE_EDITION_SOURCE_OBJECTS),
        wrap_copy=_flaky_copy(
            {
                "pipelines/builds/43": [
                    LtdSourceAccessDeniedError(
                        bucket="lsst-the-docs",
                        key="pipelines/builds/43/index.html",
                        operation="GetObject",
                    )
                ]
            },
            calls,
        ),
        copy_retry_delay_seconds=7.0,
    )
    sleeps = _record_copy_retry_sleeps(monkeypatch, db_session)

    with structlog.testing.capture_logs() as logs:
        result = await service.sync_project(
            org_id=org_id, ltd_slug="pipelines"
        )

    middle_calls = [c for c in calls if c[0].startswith("pipelines/builds/43")]
    assert len(middle_calls) == 1
    assert sleeps == []
    assert _copy_retry_logs(logs) == []
    assert len(result.edition_failures) == 1
    assert result.edition_failures[0].error_type == (
        "LtdSourceAccessDeniedError"
    )


@pytest.mark.asyncio
async def test_sync_build_reruns_copy_when_transport_error_is_the_cause(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transport error chained as a failure's cause is still retried.

    The copier re-raises a failing object's exception with its chain
    intact, so a wrapper raised ``from`` a connect timeout carries the
    same "try again later" signal as the timeout itself. The warning
    names the transport error, not the wrapper.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-copy-retry-cause")

    _seed_ltd(mock_discovery)
    wrapper = RuntimeError("upload failed")
    wrapper.__cause__ = httpx.ConnectTimeout("")
    calls: list[tuple[str, str]] = []
    service = _build_service(
        db_session,
        http_client,
        MockObjectStore(),
        {"pipelines/builds/42/index.html": b"<html>v1</html>"},
        wrap_copy=_flaky_copy({"pipelines/builds/42": [wrapper]}, calls),
        copy_retry_delay_seconds=7.0,
    )
    sleeps = _record_copy_retry_sleeps(monkeypatch, db_session)

    with structlog.testing.capture_logs() as logs:
        result = await service.sync_project(
            org_id=org_id, ltd_slug="pipelines"
        )

    assert result.edition_failures == ()
    assert len(calls) == 2
    assert sleeps == [(7.0, False)]
    retries = _copy_retry_logs(logs)
    assert len(retries) == 1
    assert retries[0]["error_type"] == "ConnectTimeout"


_LTD_ENDPOINT = "https://lsst-the-docs.s3.amazonaws.com/"


class _FlakyDownloadLtdSource(_FakeLtdSource):
    """In-memory LTD source whose chosen downloads fail.

    ``outcomes`` maps a key to what its successive ``download_object``
    calls do, in order: an exception is raised, ``None`` returns the
    bytes. Once a key's queue is empty its downloads succeed. keeper-sync
    downloads every object once to hash the build's manifest before it
    copies, so a key's first entry is the hash, its second the copy's
    first pass, and its third the build-level re-run. Every call is
    appended to ``downloads``.
    """

    def __init__(
        self,
        objects: dict[str, bytes],
        *,
        outcomes: dict[str, list[BaseException | None]],
    ) -> None:
        super().__init__(objects)
        self._outcomes = outcomes
        self.downloads: list[str] = []

    async def download_object(self, *, key: str) -> bytes:
        self.downloads.append(key)
        queued = self._outcomes.get(key)
        if queued:
            outcome = queued.pop(0)
            if outcome is not None:
                raise outcome
        return await super().download_object(key=key)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        ReadTimeoutError(endpoint_url=_LTD_ENDPOINT),
        EndpointConnectionError(endpoint_url=_LTD_ENDPOINT),
        ConnectionClosedError(endpoint_url=_LTD_ENDPOINT),
    ],
    ids=lambda error: type(error).__name__,
)
async def test_sync_build_reruns_copy_after_ltd_download_transport_failure(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
) -> None:
    """A copy that loses the LTD bucket mid-download is re-run, and lands.

    Every copy reads LTD through aiobotocore as well as writing R2
    through httpx, and a re-run is as safe on one end as the other. So a
    botocore transport error from the LTD download earns the same one
    re-run, after the same wait, as an R2 connect timeout does.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-copy-retry-ltd")

    _seed_ltd(mock_discovery)
    key = "pipelines/builds/42/index.html"
    source = _FlakyDownloadLtdSource(
        {key: b"<html>v1</html>"}, outcomes={key: [None, error]}
    )
    calls: list[tuple[str, str]] = []
    service = _build_service(
        db_session,
        http_client,
        MockObjectStore(),
        {},
        source=source,
        wrap_copy=_flaky_copy({}, calls),
        copy_retry_delay_seconds=7.0,
    )
    sleeps = _record_copy_retry_sleeps(monkeypatch, db_session)

    with structlog.testing.capture_logs() as logs:
        result = await service.sync_project(
            org_id=org_id, ltd_slug="pipelines"
        )

    assert result.edition_failures == ()
    outcome = result.edition_outcomes[0].build_outcome
    assert outcome is not None
    assert outcome.short_circuited is False
    assert outcome.object_count == 1
    # Hash, failed first pass, re-run.
    assert source.downloads == [key, key, key]
    assert len(calls) == 2
    assert calls[0] == calls[1]
    assert sleeps == [(7.0, False)]
    retries = _copy_retry_logs(logs)
    assert len(retries) == 1
    assert retries[0]["log_level"] == "warning"
    assert retries[0]["error_type"] == type(error).__name__
    assert retries[0]["error"] == repr(error)
    assert retries[0]["retry_delay"] == 7.0


def _slow_down() -> ClientError:
    """Build the error botocore raises for an S3 ``503 SlowDown``."""
    return ClientError(
        {
            "Error": {"Code": "SlowDown", "Message": "Please reduce"},
            "ResponseMetadata": {"HTTPStatusCode": 503},
        },
        "GetObject",
    )


@pytest.mark.asyncio
async def test_sync_build_does_not_rerun_copy_after_ltd_client_error(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """S3 throttling the LTD download fails the edition on the first try.

    A botocore ``ClientError`` is S3 answering, not S3 unreachable. Like
    an R2 upload that kept getting ``429``/``5xx``, it is left to the
    next sync rather than re-run, even though ``SlowDown`` is transient.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-copy-retry-slowdown")

    _seed_three_editions(mock_discovery)
    key = "pipelines/builds/43/index.html"
    throttled = _slow_down()
    source = _FlakyDownloadLtdSource(
        dict(_THREE_EDITION_SOURCE_OBJECTS), outcomes={key: [None, throttled]}
    )
    calls: list[tuple[str, str]] = []
    service = _build_service(
        db_session,
        http_client,
        MockObjectStore(),
        {},
        source=source,
        wrap_copy=_flaky_copy({}, calls),
        copy_retry_delay_seconds=7.0,
    )
    sleeps = _record_copy_retry_sleeps(monkeypatch, db_session)
    captured: list[BaseException] = []
    monkeypatch.setattr(sentry_sdk, "capture_exception", captured.append)

    with structlog.testing.capture_logs() as logs:
        result = await service.sync_project(
            org_id=org_id, ltd_slug="pipelines"
        )

    middle_calls = [c for c in calls if c[0].startswith("pipelines/builds/43")]
    assert len(middle_calls) == 1
    assert source.downloads.count(key) == 2
    assert sleeps == []
    assert _copy_retry_logs(logs) == []
    assert [o.docverse_slug for o in result.edition_outcomes] == [
        "__main",
        "u-jsick-other",
    ]
    assert len(result.edition_failures) == 1
    assert result.edition_failures[0].error_type == "ClientError"
    assert captured == [throttled]


def test_retryable_transport_error_recognizes_a_botocore_transport_error() -> (
    None
):
    """An LTD download that timed out is as retryable as an R2 upload.

    Every copy reads the LTD bucket through aiobotocore as well as
    writing R2 through httpx, so a botocore transport error is the
    source-side leaf the build-level retry has to recognize.
    """
    error = ReadTimeoutError(endpoint_url=_LTD_ENDPOINT)

    assert _retryable_transport_error(error) is error


def test_retryable_transport_error_follows_a_botocore_cause() -> None:
    """A wrapper raised ``from`` a botocore transport error is retryable."""
    cause = EndpointConnectionError(endpoint_url=_LTD_ENDPOINT)
    wrapper = RuntimeError("download failed")
    wrapper.__cause__ = cause

    assert _retryable_transport_error(wrapper) is cause


def test_retryable_transport_error_ignores_a_botocore_context() -> None:
    """A botocore error reached only through ``__context__`` is not retried.

    An exception raised while *handling* a dropped LTD connection says
    nothing about whether a re-run can succeed, so the implicit context
    link is not followed for botocore errors any more than for httpx.
    """
    handled = ConnectionClosedError(endpoint_url=_LTD_ENDPOINT)
    raised = RuntimeError("bug in the error handler")
    raised.__context__ = handled

    assert raised.__cause__ is None
    assert _retryable_transport_error(raised) is None


def test_retryable_transport_error_ignores_a_botocore_client_error() -> None:
    """S3 answering with an error, even a throttle, is not transport."""
    assert _retryable_transport_error(_slow_down()) is None


def _record_copy_reports() -> tuple[
    list[BuildCopyReport], BuildCopiedCallback
]:
    """Return a list and an ``on_build_copied`` hook that appends to it."""
    reports: list[BuildCopyReport] = []

    async def _record(report: BuildCopyReport) -> None:
        reports.append(report)

    return reports, _record


def _forbidden() -> httpx.HTTPStatusError:
    """Build the error ``raise_for_status`` gives for an R2 ``403``."""
    request = httpx.Request("PUT", "https://r2.example/index.html")
    return httpx.HTTPStatusError(
        "403 Forbidden",
        request=request,
        response=httpx.Response(403, request=request),
    )


@pytest.mark.asyncio
async def test_sync_build_reports_the_copy_and_its_retried_objects(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A copied build is reported once, with the uploads that needed retries.

    The report is what the keeper-sync worker publishes as a
    ``BuildContentCopiedEvent``; an upload that landed on its second
    attempt shows in it even though the copy itself never failed.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-copy-report")

    _seed_ltd(mock_discovery)
    reports, on_build_copied = _record_copy_reports()
    body = b"<html>v1</html>"
    service = _build_service(
        db_session,
        http_client,
        ScriptedUploadStore({"index.html": [2]}),
        {"pipelines/builds/42/index.html": body},
        on_build_copied=on_build_copied,
    )

    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert len(reports) == 1
    report = reports[0]
    assert report.project_slug == "pipelines"
    assert report.object_count == 1
    assert report.total_size_bytes == len(body)
    assert report.peak_concurrent_copies == 1
    assert report.retried_object_count == 1
    assert report.exhausted_object_count == 0
    assert report.build_retry_used is False
    assert report.succeeded is True
    assert report.duration_seconds >= 0


@pytest.mark.asyncio
async def test_sync_build_reports_a_rerun_copy_as_one_copy(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A copy the build-level retry recovered is still one report.

    It says the retry was used and keeps the first pass's exhausted
    upload — that object is the outage the retry rode out.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-copy-report-rerun")

    _seed_ltd(mock_discovery)
    reports, on_build_copied = _record_copy_reports()
    service = _build_service(
        db_session,
        http_client,
        ScriptedUploadStore({"index.html": [httpx.ConnectTimeout("")]}),
        {"pipelines/builds/42/index.html": b"<html>v1</html>"},
        copy_retry_delay_seconds=7.0,
        on_build_copied=on_build_copied,
    )
    _record_copy_retry_sleeps(monkeypatch, db_session)

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert result.edition_failures == ()
    assert len(reports) == 1
    report = reports[0]
    assert report.build_retry_used is True
    assert report.succeeded is True
    assert report.exhausted_object_count == 1
    assert report.object_count == 1


@pytest.mark.asyncio
async def test_sync_build_reports_a_copy_that_failed_both_passes(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A copy that outlasts the build-level retry is reported as failed.

    Each pass's exhausted upload is counted, and the report goes out
    before the failure reaches ``sync_edition``'s accounting — here the
    project's only edition, so the sync then fails as a whole.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-copy-report-failed")

    _seed_ltd(mock_discovery)
    reports, on_build_copied = _record_copy_reports()
    service = _build_service(
        db_session,
        http_client,
        ScriptedUploadStore(
            {
                "index.html": [
                    httpx.ConnectTimeout("first"),
                    httpx.ConnectTimeout("second"),
                ]
            }
        ),
        {"pipelines/builds/42/index.html": b"<html>v1</html>"},
        copy_retry_delay_seconds=7.0,
        on_build_copied=on_build_copied,
    )
    _record_copy_retry_sleeps(monkeypatch, db_session)
    monkeypatch.setattr(sentry_sdk, "capture_exception", lambda _exc: None)

    with pytest.raises(KeeperSyncSystemicFailureError):
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert len(reports) == 1
    report = reports[0]
    assert report.succeeded is False
    assert report.build_retry_used is True
    assert report.exhausted_object_count == 2
    assert report.object_count == 0


@pytest.mark.asyncio
async def test_sync_build_reports_a_copy_that_failed_without_a_rerun(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-transport copy failure is reported once, with no retry used.

    A ``403`` from R2 ends the copy on its first attempt, so nothing ran
    out of budget and the build-level retry never fired.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-copy-report-403")

    _seed_ltd(mock_discovery)
    reports, on_build_copied = _record_copy_reports()
    service = _build_service(
        db_session,
        http_client,
        ScriptedUploadStore({"index.html": [_forbidden()]}),
        {"pipelines/builds/42/index.html": b"<html>v1</html>"},
        on_build_copied=on_build_copied,
    )
    sleeps = _record_copy_retry_sleeps(monkeypatch, db_session)
    monkeypatch.setattr(sentry_sdk, "capture_exception", lambda _exc: None)

    with pytest.raises(KeeperSyncSystemicFailureError):
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    assert sleeps == []
    assert len(reports) == 1
    report = reports[0]
    assert report.succeeded is False
    assert report.build_retry_used is False
    assert report.exhausted_object_count == 0


@pytest.mark.asyncio
async def test_sync_build_isolates_a_raising_copy_report_hook(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A metrics hook that raises cannot fail the build it reports on.

    The report is a side channel: the copy already landed, so the hook's
    error goes to Sentry and the log, and the sync carries on.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-copy-report-raises")

    _seed_ltd(mock_discovery)
    hook_error = RuntimeError("metrics are down")

    async def _raising_hook(report: BuildCopyReport) -> None:
        raise hook_error

    service = _build_service(
        db_session,
        http_client,
        MockObjectStore(),
        {"pipelines/builds/42/index.html": b"<html>v1</html>"},
        on_build_copied=_raising_hook,
    )
    captured: list[BaseException] = []
    monkeypatch.setattr(sentry_sdk, "capture_exception", captured.append)

    with structlog.testing.capture_logs() as logs:
        result = await service.sync_project(
            org_id=org_id, ltd_slug="pipelines"
        )

    assert result.edition_failures == ()
    outcome = result.edition_outcomes[0].build_outcome
    assert outcome is not None
    assert outcome.short_circuited is False
    assert captured == [hook_error]
    assert any(
        entry["event"] == "on_build_copied callback raised; continuing"
        for entry in logs
    )


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
    ("ltd_mode", "git_ref", "expected_kind", "expected_tracking_mode"),
    [
        ("lsst_doc", "v1.2", EditionKind.release, TrackingMode.lsst_doc),
        (
            "lsst_doc",
            "tickets/DM-1",
            EditionKind.draft,
            TrackingMode.lsst_doc,
        ),
        (
            "eups_daily_release",
            "d_2026_08_10",
            EditionKind.draft,
            TrackingMode.eups_daily_release,
        ),
    ],
    ids=[
        "lsst-doc-version-release",
        "lsst-doc-branch-draft",
        "eups-daily-draft",
    ],
)
async def test_sync_imports_version_mode_editions_by_mode(
    *,
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    ltd_mode: str,
    git_ref: str,
    expected_kind: EditionKind,
    expected_tracking_mode: TrackingMode,
) -> None:
    """The LTD mode narrows the grammar; the tracked ref confirms it.

    Docverse's tracking mode still comes straight from LTD's, but the
    *kind* does not: a ``lsst_doc`` edition on ``v1.2`` is a release and
    the same mode on a ticket branch is a draft. ``eups_daily_release``
    is the one mode that decides alone, and it decides ``draft`` so
    dailies keep ageing out.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug=f"ks-mode-{ltd_mode.replace('_', '-')}-{expected_kind.value}",
        )

    _seed_two_editions_main_and_draft(
        mock_discovery,
        draft_payload=_version_edition_payload(
            slug="current", git_ref=git_ref, mode=ltd_mode
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
    left behind. ``create_internal`` is the seeding path precisely
    because it is the one keeper-sync itself uses, so the row lands with
    ``kind_source == derived`` — the system's to reconverge. Returns the
    edition id so the caller can assert on the same row regardless of
    slug.
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
        edition = await edition_store.create_internal(
            project_id=project.id,
            slug=edition_slug,
            title=edition_slug,
            kind=kind,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": git_ref},
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
async def test_resync_heals_a_wrongly_imported_release_back_to_draft(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A ``release`` the old mode table produced heals back to ``draft``.

    Before the ref confirmation, every ``lsst_doc`` / ``eups_*`` edition
    imported as ``release`` whatever it tracked, so branch-tracking rows
    landed with a long CDN cache profile and permanent lifecycle
    exemption. Those rows are ``derived``, so the next poll converges
    them downhill rather than stranding them.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-heal-down")

    edition_id = await _seed_project_with_edition(
        db_session,
        org_id=org_id,
        edition_slug="u-jsick-feature",
        kind=EditionKind.release,
        git_ref="u/jsick/feature",
    )

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug="u-jsick-feature", git_ref="u/jsick/feature"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>healed</html>",
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
    assert edition.kind == EditionKind.draft
    assert edition.kind_source == EditionKindSource.derived


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("existing_kind", "edition_slug", "git_ref"),
    [
        (EditionKind.major, "15", "15.2.1"),
        (EditionKind.minor, "15.2", "15.2.1"),
        (EditionKind.alternate, "15.2.1", "15.2.1"),
    ],
    ids=[
        "major-untouched",
        "minor-untouched",
        "alternate-untouched",
    ],
)
async def test_resync_kind_refresh_leaves_foreign_derivations_alone(
    *,
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    existing_kind: EditionKind,
    edition_slug: str,
    git_ref: str,
) -> None:
    """Aggregate and alternate kinds belong to another derivation.

    ``derive_edition_kind`` only ever answers ``main`` / ``release`` /
    ``draft``; ``major`` / ``minor`` come from the semver aggregate
    specs and ``alternate`` from the alternate branch of
    ``derive_edition_slug``. These cases derive ``release`` from a
    version ref, and must leave the rollup or variant exactly as it is.
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
async def test_resync_kind_convergence_is_locked_and_logged(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The kind write holds ``EDITION_UPDATE`` and leaves an audit line.

    ``publish_edition`` reads ``edition.kind`` to pick the CDN cache
    profile, so a job that loaded the edition before an unlocked
    promotion committed would write the KV pointer with the stale
    ``short`` profile and skip the hostname purge — leaving the edge
    serving ``max-age=60`` for a release.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-heal-lock")

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

    trace: list[str] = []
    lock_service = RecordingLockService(
        session=db_session,
        logger=structlog.get_logger("test"),
        events=[],
        trace=trace,
    )
    original_update_kind = EditionStore.update_kind

    async def _traced_update_kind(
        self: EditionStore, *, edition_id: int, kind: EditionKind
    ) -> None:
        trace.append(f"update_kind:{edition_id}")
        await original_update_kind(self, edition_id=edition_id, kind=kind)

    monkeypatch.setattr(EditionStore, "update_kind", _traced_update_kind)

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>release</html>",
    }
    service = _build_service(
        db_session,
        http_client,
        object_store,
        source_objects,
        lock_service=lock_service,
    )

    with structlog.testing.capture_logs() as logs:
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    monkeypatch.undo()
    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        edition = await edition_store.get_by_id(edition_id)
    assert edition is not None
    assert edition.kind == EditionKind.release

    lock_key = LockKey.for_edition_update(
        org_id=org_id, project_id=edition.project_id, edition_id=edition_id
    )
    write = trace.index(f"update_kind:{edition_id}")
    assert any(
        entry == f"enter:{lock_key.lock_id}" for entry in trace[:write]
    ), "kind write ran with no EDITION_UPDATE lock held"
    assert any(
        entry == f"exit:{lock_key.lock_id}" for entry in trace[write + 1 :]
    ), "EDITION_UPDATE lock released before the kind write"

    events = [e for e in logs if e["event"] == "Converged edition kind"]
    assert len(events) == 1
    assert events[0]["edition_id"] == edition_id
    assert events[0]["previous_kind"] == "draft"
    assert events[0]["edition_kind"] == "release"
    assert events[0]["kind_trigger"] == "keeper_sync"


@pytest.mark.asyncio
async def test_resync_respects_manual_demotion_to_draft(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An operator's ``release`` -> ``draft`` PATCH survives every poll.

    Convergence alone would undo the operator within minutes: the ref is
    still a version tag, so the derivation still says ``release``. The
    PATCH here goes through ``EditionStore.update``, the real
    editions-API write path, so ``kind_source`` is stamped ``declared``
    the way production stamps it — and a declared row is never
    converged.
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
async def test_sync_advances_existing_aggregate_when_autocreation_off(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """``semver_aggregates=false`` stops creation, never advancement.

    Native parity: ``find_matching_editions`` carries no autocreation
    gate, so a natively built project's existing ``15`` keeps moving with
    every release after the knob is turned off — only the implicit
    creation of new ``N`` / ``N.M`` rows stops. A migrated project's
    ``/v/15`` pointer must not freeze where an identically configured
    native one moves.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-agg-advance-off")

    project_id = await _seed_project(
        db_session,
        org_id=org_id,
        edition_autocreation={"semver_aggregates": False},
    )
    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    # The ``15`` aggregate predates the opt-out (auto-created while the
    # knob was still on, or made by hand); ``15.2`` deliberately does not
    # exist, so the same sync exercises both halves of the gate.
    async with db_session.begin():
        await edition_store.create_internal(
            project_id=project_id,
            slug="15",
            title="Latest 15.x",
            kind=EditionKind.major,
            tracking_mode=TrackingMode.semver_major,
            tracking_params={"major_version": 15},
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

    async with db_session.begin():
        editions = await edition_store.list_all_by_project(project_id)
    by_slug = {e.slug: e for e in editions}
    assert set(by_slug) == {"15.2.1", "15"}
    assert by_slug["15.2.1"].current_build_id is not None
    assert by_slug["15"].current_build_id == by_slug["15.2.1"].current_build_id


@pytest.mark.asyncio
async def test_enabling_aggregates_backfills_on_the_next_sync(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Re-enabling the knob heals the aggregates it had suppressed.

    A poll that ran with ``semver_aggregates=false`` reconciled nothing,
    so it must leave no convergence marker behind — the marker claims
    this build's ``N`` / ``N.M`` rows are already as they should be, and
    with creation disabled they are exactly not. Stamping it anyway
    pinned the skip in ``sync_edition`` to the same build id forever, so
    turning the knob back on only took effect if LTD happened to publish
    a new build for the edition.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-agg-reenable")

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

    # The operator PATCHes the knob back on. LTD publishes nothing new,
    # so the next poll's ``sync_build`` short-circuits — the marker is
    # the only thing standing between the release and its aggregates.
    async with db_session.begin():
        await db_session.execute(
            update(SqlProject)
            .where(SqlProject.id == project_id)
            .values(edition_autocreation={"semver_aggregates": True})
        )

    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    outcome = result.edition_outcomes[0]
    assert outcome.build_outcome is not None
    assert outcome.build_outcome.short_circuited is True
    async with db_session.begin():
        editions = await edition_store.list_all_by_project(project_id)
    by_slug = {e.slug: e for e in editions}
    assert set(by_slug) == {"15.2.1", "15", "15.2"}
    release_build_id = by_slug["15.2.1"].current_build_id
    assert release_build_id is not None
    assert by_slug["15"].current_build_id == release_build_id
    assert by_slug["15.2"].current_build_id == release_build_id


@pytest.mark.asyncio
async def test_sync_aggregates_survive_slug_only_user_rule(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A slug-shaping user rule does not disable the backfill.

    The gate is the ref's semver grammar, not the derived kind, so an
    org whose ``prefix_strip`` rule shadows the built-in ``SemverRule``
    (leaving the imported edition a ``draft``) still gets its ``15`` /
    ``15.2`` rows — the same call the native upload path makes for the
    same ref. ``edition_autocreation.semver_aggregates`` remains the
    way to turn the backfill off.
    """
    async with db_session.begin():
        org_id = await _seed_org(
            db_session,
            slug="ks-agg-kind-rule",
            slug_rewrite_rules=[{"type": "prefix_strip", "prefix": "v"}],
        )

    project_id = await _seed_project(db_session, org_id=org_id)

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug="15.2.1", git_ref="v15.2.1"
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
    by_slug = {e.slug: e for e in editions}
    assert set(by_slug) == {"15.2.1", "15", "15.2"}
    assert by_slug["15.2.1"].kind == EditionKind.draft
    assert by_slug["15"].kind == EditionKind.major
    assert by_slug["15.2"].kind == EditionKind.minor


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


def _record_backfill_calls(
    service: KeeperSyncService,
    monkeypatch: pytest.MonkeyPatch,
) -> list[int]:
    """Replace the aggregate backfill with a recorder of its build ids.

    Asserting the returned list is empty pins that a poll did no
    aggregate work *at all* — not one transaction and not one per-spec
    ``get_by_slug`` — which is the property a migrated project with ~80
    release editions needs on every 5-minute tick.
    """
    calls: list[int] = []

    async def _recording_backfill(**kwargs: object) -> tuple[object, ...]:
        calls.append(int(kwargs["build_id"]))  # type: ignore[call-overload]
        return ()

    monkeypatch.setattr(
        service, "_backfill_semver_aggregates", _recording_backfill
    )
    return calls


@pytest.mark.asyncio
async def test_resync_skips_aggregate_backfill_when_nothing_changed(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A short-circuited re-sync of a healed edition does no backfill.

    ``sync_build`` hands back the previously-synced build id even when it
    short-circuits, so keying the backfill off "we have a build id" made
    every tick re-open a transaction and re-SELECT both aggregate slugs
    per release edition, only to conclude nothing moved.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-agg-steady")

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
    assert {e.slug for e in editions} == {"15.2.1", "15", "15.2"}

    # Second poll: LTD state is unchanged, so nothing needs backfilling.
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    calls = _record_backfill_calls(service, monkeypatch)
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    outcome = result.edition_outcomes[0]
    assert outcome.build_outcome is not None
    assert outcome.build_outcome.short_circuited is True
    assert outcome.build_outcome.docverse_build_id is not None
    assert calls == []


@pytest.mark.asyncio
async def test_short_circuited_sync_heals_aggregates_exactly_once(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Editions imported before the backfill existed still heal.

    The one-time heal lands on a *short-circuited* sync — the content was
    imported by an older Docverse, so ``date_rebuilt`` never changes
    again — which is why the skip cannot simply key off
    ``short_circuited``. The first sync here stands in for that older
    Docverse by stubbing the backfill out entirely; the second must
    create the aggregates anyway, and the third must not go looking.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-agg-heal")

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

    legacy = _build_service(
        db_session, http_client, object_store, source_objects
    )
    _record_backfill_calls(legacy, monkeypatch)
    await legacy.sync_project(org_id=org_id, ltd_slug="pipelines")

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        editions = await edition_store.list_all_by_project(project_id)
    assert {e.slug for e in editions} == {"15.2.1"}

    # Today's code, first poll after deploy: the build short-circuits but
    # the aggregates are still missing, so the heal runs.
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    outcome = result.edition_outcomes[0]
    assert outcome.build_outcome is not None
    assert outcome.build_outcome.short_circuited is True
    async with db_session.begin():
        editions = await edition_store.list_all_by_project(project_id)
    by_slug = {e.slug: e for e in editions}
    assert set(by_slug) == {"15.2.1", "15", "15.2"}
    release_build_id = by_slug["15.2.1"].current_build_id
    assert release_build_id is not None
    assert by_slug["15"].current_build_id == release_build_id
    assert by_slug["15.2"].current_build_id == release_build_id
    # The healed aggregates are reported, so the worker publishes them.
    assert {o.docverse_slug for o in outcome.aggregate_outcomes} == {
        "15",
        "15.2",
    }

    # Third poll: the heal is recorded, so it does not run again.
    service = _build_service(
        db_session, http_client, object_store, source_objects
    )
    calls = _record_backfill_calls(service, monkeypatch)
    await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    assert calls == []


# ---------------------------------------------------------------------------
# Aggregate repointing under the EDITION_UPDATE advisory lock (#524). A
# project mid-migration publishes natively while keeper-sync still polls, so
# the aggregate's version-guard read and its pointer write have to be atomic
# against ``build_processing`` / ``publish_edition``, which take the same
# lock.
# ---------------------------------------------------------------------------


class _CompetingWriterLockService(LockService):
    """Lock service that lands a competing commit at acquire time.

    Stands in for the native ``build_processing`` path having won the
    race for an aggregate's ``EDITION_UPDATE`` lock: by the time
    keeper-sync gets inside the lock, the aggregate already points at a
    newer release. ``acquire`` deliberately does *not* delegate to the
    real :class:`LockService` — the double exists to control what is
    committed while the lock is notionally held, not to exercise
    Postgres advisory locks (``tests/services/locks_integration_test.py``
    covers those).
    """

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
        on_acquire: Callable[[LockKey], Awaitable[None]],
    ) -> None:
        super().__init__(session=session, logger=logger)
        self._on_acquire = on_acquire
        self.acquired: list[LockKey] = []

    @asynccontextmanager
    async def acquire(self, lock_key: LockKey) -> AsyncGenerator[None]:
        self.acquired.append(lock_key)
        await self._on_acquire(lock_key)
        yield


async def _seed_native_release_build(
    session: AsyncSession, *, project_id: int, git_ref: str
) -> int:
    """Create a completed build on *git_ref*, as a native upload would."""
    logger = structlog.get_logger("test")
    build_store = BuildStore(session=session, logger=logger)
    async with session.begin():
        build = await build_store.create(
            project_id=project_id,
            project_slug="pipelines",
            data=BuildCreate(
                git_ref=git_ref,
                content_hash="sha256:" + "b" * 64,
            ),
            uploader="native",
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.completed
        )
    return build.id


@pytest.mark.asyncio
async def test_sync_aggregate_rereads_pointer_under_edition_lock(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A native repoint landing before the lock is granted is not undone.

    Reproduces the mid-migration regression: keeper-sync reads ``15``,
    the native path publishes ``15.9.9`` onto it, and keeper-sync then
    writes its older ``15.0.0`` build over the top — dropping ``/v/15``
    back a release until the next one ships. The guard read has to
    happen *inside* the ``EDITION_UPDATE`` lock, and has to see the
    other writer's committed row rather than the copy this session
    already loaded.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-agg-lock")

    project_id = await _seed_project(db_session, org_id=org_id)
    native_build_id = await _seed_native_release_build(
        db_session, project_id=project_id, git_ref="15.9.9"
    )

    repointed: list[int] = []

    async def _repoint_major_aggregate(lock_key: LockKey) -> None:
        """Commit the native pointer move, once, from another session.

        Fires on the first acquisition that finds ``15`` on disk — the
        lock keeper-sync takes to decide that aggregate's pointer — and
        then disarms, so a later acquisition cannot paper over a
        keeper-sync write that should never have happened.
        """
        if repointed:
            return
        async for other in db_session_dependency():
            store = EditionStore(
                session=other, logger=structlog.get_logger("test")
            )
            async with other.begin():
                major = await store.get_by_slug(
                    project_id=project_id, slug="15"
                )
                if major is None:
                    continue
                await store.set_current_build(
                    edition_id=major.id,
                    build_id=native_build_id,
                    skip_date_guard=True,
                )
                repointed.append(major.id)

    _seed_ltd_one_edition(
        mock_discovery,
        edition_payload=_version_edition_payload(
            slug="15.0.0", git_ref="15.0.0"
        ),
    )

    object_store = MockObjectStore()
    source_objects = {
        "pipelines/builds/43/index.html": b"<html>release</html>",
    }
    lock_service = _CompetingWriterLockService(
        db_session, structlog.get_logger("test"), _repoint_major_aggregate
    )
    service = _build_service(
        db_session,
        http_client,
        object_store,
        source_objects,
        lock_service=lock_service,
    )

    await service.sync_project(org_id=org_id, ltd_slug="pipelines")

    edition_store = EditionStore(
        session=db_session, logger=structlog.get_logger("test")
    )
    async with db_session.begin():
        release = await edition_store.get_by_slug(
            project_id=project_id, slug="15.0.0"
        )
        major = await edition_store.get_by_slug(
            project_id=project_id, slug="15"
        )
        minor = await edition_store.get_by_slug(
            project_id=project_id, slug="15.0"
        )
    assert release is not None
    assert release.current_build_id is not None
    assert major is not None
    assert repointed == [major.id]
    # The native ``15.9.9`` pointer survives: the version guard ran on
    # the row as it stood inside the lock, not on the pre-lock read.
    assert major.current_build_id == native_build_id
    # The minor aggregate has no competing writer, so it still advances.
    assert minor is not None
    assert minor.current_build_id == release.current_build_id
    # The lock keyed on the aggregate really was taken.
    assert (
        LockKey.for_edition_update(
            org_id=org_id, project_id=project_id, edition_id=major.id
        )
        in lock_service.acquired
    )


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

    # Fail per-aggregate rather than stubbing the whole backfill, so the
    # real semver guards still run: ``__main`` is not a release and never
    # reaches this, and only the ``15.2.1`` release edition's backfill
    # blows up — the shape of the transient DB error this boundary
    # exists to absorb.
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
    prefix's content. ``main`` is this product's only edition, so the
    run imports nothing — but a denial is a permanent per-edition fault,
    not an outage, so it stays on the partial-success path instead of
    failing the job the tier cron would then replay identically forever.
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


_REPUBLISHED_EDITION_PREFIX = "pipelines/v/0.3.0/"


def _seed_republish_race(
    mock_discovery: respx.Router,
    *,
    no_date_rebuilt: bool = False,
) -> tuple[dict[str, bytes], _FakeLtdSource, Callable[[str], None]]:
    """Set up a denied build whose ``v/`` prefix republishes mid-sync.

    Returns the mutable source objects, the LTD source over them, and
    the ``after_manifest_hash`` hook that overwrites the edition prefix
    once — reproducing an LTD republish that lands between the hash
    keeper-sync resolved on and the copy that reads the bytes.

    ``no_date_rebuilt`` nulls the LTD edition's rebuild timestamp,
    reproducing LTD's oldest editions — the same population whose
    un-ACL'd build uploads force the ``v/`` fallback in the first place.
    """
    draft_payload = _version_edition_payload(slug="0.3.0", git_ref="0.3.0")
    if no_date_rebuilt:
        draft_payload["date_rebuilt"] = None
    _seed_two_editions_main_and_draft(
        mock_discovery, draft_payload=draft_payload
    )
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>main</html>",
        # The un-ACL'd build upload that forces the ``v/`` fallback.
        "pipelines/builds/43/index.html": b"<html>denied</html>",
        f"{_REPUBLISHED_EDITION_PREFIX}index.html": b"<html>release v1</html>",
    }
    source = _FakeLtdSource(
        source_objects, denied_prefixes=frozenset({"pipelines/builds/43/"})
    )
    republished = False

    def republish(prefix: str) -> None:
        nonlocal republished
        if prefix != _REPUBLISHED_EDITION_PREFIX or republished:
            return
        republished = True
        source_objects[f"{_REPUBLISHED_EDITION_PREFIX}index.html"] = (
            b"<html>release v2</html>"
        )

    return source_objects, source, republish


@pytest.mark.asyncio
async def test_republish_between_hash_and_copy_clears_the_rebuilt_marker(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A mutated source prefix must not leave a converged-looking state.

    ``<product>/v/<slug>/`` is a live prefix LTD rewrites on every
    republish, so the hash keeper-sync resolves on and the bytes it
    later copies can come from two different publishes. When they do,
    the recorded hash must describe the *copied* bytes and the build's
    ``date_rebuilt_seen`` marker must not be written — writing it would
    claim LTD's current rebuild had been imported when what landed is a
    prefix state that no longer exists.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-republish-race")

    source_objects, source, republish = _seed_republish_race(mock_discovery)
    resolved_hash = await _manifest_hash_of(
        source, prefix=_REPUBLISHED_EDITION_PREFIX
    )

    object_store = MockObjectStore()
    service = _build_service(
        db_session,
        http_client,
        object_store,
        source_objects,
        source=source,
        after_manifest_hash=republish,
    )

    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    assert result.edition_failures == ()

    copied_hash = await _manifest_hash_of(
        source, prefix=_REPUBLISHED_EDITION_PREFIX
    )
    assert copied_hash != resolved_hash

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
        # The bytes that landed are the republished ones, and that is
        # the hash both the build row and the state row carry.
        assert build.content_hash == copied_hash

        state = await state_store.get(
            org_id=org_id, resource_type=ResourceType.build, ltd_id=43
        )
        assert state is not None
        assert state.content_hash == copied_hash
        # No marker: the next poll re-resolves instead of concluding
        # LTD's current rebuild is already imported. The retraction is
        # recorded explicitly rather than left to the cleared column,
        # which an edition with no ``date_rebuilt`` cannot express.
        assert state.date_rebuilt_seen is None
        assert state.annotations is not None
        assert state.annotations["date_rebuilt_seen_retracted"] is True
        assert state.annotations["ltd_source_manifest_hash"] == resolved_hash


@pytest.mark.asyncio
async def test_republished_prefix_reconverges_on_the_next_sync(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """The retracted marker is what makes the following poll finish.

    Convergence, not just detection: once the prefix settles, the next
    sync re-resolves the edition (rather than short-circuiting on the
    marker), finds the hash it now reads already carried by the build
    the mutated run imported, and dedupes onto it — recording the
    rebuild marker without copying a single object again.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-republish-converge")

    source_objects, source, republish = _seed_republish_race(mock_discovery)
    object_store = MockObjectStore()
    service = _build_service(
        db_session,
        http_client,
        object_store,
        source_objects,
        source=source,
        after_manifest_hash=republish,
    )
    assert (
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    ).edition_failures == ()

    logger = structlog.get_logger("test")
    project_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
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
    build_id_after_first = release.current_build_id
    assert build_id_after_first is not None
    objects_after_first = set(object_store.objects)
    prefixes_after_first = len(source.listed_prefixes)

    # Second poll, with the prefix now settled on the republished bytes.
    service = _build_service(
        db_session, http_client, object_store, source_objects, source=source
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    assert result.edition_failures == ()

    # It re-read the edition prefix instead of short-circuiting.
    assert (
        _REPUBLISHED_EDITION_PREFIX
        in source.listed_prefixes[prefixes_after_first:]
    )
    # ...and dedupe carried it: no object was copied a second time.
    assert set(object_store.objects) == objects_after_first

    async with db_session.begin():
        release = await edition_store.get_by_slug(
            project_id=project.id, slug="0.3.0"
        )
        assert release is not None
        assert release.current_build_id == build_id_after_first

        state = await state_store.get(
            org_id=org_id, resource_type=ResourceType.build, ltd_id=43
        )
        assert state is not None
        # The marker is back: this LTD rebuild really is imported now.
        assert state.date_rebuilt_seen is not None
        # And the forensic annotation is gone with the mutation it
        # described — annotations replace wholesale on a clean resolve.
        assert state.annotations is not None
        assert "ltd_source_manifest_hash" not in state.annotations


@pytest.mark.asyncio
async def test_republished_prefix_without_a_rebuilt_date_reconverges(
    db_session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A retracted marker outranks LTD reporting no rebuild timestamp.

    ``LtdEdition.date_rebuilt`` is nullable, and the editions that
    report it null are the oldest uploads — precisely the ones whose
    un-ACL'd build prefixes send keeper-sync down the mutable ``v/``
    fallback. Comparing the retracted marker to LTD's timestamp on
    equality alone made ``None == None`` read as "already converged", so
    the poll after a mid-import republish short-circuited and the
    edition kept serving a prefix state that no longer exists. The
    retraction has to be distinguishable from a marker that matches.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-republish-nodate")

    source_objects, source, republish = _seed_republish_race(
        mock_discovery, no_date_rebuilt=True
    )
    object_store = MockObjectStore()
    service = _build_service(
        db_session,
        http_client,
        object_store,
        source_objects,
        source=source,
        after_manifest_hash=republish,
    )
    assert (
        await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    ).edition_failures == ()

    logger = structlog.get_logger("test")
    project_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
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
        state = await state_store.get(
            org_id=org_id, resource_type=ResourceType.build, ltd_id=43
        )
        assert state is not None
    build_id_after_first = release.current_build_id
    assert build_id_after_first is not None
    # Both the retracted marker and LTD's timestamp are null, so the
    # short-circuit cannot tell them apart on value alone.
    assert state.date_rebuilt_seen is None
    objects_after_first = set(object_store.objects)
    prefixes_after_first = len(source.listed_prefixes)

    # Second poll, with the prefix now settled on the republished bytes.
    service = _build_service(
        db_session, http_client, object_store, source_objects, source=source
    )
    result = await service.sync_project(org_id=org_id, ltd_slug="pipelines")
    assert result.edition_failures == ()

    # It re-read the edition prefix rather than short-circuiting on the
    # retracted marker...
    assert (
        _REPUBLISHED_EDITION_PREFIX
        in source.listed_prefixes[prefixes_after_first:]
    )
    # ...and dedupe carried it onto the build the mutated run imported.
    assert set(object_store.objects) == objects_after_first

    async with db_session.begin():
        release = await edition_store.get_by_slug(
            project_id=project.id, slug="0.3.0"
        )
        assert release is not None
        assert release.current_build_id == build_id_after_first

        state = await state_store.get(
            org_id=org_id, resource_type=ResourceType.build, ltd_id=43
        )
        assert state is not None
        assert state.annotations is not None
        # The retraction is spent: LTD still reports no rebuild
        # timestamp, but this resolution held, so the next poll is free
        # to short-circuit again.
        assert "date_rebuilt_seen_retracted" not in state.annotations
        assert "ltd_source_manifest_hash" not in state.annotations


@pytest.mark.asyncio
async def test_finalize_synced_build_does_not_deadlock_with_delete(
    db_session: AsyncSession,
    db_session_factory: async_sessionmaker[AsyncSession],
    http_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A synced-build finalize and a project DELETE serialize.

    ``_finalize_synced_build`` is a composite writer: it stamps the
    placeholder build's hash, inventory and status — four locked writes
    on the ``builds`` row — and only then repoints the edition. Left to
    itself that transaction runs ``builds -> projects -> editions``,
    the reverse of the project soft-delete cascade, so a DELETE parked
    between its edition cascade and its build cascade would find the
    build row held by a finalize that was itself waiting for the
    project row the DELETE holds. PostgreSQL breaks that cycle by
    aborting one side, and ``keeper_sync_project`` runs with
    ``max_tries=1`` while ``delete_project`` simply 500s.

    Taking the project and edition rows at the head of the transaction
    turns the race into a wait, and the finalize — once it wakes —
    stands down on the build the cascade cancelled out from under it.
    """
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-deadlock")
        project_store = ProjectStore(session=db_session, logger=logger)
        edition_store = EditionStore(session=db_session, logger=logger)
        build_store = BuildStore(session=db_session, logger=logger)
        project = await project_store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="ks-deadlock-proj",
                title="Deadlock",
                source_url="https://example.com/example/repo",
            ),
        )
        edition = await edition_store.create_internal(
            project_id=project.id,
            slug=DEFAULT_EDITION_SLUG,
            title="Main",
            kind=EditionKind.main,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "main"},
        )
        build = await build_store.create(
            project_id=project.id,
            project_slug=project.slug,
            data=BuildCreate(
                git_ref="main",
                content_hash=PLACEHOLDER_CONTENT_HASH,
            ),
            uploader="keeper-sync",
        )

    # Park the DELETE between its edition cascade and its build
    # cascade: the only window in which it holds the project and
    # edition rows and still needs the build row.
    at_builds = asyncio.Event()
    release_builds = asyncio.Event()
    cascade_builds = BuildStore.soft_delete_all_by_project

    async def paused_cascade(
        self: BuildStore, *, project_id: int
    ) -> list[int]:
        at_builds.set()
        await release_builds.wait()
        return await cascade_builds(self, project_id=project_id)

    monkeypatch.setattr(
        BuildStore, "soft_delete_all_by_project", paused_cascade
    )

    copy_result = CopyResult(
        object_count=2,
        total_size_bytes=64,
        content_hash="sha256:" + "b" * 64,
    )
    finalize_error: list[BaseException | None] = []

    async with (
        db_session_factory() as delete_session,
        db_session_factory() as sync_session,
        db_session_factory() as probe,
    ):
        # Leaves the PID's transaction open on purpose: the finalize
        # below runs inside it, so the backend that blocks is the one
        # the probe is watching.
        sync_pid = await backend_pid(sync_session)

        async def run_delete() -> None:
            store = ProjectStore(session=delete_session, logger=logger)
            await store.soft_delete(
                org_id=org_id,
                slug="ks-deadlock-proj",
                reason=TombstoneReason.manual_delete,
            )
            await delete_session.commit()

        async def run_finalize() -> None:
            service = _build_service(
                sync_session, http_client, MockObjectStore(), {}
            )
            try:
                await service._finalize_synced_build(
                    build=build,
                    edition=edition,
                    copy_result=copy_result,
                    project_slug="ks-deadlock-proj",
                    org_slug="ks-deadlock",
                )
                await sync_session.commit()
            except BaseException as exc:
                finalize_error.append(exc)
            else:
                finalize_error.append(None)

        deleting = asyncio.ensure_future(run_delete())
        finalizing: asyncio.Task[None] | None = None
        parked = False
        try:
            await asyncio.wait_for(at_builds.wait(), timeout=LOCK_WAIT_TIMEOUT)
            finalizing = asyncio.ensure_future(run_finalize())
            parked = await wait_until_blocked_or_finished(
                probe, pid=sync_pid, task=finalizing
            )
            release_builds.set()
            await deleting
            await finalizing
        finally:
            release_builds.set()
            for task in (deleting, finalizing):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            await delete_session.rollback()
            await sync_session.rollback()

    # The finalize waited on the project row rather than sailing past
    # it holding the build the cascade needed next.
    assert parked
    # Neither side was aborted: the DELETE committed...
    async with db_session_factory() as reader:
        project_row = (
            await reader.execute(
                select(SqlProject).where(SqlProject.id == project.id)
            )
        ).scalar_one()
        assert project_row.date_deleted is not None
        build_row = (
            await reader.execute(
                select(SqlBuild).where(SqlBuild.id == build.id)
            )
        ).scalar_one()
        assert build_row.date_deleted is not None
        assert build_row.status == BuildStatus.cancelled
        assert build_row.content_hash == PLACEHOLDER_CONTENT_HASH
    # ...and the finalize lost the way a loser should: a deterministic
    # refusal to advance a build the DELETE had already cancelled, not
    # a deadlock abort.
    assert len(finalize_error) == 1
    assert isinstance(finalize_error[0], InvalidBuildStateError)
