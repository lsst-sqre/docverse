"""Tests for ``GET /orgs/{org}/keeper-sync/projects/{ltd_slug}``.

The org-admin-scoped read endpoint that gives operators a single-call
view of how a specific LTD project is being sync'd to Docverse: which
tier cohort it sits in for each tier, when it was last polled, when
it will next be polled (jitter-aware), and which Docverse-side
editions exist with their sync state. ``?ltd=true`` adds a live-LTD
reconciliation diff for deeper debugging.

This is the read-side companion to the one-shot refresh in #316:
operator inspects state via GET, optionally promotes via POST.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import httpx
import pytest
import respx
import structlog
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency

from docverse.client.models import OrgRole
from docverse.services.keeper_sync.scheduler import (
    ANNOTATION_DATE_MAIN_LAST_POLLED,
    TIER_MAIN_DORMANT_INTERVAL,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.keeper_sync import KeeperSyncStateStore, ResourceType
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from tests.conftest import seed_member, seed_org_with_admin

_ADMIN = "admin-user"
_ORG = "ks-status-org"
_LTD_SLUG = "pipelines"
_LTD_BASE = "https://keeper.lsst.codes"


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


async def _setup_org(client: AsyncClient) -> None:
    await seed_org_with_admin(client, _ORG, _ADMIN)


async def _enable_sync(
    client: AsyncClient,
    *,
    project_slugs: list[str] | Literal["*"] = "*",
) -> None:
    response = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={
            "enabled": True,
            "ltd_base_url": f"{_LTD_BASE}/",
            "project_slugs": project_slugs,
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200


async def _create_project(
    client: AsyncClient, *, slug: str = _LTD_SLUG
) -> int:
    response = await client.post(
        f"/docverse/orgs/{_ORG}/projects",
        json={
            "slug": slug,
            "title": slug.title(),
            "doc_repo": f"https://github.com/example/{slug}",
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201
    async for session in db_session_dependency():
        org_store = OrganizationStore(session=session, logger=_logger())
        org = await org_store.get_by_slug(_ORG)
        assert org is not None
        proj_store = ProjectStore(session=session, logger=_logger())
        project = await proj_store.get_by_slug(org_id=org.id, slug=slug)
        assert project is not None
        return project.id
    msg = "no session"
    raise AssertionError(msg)


async def _get_org_id() -> int:
    async for session in db_session_dependency():
        store = OrganizationStore(session=session, logger=_logger())
        org = await store.get_by_slug(_ORG)
        assert org is not None
        return org.id
    msg = "no session"
    raise AssertionError(msg)


async def _get_main_edition_id(project_id: int) -> int:
    """Look up the auto-created ``__main`` edition's internal id by DB."""
    async for session in db_session_dependency():
        edition_store = EditionStore(session=session, logger=_logger())
        editions = await edition_store.list_all_by_project(project_id)
        for edition in editions:
            if edition.kind.value == "main":
                return edition.id
        msg = f"no main edition for project_id={project_id}"
        raise AssertionError(msg)
    msg = "no session"
    raise AssertionError(msg)


async def _seed_state(
    *,
    org_id: int,
    resource_type: ResourceType,
    ltd_slug: str,
    ltd_id: int | None = None,
    docverse_id: int | None = None,
    date_last_synced: datetime | None = None,
    date_rebuilt_seen: datetime | None = None,
    annotations: dict[str, Any] | None = None,
) -> None:
    async for session in db_session_dependency():
        async with session.begin():
            store = KeeperSyncStateStore(session=session, logger=_logger())
            await store.upsert(
                org_id=org_id,
                resource_type=resource_type,
                ltd_id=ltd_id,
                ltd_slug=ltd_slug,
                docverse_id=docverse_id,
                date_last_synced=date_last_synced,
                date_rebuilt_seen=date_rebuilt_seen,
                annotations=annotations,
            )
            await session.commit()
        return
    msg = "no session"
    raise AssertionError(msg)


def _stub_ltd_editions(
    mock_discovery: respx.Router,
    *,
    product_slug: str,
    edition_ids: list[int],
    base_url: str = _LTD_BASE,
) -> None:
    """Stub the LTD edition listing + each edition's body.

    ``list_editions_for_product`` walks both the listing endpoint and
    each edition URL, so we need both stubs in place for the
    ``?ltd=true`` path.
    """
    urls = [f"{base_url}/editions/{i}" for i in edition_ids]
    mock_discovery.get(f"{base_url}/products/{product_slug}/editions/").mock(
        return_value=httpx.Response(200, json={"editions": urls})
    )
    for i in edition_ids:
        slug = "main" if i == 1 else f"branch-{i}"
        payload = {
            "self_url": f"{base_url}/editions/{i}",
            "product_url": f"{base_url}/products/{product_slug}",
            "build_url": f"{base_url}/builds/{i * 100}",
            "published_url": f"{base_url}/{product_slug}-{slug}/",
            "slug": slug,
            "title": slug,
            "date_created": "2024-01-01T00:00:00+00:00",
            "date_rebuilt": None,
            "date_ended": None,
            "tracked_refs": ["main"],
            "mode": "git_refs",
            "pending_rebuild": False,
        }
        mock_discovery.get(f"{base_url}/editions/{i}").mock(
            return_value=httpx.Response(
                200,
                content=json.dumps(payload).encode(),
                headers={"content-type": "application/json"},
            )
        )


# ---------------------------------------------------------------------------
# Auth + config gating
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_status_returns_404_when_sync_disabled(
    client: AsyncClient,
) -> None:
    """No PUT to enable sync — endpoint returns 404, not 200."""
    await _setup_org(client)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_status_returns_404_when_slug_not_in_allowlist(
    client: AsyncClient,
) -> None:
    """A slug outside the configured allowlist is treated as 404.

    Distinct from the refresh endpoint's 400: GET-side, the resource
    "a sync-eligible project on this org" does not exist for an
    out-of-allowlist slug, so 404 is the appropriate semantic.
    """
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/not-allowed",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_status_403_for_non_admin(client: AsyncClient) -> None:
    """A reader-role user gets 403."""
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_status_403_when_no_auth_header(
    client: AsyncClient,
) -> None:
    """No ``X-Auth-Request-User`` header → 403 from ``require_admin``."""
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}",
    )
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# Stub body when no project state row exists
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_status_stub_when_no_state_row(
    client: AsyncClient,
) -> None:
    """Allowlisted slug, no state row yet — 200 with stub body.

    Acceptance criterion: ``project_state=None``, ``tier_status[*].
    cohort = "unseen"``, ``editions=[]``. The stub answers ops who
    point at a project they've just allowlisted but the next tier_main
    tick has not yet observed.
    """
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["org_slug"] == _ORG
    assert body["ltd_slug"] == _LTD_SLUG
    assert body["project_state"] is None
    assert body["editions"] == []
    assert body.get("edition_diff") is None
    cohorts = {entry["tier"]: entry["cohort"] for entry in body["tier_status"]}
    assert cohorts == {
        "main": "unseen",
        "discovery": "unseen",
        "other": "unseen",
    }
    for entry in body["tier_status"]:
        assert entry["last_polled_at"] is None
        assert entry["next_due_at"] is None


# ---------------------------------------------------------------------------
# Full body when state row + Docverse project exist
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_status_full_body_for_synced_project(
    client: AsyncClient,
) -> None:
    """Synced project with an edition: tier_status reflects dormancy state.

    Seeds a dormant-but-recently-polled project state row whose
    ``date_main_last_polled`` annotation is ~1 h old; the planner gate
    is currently False on tier_main. The explainer must surface
    ``cohort='dormant'`` with a future ``next_due_at`` for tier_main.
    """
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    project_id = await _create_project(client, slug=_LTD_SLUG)
    org_id = await _get_org_id()

    last_polled = datetime.now(tz=UTC) - timedelta(hours=1)
    await _seed_state(
        org_id=org_id,
        resource_type=ResourceType.project,
        ltd_slug=_LTD_SLUG,
        docverse_id=project_id,
        date_last_synced=datetime(2026, 5, 1, tzinfo=UTC),
        date_rebuilt_seen=datetime(2026, 1, 1, tzinfo=UTC),
        annotations={
            ANNOTATION_DATE_MAIN_LAST_POLLED: last_polled.isoformat()
        },
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["project_state"] is not None
    assert "docverse_project_id" not in body["project_state"]
    assert body["project_state"]["ltd_slug"] == _LTD_SLUG
    assert (
        body["project_state"]["annotations"][ANNOTATION_DATE_MAIN_LAST_POLLED]
        == last_polled.isoformat()
    )

    cohorts = {entry["tier"]: entry["cohort"] for entry in body["tier_status"]}
    assert cohorts["main"] == "dormant"
    main_status = next(e for e in body["tier_status"] if e["tier"] == "main")
    assert main_status["last_polled_at"] is not None
    assert main_status["next_due_at"] is not None
    next_due = datetime.fromisoformat(main_status["next_due_at"])
    last_polled_response = datetime.fromisoformat(
        main_status["last_polled_at"]
    )
    # next_due >= last_polled + dormant_interval (jitter only widens).
    assert next_due >= last_polled_response + TIER_MAIN_DORMANT_INTERVAL

    # Editions list should at least include the auto-created __main.
    assert len(body["editions"]) >= 1
    main_edition = next(
        (e for e in body["editions"] if e["kind"] == "main"), None
    )
    assert main_edition is not None
    assert "docverse_edition_id" not in main_edition
    assert "docverse_slug" not in main_edition
    assert "docverse_kind" not in main_edition
    expected_main_url = str(
        client.base_url.join(
            f"/docverse/orgs/{_ORG}/projects/{_LTD_SLUG}/editions/__main"
        )
    )
    assert main_edition["edition_url"] == expected_main_url
    assert main_edition["slug"] == "__main"
    # No edition state row was seeded — the LTD-side join is null.
    assert main_edition["ltd_id"] is None
    assert main_edition["ltd_slug"] is None


@pytest.mark.asyncio
async def test_get_status_edition_left_joins_state_row(
    client: AsyncClient,
) -> None:
    """An edition keeper_sync_state row attaches LTD attribution to the join.

    The acceptance criterion is "default response lists Docverse-side
    editions left-joined with keeper_sync_state rows on docverse_id";
    seeding an edition state row whose ``docverse_id`` matches the
    Docverse __main edition should populate ``ltd_id`` / ``ltd_slug``
    on that edition entry.
    """
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    project_id = await _create_project(client, slug=_LTD_SLUG)
    org_id = await _get_org_id()

    # Seed the project-resource state row to trigger the edition join.
    await _seed_state(
        org_id=org_id,
        resource_type=ResourceType.project,
        ltd_slug=_LTD_SLUG,
        docverse_id=project_id,
        date_rebuilt_seen=datetime.now(tz=UTC),
    )
    # Find the auto-created __main edition's id and seed an edition
    # state row pointing at it.
    main_edition_id = await _get_main_edition_id(project_id)
    sync_time = datetime(2026, 5, 7, 10, tzinfo=UTC)
    await _seed_state(
        org_id=org_id,
        resource_type=ResourceType.edition,
        ltd_id=42,
        ltd_slug="main",
        docverse_id=main_edition_id,
        date_last_synced=sync_time,
    )

    status_response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert status_response.status_code == 200
    body = status_response.json()
    expected_main_url = str(
        client.base_url.join(
            f"/docverse/orgs/{_ORG}/projects/{_LTD_SLUG}/editions/__main"
        )
    )
    main_edition = next(
        e for e in body["editions"] if e["edition_url"] == expected_main_url
    )
    assert main_edition["kind"] == "main"
    assert main_edition["slug"] == "__main"
    assert main_edition["ltd_id"] == 42
    assert main_edition["ltd_slug"] == "main"
    assert (
        datetime.fromisoformat(main_edition["date_last_synced"]) == sync_time
    )


# ---------------------------------------------------------------------------
# ?ltd=true — live LTD diff
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_status_with_ltd_true_returns_edition_diff(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """``?ltd=true`` produces missing_in_docverse / missing_in_ltd arrays.

    Set up:
    - LTD lists editions with ids [1, 2] (slugs ``main``, ``branch-2``).
    - Docverse-side has the auto-created __main edition with a state
      row keyed on a different ltd_id (3) — which means LTD has 1 and
      2 but Docverse tracks only 3.

    Expected diff: ``missing_in_docverse=[main, branch-2]`` (LTD ids 1
    and 2 have no state row), ``missing_in_ltd=[branch-3]`` (state row
    ltd_id 3 is no longer on LTD).
    """
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    project_id = await _create_project(client, slug=_LTD_SLUG)
    org_id = await _get_org_id()

    await _seed_state(
        org_id=org_id,
        resource_type=ResourceType.project,
        ltd_slug=_LTD_SLUG,
        docverse_id=project_id,
        date_rebuilt_seen=datetime.now(tz=UTC),
    )
    # Resolve the auto-created __main edition id and link a state row
    # whose ltd_id is NOT in LTD's listing — that simulates the soft-
    # deletion path on LTD.
    main_edition_id = await _get_main_edition_id(project_id)
    await _seed_state(
        org_id=org_id,
        resource_type=ResourceType.edition,
        ltd_id=3,
        ltd_slug="branch-3",
        docverse_id=main_edition_id,
        date_last_synced=datetime.now(tz=UTC),
    )

    _stub_ltd_editions(
        mock_discovery, product_slug=_LTD_SLUG, edition_ids=[1, 2]
    )

    status_response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}?ltd=true",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert status_response.status_code == 200
    body = status_response.json()
    diff = body["edition_diff"]
    assert diff is not None
    assert diff["missing_in_docverse"] == ["branch-2", "main"]
    assert diff["missing_in_ltd"] == ["branch-3"]


@pytest.mark.asyncio
async def test_get_status_default_omits_edition_diff(
    client: AsyncClient,
) -> None:
    """Default request (no ``?ltd=true``) does not call LTD or include diff.

    Skipping the LTD round-trip is the whole point of making the diff
    opt-in; the test asserts the response shape without seeding any
    LTD respx stubs (a leaked LTD call would surface as a respx
    pass-through error).
    """
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    assert response.json().get("edition_diff") is None


@pytest.mark.asyncio
async def test_get_status_ltd_diff_handles_ltd_error(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An LTD failure during ``?ltd=true`` returns an empty diff, not 5xx.

    Operators inspecting state should not be locked out of the
    project-status endpoint just because LTD is misbehaving — the
    rest of the response (project_state, tier_status, editions) is
    still useful for diagnostics.
    """
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    await _create_project(client, slug=_LTD_SLUG)
    mock_discovery.get(f"{_LTD_BASE}/products/{_LTD_SLUG}/editions/").mock(
        return_value=httpx.Response(500, json={"error": "boom"})
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}?ltd=true",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    diff = response.json()["edition_diff"]
    assert diff == {"missing_in_docverse": [], "missing_in_ltd": []}
