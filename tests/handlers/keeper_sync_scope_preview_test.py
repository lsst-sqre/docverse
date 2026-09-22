"""Tests for ``POST /orgs/{org}/keeper-sync/scope-preview``.

The side-effect-free endpoint an operator calls *before* saving a wider
keeper-sync scope. Saving is not inert — the tier crons act on the
stored config at their next tick — so PRD #667 gives the operator a way
to resolve a candidate config against the live LTD product listing
without persisting it or enqueueing any work.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal, cast

import httpx
import pytest
import respx
import structlog
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import OrgRole
from docverse_server.factory import Factory
from docverse_server.services.keeper_sync_tombstone import (
    KeeperSyncTombstoneService,
)
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse_server.storage.ltd.products_client import LtdProductsClient
from docverse_server.storage.organization_store import OrganizationStore
from tests.conftest import seed_member, seed_org_with_admin

_ADMIN = "admin-user"
_ORG = "ks-preview-org"
_LTD_BASE = "https://keeper.lsst.codes"
_PREVIEW_URL = f"/docverse/orgs/{_ORG}/keeper-sync/scope-preview"


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


async def _get_org_id() -> int:
    async for session in db_session_dependency():
        store = OrganizationStore(session=session, logger=_logger())
        org = await store.get_by_slug(_ORG)
        assert org is not None
        return org.id
    msg = "no session"
    raise AssertionError(msg)


async def _seed_project_state(*, org_id: int, ltd_slug: str) -> None:
    """Record a project-resource state row, as an import would."""
    async for session in db_session_dependency():
        async with session.begin():
            state_store = KeeperSyncStateStore(
                session=session, logger=_logger()
            )
            await state_store.upsert(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug=ltd_slug,
            )
            await session.commit()
        return
    msg = "no session"
    raise AssertionError(msg)


async def _record_project_tombstone(*, org_id: int, ltd_slug: str) -> None:
    """Tombstone an LTD product, as an operator-side delete would."""
    async for session in db_session_dependency():
        async with session.begin():
            state_store = KeeperSyncStateStore(
                session=session, logger=_logger()
            )
            service = KeeperSyncTombstoneService(
                session=session, state_store=state_store, logger=_logger()
            )
            await service.record(
                org_id=org_id,
                resource_type=ResourceType.project,
                reason=TombstoneReason.manual_delete,
                ltd_slug=ltd_slug,
            )
            await session.commit()
        return
    msg = "no session"
    raise AssertionError(msg)


def _mock_ltd_products(router: respx.Router, slugs: list[str]) -> None:
    """Seed the LTD ``/products/`` listing with ``slugs``, in order."""
    products = [f"{_LTD_BASE}/products/{slug}/" for slug in slugs]
    router.get(f"{_LTD_BASE}/products/").mock(
        return_value=httpx.Response(
            200,
            content=json.dumps({"products": products}).encode(),
            headers={"content-type": "application/json"},
        )
    )


@dataclass(frozen=True, slots=True)
class _Fetch:
    """One observed attempt to read the LTD product listing."""

    base_url: str
    in_transaction: bool


@dataclass(slots=True)
class _FetchProbe:
    """Every LTD product-listing fetch a request made, in order."""

    fetches: list[_Fetch] = field(default_factory=list)


class _ProbeProductsClient:
    """Records the session state each LTD fetch is made under.

    Wraps the real client rather than replacing it, so the respx-mocked
    listing still supplies the data and only the observation is added.
    """

    def __init__(
        self,
        *,
        session: AsyncSession,
        inner: LtdProductsClient,
        base_url: str,
        probe: _FetchProbe,
    ) -> None:
        self._session = session
        self._inner = inner
        self._base_url = base_url
        self._probe = probe

    async def list_product_slugs(self) -> list[str]:
        self._probe.fetches.append(
            _Fetch(
                base_url=self._base_url,
                in_transaction=self._session.in_transaction(),
            )
        )
        return await self._inner.list_product_slugs()


def _install_fetch_probe(monkeypatch: pytest.MonkeyPatch) -> _FetchProbe:
    """Observe every LTD product fetch the request-scoped factory mints."""
    probe = _FetchProbe()
    create = Factory.create_ltd_products_client

    def _create(self: Factory, *, base_url: str) -> LtdProductsClient:
        return cast(
            "LtdProductsClient",
            _ProbeProductsClient(
                session=self._session,
                inner=create(self, base_url=base_url),
                base_url=base_url,
                probe=probe,
            ),
        )

    monkeypatch.setattr(Factory, "create_ltd_products_client", _create)
    return probe


async def _setup(client: AsyncClient) -> None:
    await seed_org_with_admin(client, _ORG, _ADMIN)


async def _put_config(
    client: AsyncClient,
    *,
    enabled: bool = True,
    project_slugs: list[str] | Literal["*"] | None = None,
    project_slug_patterns: list[str] | None = None,
    exclude_project_slugs: list[str] | None = None,
    exclude_project_slug_patterns: list[str] | None = None,
) -> None:
    response = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={
            "enabled": enabled,
            "ltd_base_url": f"{_LTD_BASE}/",
            "project_slugs": (
                project_slugs if project_slugs is not None else []
            ),
            "project_slug_patterns": project_slug_patterns or [],
            "exclude_project_slugs": exclude_project_slugs or [],
            "exclude_project_slug_patterns": (
                exclude_project_slug_patterns or []
            ),
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200


async def _preview(
    client: AsyncClient,
    *,
    body: dict[str, Any] | None = None,
    username: str = _ADMIN,
) -> httpx.Response:
    kwargs: dict[str, Any] = {"headers": {"X-Auth-Request-User": username}}
    if body is not None:
        kwargs["json"] = body
    return await client.post(_PREVIEW_URL, **kwargs)


@pytest.mark.asyncio
async def test_empty_body_previews_the_stored_config(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """With no body, the stored config is resolved against live LTD.

    ``in_scope_slugs`` follows the LTD listing order — the order a
    backfill fans its children out in — not the config's order.
    """
    await _setup(client)
    await _put_config(client, project_slug_patterns=[r"sqr-\d+"])
    _mock_ltd_products(mock_discovery, ["sqr-060", "dmtn-201", "sqr-112"])

    response = await _preview(client)

    assert response.status_code == 200
    body = response.json()
    assert body["ltd_count"] == 3
    assert body["in_scope_count"] == 2
    assert body["in_scope_slugs"] == ["sqr-060", "sqr-112"]


@pytest.mark.asyncio
async def test_candidate_body_widens_the_scope(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    r"""A candidate body is merged over the stored config, as ``PATCH`` does.

    Only the fields the body carries change: the stored ``sqr-\d+``
    pattern is replaced by the candidate's two-pattern list, while the
    stored ``exclude_project_slugs`` is left in force.
    """
    await _setup(client)
    await _put_config(
        client,
        project_slug_patterns=[r"sqr-\d+"],
        exclude_project_slugs=["sqr-060"],
    )
    _mock_ltd_products(
        mock_discovery, ["sqr-060", "dmtn-201", "sqr-112", "www"]
    )

    response = await _preview(
        client,
        body={"project_slug_patterns": [r"sqr-\d+", r"dmtn-\d+"]},
    )

    assert response.status_code == 200
    body = response.json()
    # ``sqr-060`` stays out: the un-patched exclude still applies.
    assert body["in_scope_slugs"] == ["dmtn-201", "sqr-112"]
    assert body["in_scope_count"] == 2


@pytest.mark.asyncio
async def test_preview_persists_nothing_and_enqueues_nothing(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A candidate preview leaves the stored config and job list alone.

    The whole point of the endpoint: an operator can try a scope on
    without the tier crons picking it up at their next tick.
    """
    await _setup(client)
    stored = {
        "enabled": True,
        "ltd_base_url": f"{_LTD_BASE}/",
        "project_slugs": ["sqr-112"],
        "project_slug_patterns": [],
        "exclude_project_slugs": [],
        "exclude_project_slug_patterns": [],
    }
    put = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json=stored,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert put.status_code == 200
    _mock_ltd_products(mock_discovery, ["sqr-060", "sqr-112", "dmtn-201"])

    jobs_before = await client.get(
        f"/docverse/orgs/{_ORG}/jobs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert jobs_before.status_code == 200

    preview = await _preview(
        client,
        body={"project_slugs": "*", "enabled": False},
    )
    assert preview.status_code == 200
    assert preview.json()["in_scope_count"] == 3

    fetched = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert fetched.status_code == 200
    assert fetched.json() == stored

    jobs_after = await client.get(
        f"/docverse/orgs/{_ORG}/jobs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert jobs_after.status_code == 200
    assert jobs_after.json() == jobs_before.json()


@pytest.mark.asyncio
async def test_new_slugs_excludes_already_tracked_projects(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """``new_slugs`` is the in-scope slugs with no state row yet.

    A slug keeper-sync has already imported has a project-resource
    state row, so it is in scope but not new — the operator sees only
    what the next backfill would import for the first time.
    """
    await _setup(client)
    await _put_config(client, project_slugs="*")
    org_id = await _get_org_id()
    await _seed_project_state(org_id=org_id, ltd_slug="sqr-112")
    _mock_ltd_products(mock_discovery, ["sqr-060", "sqr-112", "dmtn-201"])

    response = await _preview(client)

    assert response.status_code == 200
    body = response.json()
    assert body["in_scope_slugs"] == ["sqr-060", "sqr-112", "dmtn-201"]
    assert body["new_slugs"] == ["sqr-060", "dmtn-201"]


@pytest.mark.asyncio
async def test_tombstoned_in_scope_slug_is_reported(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A tombstoned in-scope slug is listed, and is not ``new``.

    ``in_scope_slugs`` reports the scope as the *config* resolves it, so
    the slug stays there; ``tombstoned_slugs`` is what tells the
    operator sync will skip it until the tombstone is cleared.
    """
    await _setup(client)
    await _put_config(client, project_slugs="*")
    org_id = await _get_org_id()
    await _record_project_tombstone(org_id=org_id, ltd_slug="sqr-060")
    _mock_ltd_products(mock_discovery, ["sqr-060", "sqr-112"])

    response = await _preview(client)

    assert response.status_code == 200
    body = response.json()
    assert body["in_scope_slugs"] == ["sqr-060", "sqr-112"]
    assert body["tombstoned_slugs"] == ["sqr-060"]
    # The tombstoned slug has a state row, so it is known, not new.
    assert body["new_slugs"] == ["sqr-112"]


@pytest.mark.asyncio
async def test_misspelled_exact_slugs_are_reported_as_unmatched(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Exact-slug entries LTD does not list are surfaced as typos.

    Both exact-slug fields are checked, ``project_slugs`` first. Pattern
    fields are not: a pattern matching nothing is how a future wave is
    staged, so flagging it would be noise.
    """
    await _setup(client)
    await _put_config(
        client,
        project_slugs=["sqr-112", "sqr-9999"],
        project_slug_patterns=[r"nothing-matches-\d+"],
        exclude_project_slugs=["wwww"],
    )
    _mock_ltd_products(mock_discovery, ["sqr-060", "sqr-112", "www"])

    response = await _preview(client)

    assert response.status_code == 200
    body = response.json()
    assert body["unmatched_project_slugs"] == ["sqr-9999", "wwww"]
    assert body["in_scope_slugs"] == ["sqr-112"]


@pytest.mark.asyncio
async def test_preview_works_while_sync_is_disabled(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A disabled org can still stage a scope before turning sync on."""
    await _setup(client)
    await _put_config(
        client, enabled=False, project_slug_patterns=[r"sqr-\d+"]
    )
    _mock_ltd_products(mock_discovery, ["sqr-060", "dmtn-201"])

    response = await _preview(client)

    assert response.status_code == 200
    assert response.json()["in_scope_slugs"] == ["sqr-060"]


@pytest.mark.asyncio
async def test_preview_without_a_stored_config_resolves_the_default(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An org that has never been configured previews the default config.

    The default has an empty ``project_slugs`` and no patterns, so
    nothing is in scope — but the LTD listing is still reported, which
    is what makes the endpoint useful for a first-time setup.
    """
    await _setup(client)
    _mock_ltd_products(mock_discovery, ["sqr-060", "dmtn-201"])

    response = await _preview(client)

    assert response.status_code == 200
    body = response.json()
    assert body["ltd_count"] == 2
    assert body["in_scope_count"] == 0
    assert body["in_scope_slugs"] == []


@pytest.mark.asyncio
async def test_uncompilable_candidate_pattern_is_rejected(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An invalid candidate body 422s with the same validation as ``PATCH``.

    The preview must not be a way to sneak past the pattern rules —
    an operator checking a candidate here should see the same 422 the
    later ``PATCH`` would give, naming the field and the pattern.
    """
    await _setup(client)
    await _put_config(client, project_slugs="*")
    _mock_ltd_products(mock_discovery, ["sqr-060"])

    response = await _preview(
        client, body={"project_slug_patterns": ["sqr-("]}
    )

    assert response.status_code == 422
    detail = json.dumps(response.json()["detail"])
    assert "project_slug_patterns" in detail
    assert "sqr-(" in detail


@pytest.mark.asyncio
async def test_ltd_is_fetched_with_no_transaction_open(
    client: AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The LTD fetch never runs inside a database transaction.

    LTD is a third party on the far side of an httpx timeout. Reading
    the org row and then awaiting that fetch inside one transaction
    pinned a pooled Postgres connection ``idle in transaction`` for the
    whole timeout — and previews are exactly what an operator retries
    when LTD is slow, so the pin multiplies under the retries. The
    request therefore takes two short transactions with the fetch in
    the gap between them.
    """
    await _setup(client)
    await _put_config(client, project_slugs="*")
    _mock_ltd_products(mock_discovery, ["sqr-060", "sqr-112"])
    probe = _install_fetch_probe(monkeypatch)

    response = await _preview(client)

    assert response.status_code == 200
    assert response.json()["in_scope_slugs"] == ["sqr-060", "sqr-112"]
    assert [fetch.in_transaction for fetch in probe.fetches] == [False]
    # And the listing came from the stored config's LTD instance.
    assert probe.fetches[0].base_url.startswith(_LTD_BASE)


@pytest.mark.asyncio
async def test_candidate_ltd_base_url_is_rejected_before_any_fetch(
    client: AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A candidate ``ltd_base_url`` is a 422, and nothing is fetched.

    Merging a candidate base URL in before the fetch turned the preview
    into an interactive status oracle: an org admin could aim it at an
    in-cluster service name or ``http://169.254.169.254/`` and read the
    upstream status and failure class back out of the 502. The field is
    refused by name, and the refusal lands before any outbound request.
    """
    await _setup(client)
    await _put_config(client, project_slugs="*")
    _mock_ltd_products(mock_discovery, ["sqr-060"])
    probe = _install_fetch_probe(monkeypatch)

    response = await _preview(
        client,
        body={
            "project_slugs": "*",
            "ltd_base_url": "http://169.254.169.254/",
        },
    )

    assert response.status_code == 422
    assert "ltd_base_url" in json.dumps(response.json()["detail"])
    assert probe.fetches == []


@pytest.mark.asyncio
async def test_explicit_null_ltd_base_url_is_rejected(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """Naming the field at all is the error, ``null`` included."""
    await _setup(client)
    await _put_config(client, project_slugs="*")
    _mock_ltd_products(mock_discovery, ["sqr-060"])

    response = await _preview(client, body={"ltd_base_url": None})

    assert response.status_code == 422
    assert "ltd_base_url" in json.dumps(response.json()["detail"])


@pytest.mark.asyncio
async def test_explicit_null_candidate_field_is_rejected(
    client: AsyncClient,
) -> None:
    """``null`` is rejected here exactly as it is on ``PATCH``."""
    await _setup(client)
    await _put_config(client, project_slugs="*")

    response = await _preview(client, body={"project_slugs": None})

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_ltd_failure_surfaces_as_502_with_the_upstream_status(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """An LTD outage is a 502 naming LTD's status, never a Docverse 500."""
    await _setup(client)
    await _put_config(client, project_slugs="*")
    mock_discovery.get(f"{_LTD_BASE}/products/").mock(
        return_value=httpx.Response(503, text="LTD is down")
    )

    response = await _preview(client)

    assert response.status_code == 502
    message = response.json()["detail"][0]["msg"]
    assert "503" in message
    assert response.json()["detail"][0]["type"] == "upstream_error"


@pytest.mark.asyncio
async def test_ltd_transport_failure_surfaces_as_502(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A connect failure has no upstream status but is still a 502."""
    await _setup(client)
    await _put_config(client, project_slugs="*")
    mock_discovery.get(f"{_LTD_BASE}/products/").mock(
        side_effect=httpx.ConnectError("connection refused")
    )

    response = await _preview(client)

    assert response.status_code == 502
    assert response.json()["detail"][0]["type"] == "upstream_error"


@pytest.mark.asyncio
async def test_ltd_html_body_on_a_200_surfaces_as_502(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A maintenance page served as 200 is still LTD's fault, not ours.

    LTD answering 200 is not a promise that the body is the product
    listing — a proxy or maintenance page in front of it answers 200
    with HTML. Parsing that used to raise ``json.JSONDecodeError`` from
    outside the handled taxonomy and render a Docverse 500, which the
    endpoint's contract (and ``docs/keeper-sync-scope.md``) says never
    happens (issue #675).
    """
    await _setup(client)
    await _put_config(client, project_slugs="*")
    mock_discovery.get(f"{_LTD_BASE}/products/").mock(
        return_value=httpx.Response(
            200,
            content=b"<html><body>LTD is down for maintenance</body></html>",
            headers={"content-type": "text/html"},
        )
    )

    response = await _preview(client)

    assert response.status_code == 502
    detail = response.json()["detail"][0]
    assert detail["type"] == "upstream_error"
    # The upstream status is still named, even though it is a 200.
    assert "200" in detail["msg"]


@pytest.mark.asyncio
async def test_ltd_json_array_body_surfaces_as_502(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """A JSON array where the listing object belongs is also a 502."""
    await _setup(client)
    await _put_config(client, project_slugs="*")
    mock_discovery.get(f"{_LTD_BASE}/products/").mock(
        return_value=httpx.Response(
            200,
            content=json.dumps([f"{_LTD_BASE}/products/sqr-060/"]).encode(),
            headers={"content-type": "application/json"},
        )
    )

    response = await _preview(client)

    assert response.status_code == 502
    assert response.json()["detail"][0]["type"] == "upstream_error"


@pytest.mark.asyncio
async def test_non_admin_is_rejected(
    client: AsyncClient,
    mock_discovery: respx.Router,
) -> None:
    """The preview is org-admin only, like the rest of keeper-sync."""
    await _setup(client)
    await _put_config(client, project_slugs="*")
    await seed_member(_ORG, "uploader-user", OrgRole.uploader)
    _mock_ltd_products(mock_discovery, ["sqr-060"])

    response = await _preview(client, username="uploader-user")

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_openapi_documents_the_endpoint(client: AsyncClient) -> None:
    """The contract carries the optional body, the 502, and an example.

    The OpenAPI spec is the whole announcement for an operator-facing
    endpoint nobody has a CLI for yet, so the pieces that make it usable
    — that the body may be omitted, that a 502 means LTD and not
    Docverse, and a worked example of the report — are pinned here.
    """
    spec = (await client.get("/docverse/openapi.json")).json()
    operation = spec["paths"][
        "/docverse/orgs/{org}/keeper-sync/scope-preview"
    ]["post"]
    assert operation["operationId"] == "post_org_keeper_sync_scope_preview"
    assert operation["description"]
    # The candidate body is optional: an empty request previews the
    # stored config.
    assert operation["requestBody"].get("required", False) is False
    assert "502" in operation["responses"]

    # The body is the preview's own request model, and it does not
    # advertise ``ltd_base_url`` — a field it would always reject.
    body_ref = operation["requestBody"]["content"]["application/json"][
        "schema"
    ]
    assert "KeeperSyncScopePreviewRequest" in json.dumps(body_ref)
    request_schema = spec["components"]["schemas"][
        "KeeperSyncScopePreviewRequest"
    ]
    assert "ltd_base_url" not in request_schema["properties"]
    assert "project_slug_patterns" in request_schema["properties"]

    schema = spec["components"]["schemas"]["KeeperSyncScopePreview"]
    assert schema["examples"]
    for name in (
        "ltd_count",
        "in_scope_count",
        "in_scope_slugs",
        "new_slugs",
        "tombstoned_slugs",
        "unmatched_project_slugs",
    ):
        assert schema["properties"][name]["description"]
