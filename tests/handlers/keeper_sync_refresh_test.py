"""Tests for ``POST /orgs/{org}/keeper-sync/projects/{ltd_slug}/refresh``.

These cover the org-admin-scoped one-shot trigger that bypasses the
tier_main dormancy gate and enqueues an immediate ``keeper_sync_project``
job for the named LTD slug. The handler is the operator-facing counter-
part to the tier-cron enqueue path and so the queue-job semantics
(``keeper_sync_run_id IS NULL``, payload omits ``run_id``,
``subject_label == ltd_slug``) must mirror those of tier_main.
"""

from __future__ import annotations

from typing import Literal

import pytest
import structlog
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import select

from docverse.client.models import OrgRole
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.queue import JobKind
from docverse.storage.organization_store import OrganizationStore
from tests.conftest import seed_member, seed_org_with_admin

_ADMIN = "admin-user"
_ORG = "ks-org"
_LTD_SLUG = "pipelines"


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
            "ltd_base_url": "https://keeper.lsst.codes/",
            "project_slugs": project_slugs,
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200


async def _get_org_id() -> int:
    logger = structlog.get_logger("test")
    async for session in db_session_dependency():
        store = OrganizationStore(session=session, logger=logger)
        org = await store.get_by_slug(_ORG)
        assert org is not None
        return org.id
    msg = "no session"
    raise AssertionError(msg)


@pytest.mark.asyncio
async def test_post_refresh_returns_202_with_queue_job_link(
    client: AsyncClient,
) -> None:
    """Happy path: enqueues one ``keeper_sync_project`` and returns 202."""
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])

    response = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}/refresh",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["job_id"]
    assert body["job_url"].endswith(f"/orgs/{_ORG}/jobs/{body['job_id']}")
    assert response.headers["Location"] == body["job_url"]
    # The job_url resolves via the org-scoped GET.
    job_response = await client.get(
        body["job_url"],
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert job_response.status_code == 200
    assert job_response.json()["id"] == body["job_id"]

    org_id = await _get_org_id()
    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
                SqlQueueJob.org_id == org_id,
            )
            rows = (await session.execute(stmt)).scalars().all()
            assert len(rows) == 1
            row = rows[0]
            # Mirrors the tier-cron enqueue path: no run attribution and
            # the LTD slug is the human-readable subject label.
            assert row.keeper_sync_run_id is None
            assert row.subject_label == _LTD_SLUG
            assert row.backend_job_id is not None


@pytest.mark.asyncio
async def test_post_refresh_wildcard_allowlist_accepts_any_slug(
    client: AsyncClient,
) -> None:
    """``project_slugs == "*"`` does not gate any specific slug."""
    await _setup_org(client)
    await _enable_sync(client, project_slugs="*")

    response = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/some-other-slug/refresh",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 202


@pytest.mark.asyncio
async def test_post_refresh_returns_404_when_sync_disabled(
    client: AsyncClient,
) -> None:
    """``POST /refresh`` against a disabled config returns 404."""
    await _setup_org(client)
    # Default config is disabled — no PUT.
    response = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}/refresh",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_post_refresh_returns_404_when_slug_not_in_allowlist(
    client: AsyncClient,
) -> None:
    """A slug outside the configured allowlist returns 404."""
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])

    response = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/not-allowed/refresh",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_post_refresh_returns_409_when_active_job_exists(
    client: AsyncClient,
) -> None:
    """A second ``POST /refresh`` for the same slug returns 409.

    The first ``POST`` enqueues an active ``keeper_sync_project`` row
    (status='queued'). The mutex pre-check on the second ``POST``
    surfaces as ``ConflictError`` → HTTP 409, mirroring the per-org
    run-uniqueness translation on ``POST /runs``.
    """
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])

    first = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}/refresh",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert first.status_code == 202

    second = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}/refresh",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert second.status_code == 409

    org_id = await _get_org_id()
    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
                SqlQueueJob.org_id == org_id,
            )
            rows = (await session.execute(stmt)).scalars().all()
            # Only the first POST left a row; the 409 short-circuited
            # before any insert.
            assert len(rows) == 1


@pytest.mark.asyncio
async def test_post_refresh_403_for_non_admin(client: AsyncClient) -> None:
    """A reader-role user gets 403."""
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}/refresh",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_post_refresh_403_when_no_auth_header(
    client: AsyncClient,
) -> None:
    """No ``X-Auth-Request-User`` header → 403 from ``require_admin``."""
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    response = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}/refresh",
    )
    assert response.status_code == 403
