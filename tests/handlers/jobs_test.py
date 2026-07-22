"""Tests for the org-scoped GET /orgs/:org/jobs/:job endpoint."""

from __future__ import annotations

import pytest
import structlog
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency

from docverse.client.models import OrgRole
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.queue import JobKind
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.queue_job_store import QueueJobStore
from tests.conftest import seed_member, seed_org_with_admin

_ADMIN = "admin-user"


async def _org_id(org_slug: str) -> int:
    """Resolve an org's integer primary key from its slug."""
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.get_by_slug(org_slug)
            assert org is not None
            return org.id
    msg = "no session"
    raise AssertionError(msg)


async def _seed_job(
    org_id: int, *, kind: JobKind = JobKind.build_processing
) -> str:
    """Seed a queue job for an org; return its Base32 public id."""
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            store = QueueJobStore(session=session, logger=logger)
            job = await store.create(kind=kind, org_id=org_id)
            await session.commit()
            return serialize_base32_id(job.public_id)
    msg = "no session"
    raise AssertionError(msg)


@pytest.mark.asyncio
async def test_get_org_job_as_reader(client: AsyncClient) -> None:
    """A reader can fetch a job in their org and gets the QueueJob model."""
    await seed_org_with_admin(client, "read-org", _ADMIN)
    await seed_member("read-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("read-org")
    job_id = await _seed_job(org_id)

    response = await client.get(
        f"/docverse/orgs/read-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == job_id
    assert data["kind"] == "build_processing"
    assert data["self_url"].endswith(f"/orgs/read-org/jobs/{job_id}")


@pytest.mark.asyncio
async def test_get_org_job_cross_org_returns_404(client: AsyncClient) -> None:
    """A job belonging to another org returns 404 (no existence leak)."""
    await seed_org_with_admin(client, "owner-org", _ADMIN)
    await seed_org_with_admin(client, "other-org", _ADMIN)
    await seed_member("other-org", "reader-user", OrgRole.reader)

    owner_id = await _org_id("owner-org")
    job_id = await _seed_job(owner_id)

    # reader-user is a reader of other-org but the job lives in owner-org.
    response = await client.get(
        f"/docverse/orgs/other-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_org_job_no_role_returns_403(client: AsyncClient) -> None:
    """A user with no role in the org gets 403."""
    await seed_org_with_admin(client, "perm-org", _ADMIN)
    org_id = await _org_id("perm-org")
    job_id = await _seed_job(org_id)

    response = await client.get(
        f"/docverse/orgs/perm-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "nobody-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_org_job_not_found_returns_404(client: AsyncClient) -> None:
    """A nonexistent job in the org returns 404."""
    await seed_org_with_admin(client, "nf-org", _ADMIN)
    await seed_member("nf-org", "reader-user", OrgRole.reader)

    response = await client.get(
        "/docverse/orgs/nf-org/jobs/1000-0000-0000-05",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 404
