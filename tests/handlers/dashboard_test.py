"""Tests for the dashboard rebuild handler."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from docverse.client.models import OrgRole
from tests.conftest import seed_member, seed_org_with_admin


async def _setup(client: AsyncClient) -> None:
    await seed_org_with_admin(client, "dash-org", "admin-user")
    await client.post(
        "/docverse/orgs/dash-org/projects",
        json={
            "slug": "dash-proj",
            "title": "Dash Project",
            "doc_repo": "https://github.com/example/dash",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )


@pytest.mark.asyncio
async def test_dashboard_rebuild_returns_202_with_queue_id(
    client: AsyncClient,
) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/dash-org/projects/dash-proj/dashboard/rebuild",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 202
    body = response.json()
    assert "queue_job_id" in body
    assert isinstance(body["queue_job_id"], str)
    assert len(body["queue_job_id"]) > 0
    assert body["queue_job_url"].endswith(
        f"/queue/jobs/{body['queue_job_id']}"
    )


@pytest.mark.asyncio
async def test_dashboard_rebuild_404_for_unknown_project(
    client: AsyncClient,
) -> None:
    await seed_org_with_admin(client, "dash-org", "admin-user")
    response = await client.post(
        "/docverse/orgs/dash-org/projects/nope/dashboard/rebuild",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_dashboard_rebuild_403_without_admin_scope(
    client: AsyncClient,
) -> None:
    await _setup(client)
    await seed_member("dash-org", "reader-user", OrgRole.reader)
    response = await client.post(
        "/docverse/orgs/dash-org/projects/dash-proj/dashboard/rebuild",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_dashboard_rebuild_403_without_auth_header(
    client: AsyncClient,
) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/dash-org/projects/dash-proj/dashboard/rebuild",
    )
    assert response.status_code == 403


async def _create_project(
    client: AsyncClient, org_slug: str, slug: str, *, admin: str
) -> None:
    response = await client.post(
        f"/docverse/orgs/{org_slug}/projects",
        json={
            "slug": slug,
            "title": f"Project {slug}",
            "doc_repo": f"https://github.com/example/{slug}",
        },
        headers={"X-Auth-Request-User": admin},
    )
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_org_dashboard_rebuild_returns_one_job_per_project(
    client: AsyncClient,
) -> None:
    await seed_org_with_admin(client, "dash-org", "admin-user")
    for slug in ("alpha", "beta", "gamma"):
        await _create_project(client, "dash-org", slug, admin="admin-user")

    response = await client.post(
        "/docverse/orgs/dash-org/dashboard/rebuild",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 202
    body = response.json()
    assert isinstance(body, list)
    assert len(body) == 3
    by_slug = {entry["project_slug"]: entry for entry in body}
    assert set(by_slug) == {"alpha", "beta", "gamma"}
    job_ids = {entry["queue_job_id"] for entry in body}
    assert len(job_ids) == 3
    assert all(isinstance(jid, str) and jid for jid in job_ids)
    for entry in body:
        assert entry["queue_job_url"].endswith(
            f"/queue/jobs/{entry['queue_job_id']}"
        )


@pytest.mark.asyncio
async def test_org_dashboard_rebuild_empty_when_no_projects(
    client: AsyncClient,
) -> None:
    await seed_org_with_admin(client, "dash-org", "admin-user")
    response = await client.post(
        "/docverse/orgs/dash-org/dashboard/rebuild",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 202
    assert response.json() == []


@pytest.mark.asyncio
async def test_org_dashboard_rebuild_excludes_deleted_projects(
    client: AsyncClient,
) -> None:
    await seed_org_with_admin(client, "dash-org", "admin-user")
    for slug in ("keep-one", "delete-me", "keep-two"):
        await _create_project(client, "dash-org", slug, admin="admin-user")

    delete_response = await client.delete(
        "/docverse/orgs/dash-org/projects/delete-me",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert delete_response.status_code == 204

    response = await client.post(
        "/docverse/orgs/dash-org/dashboard/rebuild",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 202
    body = response.json()
    slugs = {entry["project_slug"] for entry in body}
    assert slugs == {"keep-one", "keep-two"}


@pytest.mark.asyncio
async def test_org_dashboard_rebuild_403_without_admin_scope(
    client: AsyncClient,
) -> None:
    await seed_org_with_admin(client, "dash-org", "admin-user")
    await _create_project(client, "dash-org", "dash-proj", admin="admin-user")
    await seed_member("dash-org", "reader-user", OrgRole.reader)
    response = await client.post(
        "/docverse/orgs/dash-org/dashboard/rebuild",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_org_dashboard_rebuild_404_for_unknown_org(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/docverse/orgs/missing-org/dashboard/rebuild",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_org_dashboard_rebuild_403_without_auth_header(
    client: AsyncClient,
) -> None:
    await seed_org_with_admin(client, "dash-org", "admin-user")
    response = await client.post(
        "/docverse/orgs/dash-org/dashboard/rebuild",
    )
    assert response.status_code == 403
