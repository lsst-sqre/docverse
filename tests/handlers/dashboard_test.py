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
