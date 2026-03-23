"""Tests for super admin (config-based username) access to org endpoints."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from docverse.dependencies.context import context_dependency
from tests.conftest import seed_org_with_admin


@pytest.fixture(autouse=True)
def _enable_superadmin_username(app: FastAPI) -> None:  # noqa: ARG001
    """Configure the context dependency with a super admin username."""
    context_dependency._superadmin_usernames = ["superadmin"]


@pytest.mark.asyncio
async def test_superadmin_access_without_membership(
    client: AsyncClient,
) -> None:
    """A super admin can access org endpoints without any membership."""
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "sa-org",
            "title": "Super Admin Org",
            "base_domain": "sa.example.com",
        },
    )
    # Super admin should be able to list projects (reader endpoint)
    response = await client.get(
        "/docverse/orgs/sa-org/projects",
        headers={"X-Auth-Request-User": "superadmin"},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_superadmin_can_create_project(
    client: AsyncClient,
) -> None:
    """Super admin can create a project without explicit membership."""
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "sa-proj-org",
            "title": "SA Project Org",
            "base_domain": "sa-proj.example.com",
        },
    )
    response = await client.post(
        "/docverse/orgs/sa-proj-org/projects",
        json={
            "slug": "my-proj",
            "title": "My Project",
            "doc_repo": "https://github.com/example/proj",
        },
        headers={"X-Auth-Request-User": "superadmin"},
    )
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_superadmin_overrides_reader_membership(
    client: AsyncClient,
) -> None:
    """Super admin with reader membership is still treated as admin."""
    await seed_org_with_admin(client, "sa-reader-org", "org-admin")
    # Add superadmin as reader
    await client.post(
        "/docverse/orgs/sa-reader-org/members",
        json={
            "principal": "superadmin",
            "principal_type": "user",
            "role": "reader",
        },
        headers={"X-Auth-Request-User": "org-admin"},
    )
    # Super admin should still be able to create a project (admin-only)
    response = await client.post(
        "/docverse/orgs/sa-reader-org/projects",
        json={
            "slug": "sa-override-proj",
            "title": "Override Project",
            "doc_repo": "https://github.com/example/override",
        },
        headers={"X-Auth-Request-User": "superadmin"},
    )
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_superadmin_can_manage_members(
    client: AsyncClient,
) -> None:
    """A super admin can manage org members without explicit membership."""
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "sa-mem-org",
            "title": "SA Members Org",
            "base_domain": "sa-mem.example.com",
        },
    )
    response = await client.get(
        "/docverse/orgs/sa-mem-org/members",
        headers={"X-Auth-Request-User": "superadmin"},
    )
    assert response.status_code == 200
