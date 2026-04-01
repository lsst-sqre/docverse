"""Tests for project endpoints."""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import pytest
from httpx import AsyncClient

from tests.conftest import seed_org_with_admin


async def _setup(client: AsyncClient) -> None:
    """Create an org and seed an admin membership."""
    await seed_org_with_admin(client, "proj-org", "testuser")


@pytest.mark.asyncio
async def test_create_project(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "my-docs",
            "title": "My Docs",
            "doc_repo": "https://github.com/example/docs",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["slug"] == "my-docs"
    assert data["title"] == "My Docs"
    assert "id" not in data
    assert data["self_url"].endswith("/orgs/proj-org/projects/my-docs")


@pytest.mark.asyncio
async def test_list_projects(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "proj-aa",
            "title": "A",
            "doc_repo": "https://github.com/example/a",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    slugs = [p["slug"] for p in data]
    assert "proj-aa" in slugs
    assert "Link" in response.headers
    assert "X-Total-Count" in response.headers


@pytest.mark.asyncio
async def test_get_project(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "get-proj",
            "title": "Get Proj",
            "doc_repo": "https://github.com/example/get",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/proj-org/projects/get-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["slug"] == "get-proj"


@pytest.mark.asyncio
async def test_get_project_not_found(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/proj-org/projects/nonexistent",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_project(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "patch-proj",
            "title": "Original",
            "doc_repo": "https://github.com/example/patch",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/patch-proj",
        json={"title": "Updated"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["title"] == "Updated"


@pytest.mark.asyncio
async def test_delete_project(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "del-proj",
            "title": "Delete Me",
            "doc_repo": "https://github.com/example/del",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.delete(
        "/docverse/orgs/proj-org/projects/del-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 204

    # Should not be found after soft delete
    response = await client.get(
        "/docverse/orgs/proj-org/projects/del-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_search_by_slug(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    for slug, title in [
        ("pipelines-guide", "Pipelines Guide"),
        ("pipeline-tutorial", "Pipeline Tutorial"),
        ("admin-manual", "Admin Manual"),
    ]:
        await client.post(
            "/docverse/orgs/proj-org/projects",
            json={
                "slug": slug,
                "title": title,
                "doc_repo": f"https://github.com/example/{slug}",
            },
            headers=headers,
        )
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "pipeline"},
        headers=headers,
    )
    assert response.status_code == 200
    data = response.json()
    slugs = [p["slug"] for p in data]
    assert "pipelines-guide" in slugs
    assert "pipeline-tutorial" in slugs
    assert "admin-manual" not in slugs
    assert int(response.headers["X-Total-Count"]) == len(data)
    assert 'rel="next"' not in response.headers.get("Link", "")


@pytest.mark.asyncio
async def test_search_by_title(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    for slug, title in [
        ("proj-a", "Deployment Guide"),
        ("proj-b", "Developer Handbook"),
        ("proj-c", "API Reference"),
    ]:
        await client.post(
            "/docverse/orgs/proj-org/projects",
            json={
                "slug": slug,
                "title": title,
                "doc_repo": f"https://github.com/example/{slug}",
            },
            headers=headers,
        )
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "guide"},
        headers=headers,
    )
    assert response.status_code == 200
    data = response.json()
    slugs = [p["slug"] for p in data]
    assert "proj-a" in slugs
    assert "proj-c" not in slugs


@pytest.mark.asyncio
async def test_search_no_results(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "zzzznonexistent"},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == []
    assert response.headers["X-Total-Count"] == "0"


@pytest.mark.asyncio
async def test_search_pagination(client: AsyncClient) -> None:
    """Search results can be paginated via cursor."""
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    # Create 4 projects that all match "pipeline" to exceed a limit of 2
    for i in range(4):
        await client.post(
            "/docverse/orgs/proj-org/projects",
            json={
                "slug": f"pipeline-{i}",
                "title": f"Pipeline Project {i}",
                "doc_repo": f"https://github.com/example/pipeline-{i}",
            },
            headers=headers,
        )

    # First page
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "pipeline", "limit": 2},
        headers=headers,
    )
    assert response.status_code == 200
    first_page = response.json()
    assert len(first_page) == 2
    total = int(response.headers["X-Total-Count"])
    assert total == 4
    assert "Link" in response.headers
    link_header = response.headers["Link"]
    assert 'rel="next"' in link_header

    # Extract next cursor from Link header
    next_cursor = None
    for link_part in link_header.split(","):
        stripped = link_part.strip()
        if 'rel="next"' in stripped:
            url_part = stripped.split(";")[0].strip().strip("<>")
            parsed = urlparse(url_part)
            qs = parse_qs(parsed.query)
            next_cursor = qs["cursor"][0]
            break
    assert next_cursor is not None

    # Second page
    response2 = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "pipeline", "limit": 2, "cursor": next_cursor},
        headers=headers,
    )
    assert response2.status_code == 200
    second_page = response2.json()
    assert len(second_page) == 2
    assert int(response2.headers["X-Total-Count"]) == total

    # No duplicates across pages
    first_slugs = {p["slug"] for p in first_page}
    second_slugs = {p["slug"] for p in second_page}
    assert first_slugs.isdisjoint(second_slugs)


@pytest.mark.asyncio
async def test_search_org_scoping(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    # Create a project in proj-org
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "scoped-proj",
            "title": "Scoped Project",
            "doc_repo": "https://github.com/example/scoped",
        },
        headers=headers,
    )
    # Create a second org with a similarly-named project
    await seed_org_with_admin(client, "other-org", "testuser")
    await client.post(
        "/docverse/orgs/other-org/projects",
        json={
            "slug": "scoped-proj",
            "title": "Scoped Project Other",
            "doc_repo": "https://github.com/example/scoped-other",
        },
        headers=headers,
    )
    # Search in proj-org should not return other-org projects
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "scoped"},
        headers=headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["slug"] == "scoped-proj"
    assert data[0]["self_url"].endswith("/orgs/proj-org/projects/scoped-proj")


@pytest.mark.asyncio
async def test_search_excludes_soft_deleted(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "deleted-proj",
            "title": "Deleted Project",
            "doc_repo": "https://github.com/example/deleted",
        },
        headers=headers,
    )
    await client.delete(
        "/docverse/orgs/proj-org/projects/deleted-proj",
        headers=headers,
    )
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "deleted"},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_create_project_duplicate_slug(client: AsyncClient) -> None:
    await _setup(client)
    payload = {
        "slug": "dup-proj",
        "title": "First",
        "doc_repo": "https://github.com/example/dup",
    }
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json=payload,
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json=payload,
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_create_project_has_default_edition(
    client: AsyncClient,
) -> None:
    """POST project creates a __main edition with default tracking."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "default-ed",
            "title": "Default Ed",
            "doc_repo": "https://github.com/example/default-ed",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    edition = data["default_edition"]
    assert edition is not None
    assert edition["slug"] == "__main"
    assert edition["kind"] == "main"
    assert edition["tracking_mode"] == "git_ref"
    assert edition["tracking_params"] == {"git_ref": "main"}
    assert edition["title"] == "Main"
    assert edition["lifecycle_exempt"] is True
    assert edition["self_url"].endswith(
        "/orgs/proj-org/projects/default-ed/editions/__main"
    )


@pytest.mark.asyncio
async def test_create_project_custom_default_edition(
    client: AsyncClient,
) -> None:
    """POST project with custom default_edition config."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "custom-ed",
            "title": "Custom Ed",
            "doc_repo": "https://github.com/example/custom-ed",
            "default_edition": {
                "tracking_mode": "lsst_doc",
                "title": "Custom Main",
                "lifecycle_exempt": False,
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    edition = response.json()["default_edition"]
    assert edition["tracking_mode"] == "lsst_doc"
    assert edition["title"] == "Custom Main"
    assert edition["lifecycle_exempt"] is False


@pytest.mark.asyncio
async def test_get_project_includes_default_edition(
    client: AsyncClient,
) -> None:
    """GET single project includes the default edition."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "get-ed-proj",
            "title": "Get Ed Proj",
            "doc_repo": "https://github.com/example/get-ed",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/proj-org/projects/get-ed-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    edition = response.json()["default_edition"]
    assert edition is not None
    assert edition["slug"] == "__main"


@pytest.mark.asyncio
async def test_list_projects_no_default_edition(
    client: AsyncClient,
) -> None:
    """GET project list omits default_edition (None)."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "list-ed-proj",
            "title": "List Ed Proj",
            "doc_repo": "https://github.com/example/list-ed",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    for project in response.json():
        assert project["default_edition"] is None


@pytest.mark.asyncio
async def test_patch_project_includes_default_edition(
    client: AsyncClient,
) -> None:
    """PATCH project response includes the default edition."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "patch-ed-proj",
            "title": "Patch Ed Proj",
            "doc_repo": "https://github.com/example/patch-ed",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/patch-ed-proj",
        json={"title": "Patched"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    edition = response.json()["default_edition"]
    assert edition is not None
    assert edition["slug"] == "__main"


@pytest.mark.asyncio
async def test_create_project_org_default_edition_config(
    client: AsyncClient,
) -> None:
    """Org-level default_edition_config is used when project omits it."""
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "org-dec",
            "title": "Org With Default Config",
            "base_domain": "example.io",
            "default_edition_config": {
                "tracking_mode": "git_ref",
                "tracking_params": {"git_ref": "develop"},
                "title": "Org Default",
            },
            "members": [
                {
                    "principal": "testuser",
                    "principal_type": "user",
                    "role": "admin",
                }
            ],
        },
        headers={"X-Auth-Request-User": "superadmin"},
    )
    response = await client.post(
        "/docverse/orgs/org-dec/projects",
        json={
            "slug": "org-proj",
            "title": "Org Proj",
            "doc_repo": "https://github.com/example/org-proj",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    edition = response.json()["default_edition"]
    assert edition["tracking_params"] == {"git_ref": "develop"}
    assert edition["title"] == "Org Default"


@pytest.mark.asyncio
async def test_create_project_request_overrides_org_config(
    client: AsyncClient,
) -> None:
    """Explicit default_edition in request overrides org config."""
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "org-override",
            "title": "Org Override",
            "base_domain": "example.io",
            "default_edition_config": {
                "tracking_mode": "git_ref",
                "tracking_params": {"git_ref": "develop"},
                "title": "Org Default",
            },
            "members": [
                {
                    "principal": "testuser",
                    "principal_type": "user",
                    "role": "admin",
                }
            ],
        },
        headers={"X-Auth-Request-User": "superadmin"},
    )
    response = await client.post(
        "/docverse/orgs/org-override/projects",
        json={
            "slug": "override-proj",
            "title": "Override Proj",
            "doc_repo": "https://github.com/example/override",
            "default_edition": {
                "tracking_mode": "git_ref",
                "tracking_params": {"git_ref": "master"},
                "title": "Request Override",
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    edition = response.json()["default_edition"]
    assert edition["tracking_params"] == {"git_ref": "master"}
    assert edition["title"] == "Request Override"


@pytest.mark.asyncio
async def test_permission_denied_no_auth(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
    )
    assert response.status_code == 403
