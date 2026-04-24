"""Tests for the dashboard-template binding handlers."""

from __future__ import annotations

import pytest
import structlog
from httpx import AsyncClient
from safir.arq import JobMetadata, MockArqQueue
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency

from docverse.client.models import OrgRole
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
    GitHubTemplateFileInput,
    GitHubTemplateKey,
)
from docverse.storage.organization_store import OrganizationStore
from tests.conftest import seed_member, seed_org_with_admin

_ADMIN = "admin-user"
_ORG = "tmpl-org"
_PROJECT = "tmpl-proj"

_VALID_BODY = {
    "github_owner": "lsst-sqre",
    "github_repo": "docverse-templates",
    "github_ref": "main",
    "root_path": "/",
}


async def _setup_org(client: AsyncClient) -> None:
    await seed_org_with_admin(client, _ORG, _ADMIN)


async def _setup_org_and_project(client: AsyncClient) -> None:
    await _setup_org(client)
    response = await client.post(
        f"/docverse/orgs/{_ORG}/projects",
        json={
            "slug": _PROJECT,
            "title": "Tmpl Project",
            "doc_repo": "https://github.com/example/tmpl",
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201


# ---------------------------------------------------------------------------
# Org-default binding
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_org_put_creates_binding(client: AsyncClient) -> None:
    await _setup_org(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["github_owner"] == "lsst-sqre"
    assert body["github_repo"] == "docverse-templates"
    assert body["github_ref"] == "main"
    assert body["root_path"] == "/"
    assert body["last_sync_status"] == "pending"
    assert body["last_sync_error"] is None
    assert "date_created" in body
    assert "date_updated" in body
    assert body["self_url"].endswith(f"/orgs/{_ORG}/dashboard-template")


@pytest.mark.asyncio
async def test_org_put_defaults_root_path(client: AsyncClient) -> None:
    await _setup_org(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json={
            "github_owner": "lsst-sqre",
            "github_repo": "docverse-templates",
            "github_ref": "main",
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201
    assert response.json()["root_path"] == "/"


@pytest.mark.asyncio
async def test_org_get_returns_existing_binding(client: AsyncClient) -> None:
    await _setup_org(client)
    await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    response = await client.get(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["github_owner"] == "lsst-sqre"
    assert body["github_ref"] == "main"
    assert body["last_sync_status"] == "pending"


@pytest.mark.asyncio
async def test_org_get_404_when_unset(client: AsyncClient) -> None:
    await _setup_org(client)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_org_delete_removes_binding(client: AsyncClient) -> None:
    await _setup_org(client)
    await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    response = await client.delete(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 204

    follow_up = await client.get(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert follow_up.status_code == 404


@pytest.mark.asyncio
async def test_org_delete_404_when_unset(client: AsyncClient) -> None:
    await _setup_org(client)
    response = await client.delete(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_org_put_is_idempotent_no_op(client: AsyncClient) -> None:
    """Re-PUT with identical values leaves date_updated/status unchanged."""
    await _setup_org(client)
    first = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert first.status_code == 201
    first_body = first.json()

    second = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert second.status_code == 200
    second_body = second.json()

    assert second_body["date_created"] == first_body["date_created"]
    assert second_body["date_updated"] == first_body["date_updated"]
    assert second_body["last_sync_status"] == first_body["last_sync_status"]


@pytest.mark.asyncio
async def test_org_put_updates_existing_binding(client: AsyncClient) -> None:
    await _setup_org(client)
    await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    changed = {**_VALID_BODY, "github_ref": "release"}
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=changed,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["github_ref"] == "release"


@pytest.mark.asyncio
async def test_org_put_unauthenticated_returns_403(
    client: AsyncClient,
) -> None:
    await _setup_org(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_org_put_non_admin_returns_403(client: AsyncClient) -> None:
    await _setup_org(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_org_get_non_admin_returns_403(client: AsyncClient) -> None:
    await _setup_org(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_org_put_unknown_org_returns_404(client: AsyncClient) -> None:
    response = await client.put(
        "/docverse/orgs/no-such-org/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_org_put_does_not_auto_link_github_template(
    client: AsyncClient,
) -> None:
    """Re-PUT after a template row exists does not auto-link it.

    Even if a synced template row exists for the same key, PUT leaves
    ``github_template_id`` NULL — the sync worker owns that linkage.
    """
    await _setup_org(client)

    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            tstore = DashboardGitHubTemplateStore(
                session=session, logger=logger
            )
            await tstore.upsert(
                key=GitHubTemplateKey(
                    github_owner="lsst-sqre",
                    github_repo="docverse-templates",
                    github_ref="main",
                    root_path="/",
                ),
                commit_sha="deadbeef",
                etag='W/"etag1"',
                template_toml=b"[meta]\nname='t'\n",
                files=[
                    GitHubTemplateFileInput(
                        relative_path="template.toml",
                        is_text=True,
                        data=b"[meta]\nname='t'\n",
                    )
                ],
            )
            await session.commit()
        break

    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201

    async for session in db_session_dependency():
        async with session.begin():
            ostore = OrganizationStore(session=session, logger=logger)
            org = await ostore.get_by_slug(_ORG)
            assert org is not None
            bstore = DashboardGitHubTemplateBindingStore(
                session=session, logger=logger
            )
            binding = await bstore.get_org_default(org.id)
            assert binding is not None
            assert binding.github_template_id is None
            assert binding.last_sync_status == "pending"
        break


# ---------------------------------------------------------------------------
# Payload validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_owner",
    [
        "../evil",
        "foo/bar",
        "-foo",
        "foo--",
        "",
        "a" * 40,
    ],
)
async def test_put_rejects_invalid_github_owner(
    client: AsyncClient, invalid_owner: str
) -> None:
    await _setup_org(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json={**_VALID_BODY, "github_owner": invalid_owner},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "valid_owner",
    ["lsst-sqre", "Org1", "a"],
)
async def test_put_accepts_valid_github_owner(
    client: AsyncClient, valid_owner: str
) -> None:
    await _setup_org(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json={**_VALID_BODY, "github_owner": valid_owner},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201
    assert response.json()["github_owner"] == valid_owner


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_repo",
    [
        "../evil",
        "foo/bar",
        "foo bar",
        "",
        "a" * 101,
    ],
)
async def test_put_rejects_invalid_github_repo(
    client: AsyncClient, invalid_repo: str
) -> None:
    await _setup_org(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json={**_VALID_BODY, "github_repo": invalid_repo},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "valid_repo",
    ["docverse-templates", "my.repo", "_repo"],
)
async def test_put_accepts_valid_github_repo(
    client: AsyncClient, valid_repo: str
) -> None:
    await _setup_org(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json={**_VALID_BODY, "github_repo": valid_repo},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201
    assert response.json()["github_repo"] == valid_repo


# ---------------------------------------------------------------------------
# Project-override binding
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_project_put_creates_binding(client: AsyncClient) -> None:
    await _setup_org_and_project(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["github_owner"] == "lsst-sqre"
    assert body["root_path"] == "/"
    assert body["last_sync_status"] == "pending"


@pytest.mark.asyncio
async def test_project_get_returns_existing_binding(
    client: AsyncClient,
) -> None:
    await _setup_org_and_project(client)
    await client.put(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    response = await client.get(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["github_ref"] == "main"


@pytest.mark.asyncio
async def test_project_get_404_when_unset(client: AsyncClient) -> None:
    await _setup_org_and_project(client)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_project_delete_removes_binding(client: AsyncClient) -> None:
    await _setup_org_and_project(client)
    await client.put(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    response = await client.delete(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 204

    follow_up = await client.get(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert follow_up.status_code == 404


@pytest.mark.asyncio
async def test_project_put_is_idempotent_no_op(client: AsyncClient) -> None:
    await _setup_org_and_project(client)
    first = await client.put(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert first.status_code == 201
    first_body = first.json()

    second = await client.put(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["date_updated"] == first_body["date_updated"]
    assert second_body["date_created"] == first_body["date_created"]


@pytest.mark.asyncio
async def test_project_put_unknown_project_returns_404(
    client: AsyncClient,
) -> None:
    """PUT on a non-existent project must not leave a dangling binding."""
    await _setup_org(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/projects/no-such-proj/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404

    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            ostore = OrganizationStore(session=session, logger=logger)
            org = await ostore.get_by_slug(_ORG)
            assert org is not None
            bstore = DashboardGitHubTemplateBindingStore(
                session=session, logger=logger
            )
            default = await bstore.get_org_default(org.id)
            assert default is None
        break


@pytest.mark.asyncio
async def test_project_delete_unknown_project_returns_404(
    client: AsyncClient,
) -> None:
    await _setup_org(client)
    response = await client.delete(
        f"/docverse/orgs/{_ORG}/projects/no-such-proj/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_project_put_unauthenticated_returns_403(
    client: AsyncClient,
) -> None:
    await _setup_org_and_project(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        json=_VALID_BODY,
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_project_put_non_admin_returns_403(client: AsyncClient) -> None:
    await _setup_org_and_project(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_org_default_and_project_override_are_independent(
    client: AsyncClient,
) -> None:
    """Setting org default and project override are independent rows."""
    await _setup_org_and_project(client)
    await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    project_body = {**_VALID_BODY, "github_ref": "project-branch"}
    await client.put(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        json=project_body,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    org_response = await client.get(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    project_response = await client.get(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert org_response.json()["github_ref"] == "main"
    assert project_response.json()["github_ref"] == "project-branch"

    # Deleting the project override leaves the org default in place.
    await client.delete(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    org_after = await client.get(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert org_after.status_code == 200
    assert org_after.json()["github_ref"] == "main"


def _dashboard_sync_enqueues(mock_arq: MockArqQueue) -> list[JobMetadata]:
    """Return every ``dashboard_sync`` job recorded on the mock queue."""
    return [
        j
        for queue in mock_arq._job_metadata.values()
        for j in queue.values()
        if j.name == "dashboard_sync"
    ]


@pytest.mark.asyncio
async def test_org_put_enqueues_dashboard_sync_on_create(
    client: AsyncClient,
) -> None:
    """Creating an org-default binding enqueues an initial sync."""
    await _setup_org(client)
    mock_arq: MockArqQueue = arq_dependency._arq_queue  # type: ignore[assignment]
    before = len(_dashboard_sync_enqueues(mock_arq))

    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201

    enqueues = _dashboard_sync_enqueues(mock_arq)
    assert len(enqueues) == before + 1

    # Verify the enqueued job points at the binding the PUT created.
    binding_id: int | None = None
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(
                session=session, logger=structlog.get_logger("test")
            )
            org = await org_store.get_by_slug(_ORG)
            assert org is not None
            binding_store = DashboardGitHubTemplateBindingStore(
                session=session, logger=structlog.get_logger("test")
            )
            binding = await binding_store.get_org_default(org.id)
            assert binding is not None
            binding_id = binding.id
    assert binding_id is not None
    payload = enqueues[-1].kwargs["payload"]
    assert payload["binding_id"] == binding_id


@pytest.mark.asyncio
async def test_org_put_idempotent_noop_does_not_enqueue_dashboard_sync(
    client: AsyncClient,
) -> None:
    """A PUT that writes no changes must not enqueue a new sync."""
    await _setup_org(client)
    await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    mock_arq: MockArqQueue = arq_dependency._arq_queue  # type: ignore[assignment]
    before = len(_dashboard_sync_enqueues(mock_arq))

    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200

    assert len(_dashboard_sync_enqueues(mock_arq)) == before


@pytest.mark.asyncio
async def test_project_put_enqueues_dashboard_sync(
    client: AsyncClient,
) -> None:
    """Creating a project override enqueues an initial sync."""
    await _setup_org_and_project(client)
    mock_arq: MockArqQueue = arq_dependency._arq_queue  # type: ignore[assignment]
    before = len(_dashboard_sync_enqueues(mock_arq))

    response = await client.put(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201

    enqueues = _dashboard_sync_enqueues(mock_arq)
    assert len(enqueues) == before + 1
