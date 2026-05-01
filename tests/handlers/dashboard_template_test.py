"""Tests for the dashboard-template binding handlers."""

from __future__ import annotations

import pytest
import structlog
from httpx import AsyncClient
from safir.arq import JobMetadata, MockArqQueue
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency

from docverse.client.models import OrgRole
from docverse.services.dashboard_templates.enqueue import DashboardSyncEnqueuer
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
    GitHubTemplateFileInput,
    GitHubTemplateKey,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from tests.conftest import seed_member, seed_org_with_admin
from tests.support.arq_testing import get_jobs_by_name

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
    # ``web_url`` is derived from the binding source coordinates; ``/``
    # root_path collapses to a bare ``/tree/{ref}`` URL with no trailing
    # path segment.
    assert (
        body["web_url"]
        == "https://github.com/lsst-sqre/docverse-templates/tree/main"
    )
    # ``commit_sha`` stays ``None`` until the first successful sync
    # links a template content row.
    assert body["commit_sha"] is None
    # The PUT enqueues an initial sync, so the URL points at that job.
    assert body["last_sync_queue_job_url"] is not None
    assert "/queue/jobs/" in body["last_sync_queue_job_url"]


@pytest.mark.asyncio
async def test_org_put_web_url_includes_subdirectory_root_path(
    client: AsyncClient,
) -> None:
    """A non-``/`` root path appears as a path segment after the ref."""
    await _setup_org(client)
    body = {**_VALID_BODY, "root_path": "/themes/blue"}
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=body,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201
    assert response.json()["web_url"] == (
        "https://github.com/lsst-sqre/docverse-templates/tree/main/themes/blue"
    )


@pytest.mark.asyncio
async def test_org_get_surfaces_commit_sha_after_sync(
    client: AsyncClient,
) -> None:
    """``commit_sha`` flips from ``None`` to the synced commit on success."""
    await _setup_org(client)
    create_response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert create_response.status_code == 201
    assert create_response.json()["commit_sha"] is None

    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            template_store = DashboardGitHubTemplateStore(
                session=session, logger=logger
            )
            upserted = await template_store.upsert(
                key=GitHubTemplateKey(
                    github_owner="lsst-sqre",
                    github_repo="docverse-templates",
                    github_ref="main",
                    root_path="/",
                ),
                commit_sha="cafebabe",
                etag='W/"etag-cafebabe"',
                template_toml=b"[meta]\nname='t'\n",
                files=[
                    GitHubTemplateFileInput(
                        relative_path="template.toml",
                        is_text=True,
                        data=b"[meta]\nname='t'\n",
                    )
                ],
            )
            ostore = OrganizationStore(session=session, logger=logger)
            org = await ostore.get_by_slug(_ORG)
            assert org is not None
            bstore = DashboardGitHubTemplateBindingStore(
                session=session, logger=logger
            )
            existing = await bstore.get_org_default(org.id)
            assert existing is not None
            await bstore.update_sync_state(
                binding_id=existing.id,
                last_sync_status="succeeded",
                github_template_id=upserted.template.id,
            )
            await session.commit()
        break

    response = await client.get(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    assert response.json()["commit_sha"] == "cafebabe"


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
# github_ref normalization (DM-54689)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("submitted_ref", "stored_ref"),
    [
        ("refs/heads/main", "main"),
        ("refs/tags/v1.0", "v1.0"),
    ],
)
async def test_org_put_normalizes_refs_prefix(
    client: AsyncClient, submitted_ref: str, stored_ref: str
) -> None:
    """``refs/heads/`` and ``refs/tags/`` prefixes round-trip as bare refs.

    GitHub push payloads carry fully-qualified refs, but operators
    register bindings with bare names. The validator strips the prefix
    on input so bindings store the canonical bare form regardless of
    which form the operator typed.
    """
    await _setup_org(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json={**_VALID_BODY, "github_ref": submitted_ref},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201
    assert response.json()["github_ref"] == stored_ref

    # GET returns the same canonical form.
    follow_up = await client.get(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert follow_up.status_code == 200
    assert follow_up.json()["github_ref"] == stored_ref


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("first_ref", "second_ref"),
    [
        ("refs/heads/main", "main"),
        ("refs/tags/v1.0", "v1.0"),
    ],
)
async def test_org_put_bare_form_after_prefixed_is_idempotent(
    client: AsyncClient, first_ref: str, second_ref: str
) -> None:
    """A re-PUT with the bare form after a prefixed form is a no-op."""
    await _setup_org(client)
    first = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json={**_VALID_BODY, "github_ref": first_ref},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert first.status_code == 201
    first_body = first.json()

    second = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json={**_VALID_BODY, "github_ref": second_ref},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["github_ref"] == first_body["github_ref"]
    assert second_body["date_updated"] == first_body["date_updated"]


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
    # Project-override response carries ``web_url`` + ``commit_sha`` too.
    assert (
        body["web_url"]
        == "https://github.com/lsst-sqre/docverse-templates/tree/main"
    )
    assert body["commit_sha"] is None


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
    return get_jobs_by_name(mock_arq, "dashboard_sync")


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


@pytest.mark.asyncio
async def test_org_put_response_url_matches_enqueued_queue_job(
    client: AsyncClient,
) -> None:
    """Response ``last_sync_queue_job_url`` ends in the enqueued job's id."""
    await _setup_org(client)
    mock_arq: MockArqQueue = arq_dependency._arq_queue  # type: ignore[assignment]
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201
    enqueues = _dashboard_sync_enqueues(mock_arq)
    queue_job_public_id = enqueues[-1].kwargs["payload"]["queue_job_public_id"]
    body = response.json()
    assert body["last_sync_queue_job_url"] is not None
    assert body["last_sync_queue_job_url"].endswith(
        f"/queue/jobs/{queue_job_public_id}"
    )


@pytest.mark.asyncio
async def test_org_get_returns_same_queue_job_url_as_put(
    client: AsyncClient,
) -> None:
    """GET surfaces the same ``last_sync_queue_job_url`` PUT returned.

    The URL persists across reads until a follow-up sync overwrites it.
    """
    await _setup_org(client)
    put_response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert put_response.status_code == 201
    expected_url = put_response.json()["last_sync_queue_job_url"]
    assert expected_url is not None
    get_response = await client.get(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert get_response.status_code == 200
    assert get_response.json()["last_sync_queue_job_url"] == expected_url


@pytest.mark.asyncio
async def test_org_put_overwrites_queue_job_url_on_resync(
    client: AsyncClient,
) -> None:
    """A change-PUT enqueues a fresh sync; the URL flips to the new job."""
    await _setup_org(client)
    first = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert first.status_code == 201
    first_url = first.json()["last_sync_queue_job_url"]

    second = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json={**_VALID_BODY, "github_ref": "release"},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert second.status_code == 200
    second_url = second.json()["last_sync_queue_job_url"]
    assert second_url is not None
    assert second_url != first_url


@pytest.mark.asyncio
async def test_org_put_enqueue_failure_leaves_url_null(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``try_enqueue_dashboard_sync`` fails, the URL stays ``null``.

    Pairs with ``test_org_put_enqueue_failure_marks_binding_failed``: a
    silently-dropped enqueue must surface ``last_sync_queue_job_url:
    null`` so an operator does not chase a non-existent queue job.
    """
    await _setup_org(client)

    boom_message = "arq down"

    async def _boom(
        self: DashboardSyncEnqueuer,
        binding_id: int,
    ) -> None:
        raise RuntimeError(boom_message)

    monkeypatch.setattr(DashboardSyncEnqueuer, "enqueue", _boom)

    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201
    assert response.json()["last_sync_queue_job_url"] is None


@pytest.mark.asyncio
async def test_org_put_enqueue_failure_marks_binding_failed(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mark the binding ``failed`` when the fire-and-forget enqueue raises.

    The PUT must still succeed, but the binding must reflect the failure
    as ``last_sync_status="failed"`` with a descriptive
    ``last_sync_error`` so operators can detect the stuck row.
    """
    await _setup_org(client)

    boom_message = "arq down"

    async def _boom(
        self: DashboardSyncEnqueuer,
        binding_id: int,
    ) -> None:
        raise RuntimeError(boom_message)

    monkeypatch.setattr(DashboardSyncEnqueuer, "enqueue", _boom)

    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201

    logger = structlog.get_logger("docverse")
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
            assert binding.last_sync_status == "failed"
            assert binding.last_sync_error is not None
            assert "arq down" in binding.last_sync_error
        break


# ---------------------------------------------------------------------------
# Force-sync endpoints (org admin + super-admin)
# ---------------------------------------------------------------------------


_SUPERADMIN = "superadmin"


async def _put_org_binding(client: AsyncClient) -> None:
    """Create the org-default binding the sync tests act on."""
    await _setup_org(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201


async def _put_project_binding(client: AsyncClient) -> None:
    """Create the project-override binding the sync tests act on."""
    await _setup_org_and_project(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template",
        json=_VALID_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201


async def _binding_id(*, project_slug: str | None = None) -> int:
    """Return the binding id for the org-default or project-override row."""
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            ostore = OrganizationStore(session=session, logger=logger)
            org = await ostore.get_by_slug(_ORG)
            assert org is not None
            bstore = DashboardGitHubTemplateBindingStore(
                session=session, logger=logger
            )
            if project_slug is None:
                binding = await bstore.get_org_default(org.id)
            else:
                pstore = ProjectStore(session=session, logger=logger)
                project = await pstore.get_by_slug(
                    org_id=org.id, slug=project_slug
                )
                assert project is not None
                binding = await bstore.get_project_override(
                    org_id=org.id, project_id=project.id
                )
            assert binding is not None
            return binding.id
    msg = "db_session_dependency yielded nothing"
    raise AssertionError(msg)


@pytest.mark.asyncio
async def test_org_sync_enqueues_dashboard_sync(client: AsyncClient) -> None:
    """Org admin POST enqueues a ``dashboard_sync`` for the org default."""
    await _put_org_binding(client)
    binding_id = await _binding_id()
    mock_arq: MockArqQueue = arq_dependency._arq_queue  # type: ignore[assignment]
    before = len(_dashboard_sync_enqueues(mock_arq))

    response = await client.post(
        f"/docverse/orgs/{_ORG}/dashboard-template/sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["binding_id"] == binding_id
    assert body["queue_job_id"]
    assert body["queue_job_url"].endswith(
        f"/queue/jobs/{body['queue_job_id']}"
    )

    enqueues = _dashboard_sync_enqueues(mock_arq)
    assert len(enqueues) == before + 1
    payload = enqueues[-1].kwargs["payload"]
    assert payload["binding_id"] == binding_id


@pytest.mark.asyncio
async def test_org_sync_allows_superadmin(client: AsyncClient) -> None:
    """Super-admin escalation works through the existing role-resolver."""
    await _put_org_binding(client)
    response = await client.post(
        f"/docverse/orgs/{_ORG}/dashboard-template/sync",
        headers={"X-Auth-Request-User": _SUPERADMIN},
    )
    assert response.status_code == 202


@pytest.mark.asyncio
async def test_org_sync_returns_404_when_no_binding(
    client: AsyncClient,
) -> None:
    """Sync on an org with no binding configured surfaces a 404."""
    await _setup_org(client)
    response = await client.post(
        f"/docverse/orgs/{_ORG}/dashboard-template/sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_org_sync_unknown_org_returns_404(client: AsyncClient) -> None:
    """An unknown org slug surfaces a 404 from the auth dependency."""
    response = await client.post(
        "/docverse/orgs/no-such-org/dashboard-template/sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_org_sync_unauthenticated_returns_403(
    client: AsyncClient,
) -> None:
    await _put_org_binding(client)
    response = await client.post(
        f"/docverse/orgs/{_ORG}/dashboard-template/sync",
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_org_sync_non_admin_returns_403(client: AsyncClient) -> None:
    await _put_org_binding(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.post(
        f"/docverse/orgs/{_ORG}/dashboard-template/sync",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_project_sync_enqueues_dashboard_sync(
    client: AsyncClient,
) -> None:
    """Project-override sync enqueues a ``dashboard_sync`` for that binding."""
    await _put_project_binding(client)
    binding_id = await _binding_id(project_slug=_PROJECT)
    mock_arq: MockArqQueue = arq_dependency._arq_queue  # type: ignore[assignment]
    before = len(_dashboard_sync_enqueues(mock_arq))

    response = await client.post(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template/sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["binding_id"] == binding_id
    assert body["queue_job_id"]
    assert body["queue_job_url"].endswith(
        f"/queue/jobs/{body['queue_job_id']}"
    )

    enqueues = _dashboard_sync_enqueues(mock_arq)
    assert len(enqueues) == before + 1
    payload = enqueues[-1].kwargs["payload"]
    assert payload["binding_id"] == binding_id


@pytest.mark.asyncio
async def test_project_sync_allows_superadmin(client: AsyncClient) -> None:
    await _put_project_binding(client)
    response = await client.post(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template/sync",
        headers={"X-Auth-Request-User": _SUPERADMIN},
    )
    assert response.status_code == 202


@pytest.mark.asyncio
async def test_project_sync_returns_404_when_no_binding(
    client: AsyncClient,
) -> None:
    """Sync on a project with no override surfaces a 404."""
    await _setup_org_and_project(client)
    response = await client.post(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template/sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_project_sync_unknown_project_returns_404(
    client: AsyncClient,
) -> None:
    await _setup_org(client)
    response = await client.post(
        f"/docverse/orgs/{_ORG}/projects/no-such-proj/dashboard-template/sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_project_sync_non_admin_returns_403(client: AsyncClient) -> None:
    await _put_project_binding(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.post(
        f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/dashboard-template/sync",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# OpenAPI tag assignment
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dashboard_template_openapi_tags(client: AsyncClient) -> None:
    """Verify dashboard-template endpoints carry the right OpenAPI tags.

    Org-default routes (binding CRUD + sync) are tagged ``orgs``;
    project-override routes (binding CRUD + sync) are tagged
    ``projects``.
    """
    response = await client.get("/docverse/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]

    org_path = paths["/docverse/orgs/{org}/dashboard-template"]
    for method in ("get", "put", "delete"):
        assert org_path[method]["tags"] == ["orgs"], (
            f"Expected org-default {method.upper()} to be tagged 'orgs'"
        )

    org_sync_path = paths["/docverse/orgs/{org}/dashboard-template/sync"]
    assert org_sync_path["post"]["tags"] == ["orgs"], (
        "Expected org-default sync POST to be tagged 'orgs'"
    )

    project_path = paths[
        "/docverse/orgs/{org}/projects/{project}/dashboard-template"
    ]
    for method in ("get", "put", "delete"):
        assert project_path[method]["tags"] == ["projects"], (
            f"Expected project-override {method.upper()} to be tagged "
            f"'projects'"
        )

    project_sync_path = paths[
        "/docverse/orgs/{org}/projects/{project}/dashboard-template/sync"
    ]
    assert project_sync_path["post"]["tags"] == ["projects"], (
        "Expected project-override sync POST to be tagged 'projects'"
    )

    # The legacy admin force-sync route was replaced by the slug-keyed
    # routes above; verify it is no longer registered.
    assert "/docverse/admin/dashboard-templates/{binding_id}/sync" not in paths
