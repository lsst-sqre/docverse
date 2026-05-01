"""Tests for the super-admin ``POST /admin/dashboard-templates/{id}/sync``."""

from __future__ import annotations

import pytest
import structlog
from httpx import AsyncClient
from safir.arq import JobMetadata, MockArqQueue
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency

from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.organization_store import OrganizationStore
from tests.conftest import seed_org_with_admin
from tests.support.arq_testing import get_jobs_by_name

_SUPERADMIN = "superadmin"
_ADMIN = "admin-user"
_USER = "someone-else"
_ORG = "admin-sync-org"

_BINDING_BODY = {
    "github_owner": "lsst-sqre",
    "github_repo": "docverse-templates",
    "github_ref": "main",
    "root_path": "/",
}


async def _create_binding(client: AsyncClient) -> int:
    """Create an org-default binding via the name-keyed PUT; return its id."""
    await seed_org_with_admin(client, _ORG, _ADMIN)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/dashboard-template",
        json=_BINDING_BODY,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201

    logger = structlog.get_logger("test")
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.get_by_slug(_ORG)
            assert org is not None
            binding_store = DashboardGitHubTemplateBindingStore(
                session=session, logger=logger
            )
            binding = await binding_store.get_org_default(org.id)
            assert binding is not None
            return binding.id
    msg = "db_session_dependency yielded nothing"
    raise AssertionError(msg)


def _dashboard_sync_enqueues(mock_arq: MockArqQueue) -> list[JobMetadata]:
    return get_jobs_by_name(mock_arq, "dashboard_sync")


@pytest.mark.asyncio
async def test_admin_sync_enqueues_dashboard_sync(
    client: AsyncClient,
) -> None:
    """Super-admin POST enqueues a ``dashboard_sync`` for the given binding."""
    binding_id = await _create_binding(client)
    mock_arq: MockArqQueue = arq_dependency._arq_queue  # type: ignore[assignment]
    before = len(_dashboard_sync_enqueues(mock_arq))

    response = await client.post(
        f"/docverse/admin/dashboard-templates/{binding_id}/sync",
        headers={"X-Auth-Request-User": _SUPERADMIN},
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
async def test_admin_sync_unknown_binding_returns_404(
    client: AsyncClient,
) -> None:
    """A missing binding id surfaces a 404, not 500."""
    response = await client.post(
        "/docverse/admin/dashboard-templates/999999/sync",
        headers={"X-Auth-Request-User": _SUPERADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_admin_sync_rejects_unauthenticated(client: AsyncClient) -> None:
    """Callers without an auth header are rejected (401 or 403)."""
    binding_id = await _create_binding(client)
    response = await client.post(
        f"/docverse/admin/dashboard-templates/{binding_id}/sync",
    )
    # The router-level ``require_superadmin`` dep raises 403 for missing
    # or non-superadmin users alike — either code satisfies "rejected".
    assert response.status_code in {401, 403}


@pytest.mark.asyncio
async def test_admin_sync_rejects_non_superadmin(client: AsyncClient) -> None:
    """Ordinary users cannot force-sync; the endpoint is super-admin only."""
    binding_id = await _create_binding(client)
    response = await client.post(
        f"/docverse/admin/dashboard-templates/{binding_id}/sync",
        headers={"X-Auth-Request-User": _USER},
    )
    assert response.status_code == 403
