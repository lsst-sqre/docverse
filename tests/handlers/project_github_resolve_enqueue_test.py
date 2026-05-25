"""Integration tests for the project_github_resolve enqueue hooks.

POST and PATCH on a project with a ``github`` sub-object must enqueue
one ``project_github_resolve`` arq job (PRD #346 user story 12 /
acceptance criterion 1). Projects without a GitHub binding must not
generate a no-op enqueue: the worker would just skip them, and the
queue noise would obscure real enqueues.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency

from tests.conftest import seed_org_with_admin
from tests.support.arq_testing import count_jobs_by_name, get_jobs_by_name


async def _setup(client: AsyncClient) -> None:
    """Create an org and seed an admin membership."""
    await seed_org_with_admin(client, "pgr-org", "testuser")


def _resolve_count() -> int:
    """Count enqueued ``project_github_resolve`` arq jobs."""
    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    return count_jobs_by_name(mock_arq, "project_github_resolve")


def _resolve_payloads() -> list[dict[str, object]]:
    """Return every ``project_github_resolve`` job's payload dict.

    ``JobMetadata.kwargs`` carries the keyword arguments passed to
    ``arq.enqueue_job``; ``ArqQueueBackend.enqueue`` wraps the worker's
    payload under the ``payload`` key, so we unwrap one level here so
    callers can assert on the ``project_id`` directly.
    """
    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    return [
        job.kwargs["payload"]
        for job in get_jobs_by_name(mock_arq, "project_github_resolve")
    ]


@pytest.mark.asyncio
async def test_post_project_with_github_enqueues_resolve(
    client: AsyncClient,
) -> None:
    """POST with ``github`` enqueues one ``project_github_resolve`` job.

    Reproduces the post-create steady state of user story 12: an admin
    creates a project with structured GitHub coordinates, and the
    handler fires off the asynchronous installation-id resolution so
    the operator does not have to know the installation id at create
    time.
    """
    await _setup(client)
    before = _resolve_count()

    response = await client.post(
        "/docverse/orgs/pgr-org/projects",
        json={
            "slug": "gh-bound",
            "title": "GitHub Bound",
            "github": {"owner": "lsst", "repo": "docverse"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201

    after = _resolve_count()
    assert after - before == 1


@pytest.mark.asyncio
async def test_post_project_with_github_url_enqueues_resolve(
    client: AsyncClient,
) -> None:
    """POST with a github.com ``source_url`` also enqueues a resolve.

    The handler auto-populates ``github`` from a github.com URL when
    the structured sub-object is omitted (existing behaviour from
    #379); the resolution enqueue follows the populated binding rather
    than the literal request body so this convenience path lands the
    numeric ids too.
    """
    await _setup(client)
    before = _resolve_count()

    response = await client.post(
        "/docverse/orgs/pgr-org/projects",
        json={
            "slug": "gh-from-url",
            "title": "GitHub From URL",
            "source_url": "https://github.com/lsst/from-url",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201

    after = _resolve_count()
    assert after - before == 1


@pytest.mark.asyncio
async def test_post_non_github_project_does_not_enqueue_resolve(
    client: AsyncClient,
) -> None:
    """POST with no github binding does not enqueue a resolve.

    Non-GitHub projects (user story 14) can never benefit from the
    GitHub-App-driven id resolution; enqueueing a job whose only
    outcome is ``"skipped"`` would just clutter the queue and worker
    logs.
    """
    await _setup(client)
    before = _resolve_count()

    response = await client.post(
        "/docverse/orgs/pgr-org/projects",
        json={
            "slug": "non-gh",
            "title": "Non-GitHub",
            "source_url": "https://gitlab.com/lsst/non-github",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201

    after = _resolve_count()
    assert after - before == 0


@pytest.mark.asyncio
async def test_patch_project_with_github_enqueues_resolve(
    client: AsyncClient,
) -> None:
    """PATCH that sets ``github`` enqueues a fresh resolve.

    The numeric id columns were cleared by ``ProjectService._resolve_
    github_for_update`` so the new binding's ids can be re-resolved
    against the new repo (per the prior commit's PATCH-clears-ids
    rule).
    """
    await _setup(client)
    # Create a project without GitHub coordinates first so the PATCH
    # is the only event that should fire a resolve.
    create = await client.post(
        "/docverse/orgs/pgr-org/projects",
        json={
            "slug": "to-gh",
            "title": "To GitHub",
            "source_url": "https://gitlab.com/lsst/to-gh",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert create.status_code == 201
    before = _resolve_count()

    patch = await client.patch(
        "/docverse/orgs/pgr-org/projects/to-gh",
        json={
            "source_url": None,
            "github": {"owner": "lsst", "repo": "to-gh"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert patch.status_code == 200

    after = _resolve_count()
    assert after - before == 1
    payloads = _resolve_payloads()
    project_id = payloads[-1]["project_id"]
    assert isinstance(project_id, int)
    assert project_id > 0


@pytest.mark.asyncio
async def test_patch_project_clearing_github_does_not_enqueue_resolve(
    client: AsyncClient,
) -> None:
    """A PATCH that clears ``github`` (sets it to null) does not enqueue.

    Clearing the binding leaves the project in the non-GitHub state
    (story 14); a resolve would have nothing to resolve and the worker
    would just skip.
    """
    await _setup(client)
    await client.post(
        "/docverse/orgs/pgr-org/projects",
        json={
            "slug": "gh-then-not",
            "title": "GH Then Not",
            "github": {"owner": "lsst", "repo": "gh-then-not"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    before = _resolve_count()

    patch = await client.patch(
        "/docverse/orgs/pgr-org/projects/gh-then-not",
        json={"github": None},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert patch.status_code == 200

    after = _resolve_count()
    assert after - before == 0
