"""Integration tests for the project_github_resolve enqueue hooks.

POST and PATCH on a project with a ``github`` sub-object must enqueue
one ``project_github_resolve`` arq job (PRD #346 user story 12 /
acceptance criterion 1). Projects without a GitHub binding must not
generate a no-op enqueue: the worker would just skip them, and the
queue noise would obscure real enqueues.

PRD #419 routes the resolve onto the dedicated ``maintenance`` pool
rather than the default publishing queue, so it never contends with the
live publishing flow; the POST test pins that queue target explicitly.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency

from docverse_server.config import Configuration
from docverse_server.worker.queues import MAINTENANCE_QUEUE_NAME
from tests.conftest import seed_org_with_admin
from tests.support.arq_testing import count_jobs_by_name, get_jobs_by_name

_config = Configuration()


async def _setup(client: AsyncClient) -> None:
    """Create an org and seed an admin membership."""
    await seed_org_with_admin(client, "pgr-org", "testuser")


def _resolve_count(*, queue_name: str | None = None) -> int:
    """Count enqueued ``project_github_resolve`` arq jobs.

    With ``queue_name`` given, restrict the count to that one queue;
    with ``None`` (the default) union the count across every queue the
    mock has touched (see :func:`count_jobs_by_name`).
    """
    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    return count_jobs_by_name(
        mock_arq, "project_github_resolve", queue_name=queue_name
    )


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
    """POST with ``github`` enqueues one resolve onto the maintenance queue.

    Reproduces the post-create steady state of user story 12: an admin
    creates a project with structured GitHub coordinates, and the
    handler fires off the asynchronous installation-id resolution so
    the operator does not have to know the installation id at create
    time. PRD #419 routes that resolve onto the dedicated maintenance
    pool, so this pins the enqueue to ``docverse:maintenance-queue`` and
    asserts it does not land on the default publishing queue.
    """
    await _setup(client)
    before_maintenance = _resolve_count(queue_name=MAINTENANCE_QUEUE_NAME)
    before_default = _resolve_count(queue_name=_config.arq_queue_name)

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

    assert (
        _resolve_count(queue_name=MAINTENANCE_QUEUE_NAME) - before_maintenance
        == 1
    )
    assert (
        _resolve_count(queue_name=_config.arq_queue_name) - before_default == 0
    )


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
async def test_patch_project_title_only_does_not_enqueue_resolve(
    client: AsyncClient,
) -> None:
    """A metadata-only PATCH of a bound project enqueues nothing.

    Task #651: the PATCH handler used to enqueue a resolve on every
    edit of a bound project, so retitling 100 projects queued 100 jobs
    whose only effect was to re-read the ids already stored — and, back
    when that write stamped the clock unconditionally, to hand every
    poller a second 200 with an unchanged body. The binding did not
    move, so there is nothing to resolve.
    """
    await _setup(client)
    await client.post(
        "/docverse/orgs/pgr-org/projects",
        json={
            "slug": "gh-retitle",
            "title": "GH Retitle",
            "github": {"owner": "lsst", "repo": "gh-retitle"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    before = _resolve_count()

    patch = await client.patch(
        "/docverse/orgs/pgr-org/projects/gh-retitle",
        json={"title": "GH Retitled"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert patch.status_code == 200

    after = _resolve_count()
    assert after - before == 0


@pytest.mark.asyncio
async def test_patch_project_source_url_null_does_not_enqueue_resolve(
    client: AsyncClient,
) -> None:
    """``source_url: null`` on a bound project enqueues nothing.

    ``ProjectService._resolve_github_for_update`` treats an explicit
    ``source_url: null`` as a no-op for the binding — the derived URL
    already comes from ``github`` — so the gate reads it as a metadata
    edit, not a rebind.
    """
    await _setup(client)
    await client.post(
        "/docverse/orgs/pgr-org/projects",
        json={
            "slug": "gh-clear-url",
            "title": "GH Clear URL",
            "github": {"owner": "lsst", "repo": "gh-clear-url"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    before = _resolve_count()

    patch = await client.patch(
        "/docverse/orgs/pgr-org/projects/gh-clear-url",
        json={"source_url": None},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert patch.status_code == 200

    after = _resolve_count()
    assert after - before == 0


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
