"""Integration tests for the ``project_github_resolve`` worker function."""

from __future__ import annotations

from datetime import datetime

import httpx
import pytest
import sentry_sdk
import structlog
from arq import Retry
from pydantic import SecretStr
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import OrganizationCreate, ProjectCreate
from docverse.models.projects import ProjectGitHubBindingCreate
from docverse_server.dbschema.project import SqlProject
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.worker.functions.project_github_resolve import (
    PROJECT_GITHUB_RESOLVE_MAX_TRIES,
    project_github_resolve,
)
from tests.support.github_mock import GitHubMock
from tests.worker.conftest import make_worker_ctx


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org_and_project(
    db_session: AsyncSession,
    *,
    org_slug: str = "pgr-org",
    project_slug: str = "pgr-proj",
    github_owner: str | None = "acme",
    github_repo: str | None = "templates",
) -> tuple[int, int]:
    """Seed an org plus one project with optional GitHub coordinates."""
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title=f"Org {org_slug}",
            base_domain=f"{org_slug}.example.com",
        )
    )
    binding = (
        ProjectGitHubBindingCreate(owner=github_owner, repo=github_repo)
        if github_owner is not None and github_repo is not None
        else None
    )
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug=project_slug,
            title=f"Project {project_slug}",
            github=binding,
        ),
        github_owner=github_owner,
        github_repo=github_repo,
    )
    return org.id, project.id


async def _fetch_project_github_ids(
    project_id: int,
) -> tuple[str | None, str | None, int | None, int | None, int | None]:
    """Return ``(owner, repo, owner_id, repo_id, installation_id)``."""
    async for session in db_session_dependency():
        result = await session.execute(
            select(
                SqlProject.github_owner,
                SqlProject.github_repo,
                SqlProject.github_owner_id,
                SqlProject.github_repo_id,
                SqlProject.github_installation_id,
            ).where(SqlProject.id == project_id)
        )
        row = result.one()
        return (row[0], row[1], row[2], row[3], row[4])
    msg = "No database session available"
    raise RuntimeError(msg)


async def _fetch_project_date_updated(project_id: int) -> datetime:
    """Return one project's ``date_updated`` clock."""
    async for session in db_session_dependency():
        result = await session.execute(
            select(SqlProject.date_updated).where(SqlProject.id == project_id)
        )
        return result.scalar_one()
    msg = "No database session available"
    raise RuntimeError(msg)


def _make_ctx(
    *,
    http_client: httpx.AsyncClient,
    mock_github: GitHubMock,
) -> dict[str, object]:
    return make_worker_ctx(
        http_client=http_client,
        github_app_id=mock_github.app_id,
        github_app_private_key=SecretStr(mock_github.private_key_pem),
        github_webhook_secret=SecretStr("webhook-secret"),
    )


@pytest.mark.asyncio
async def test_project_github_resolve_persists_three_ids(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A successful resolve writes the three opportunistic id columns.

    Reproduces the post-create steady state from PRD #346 user story 12:
    a project has been created with ``github={owner, repo}`` but the
    three ``github_*_id`` columns are still NULL. The worker fetches
    them from GitHub and persists them so future webhook lookups can
    use the stable numeric keys.
    """
    async with db_session.begin():
        _org_id, project_id = await _seed_org_and_project(db_session)
        await db_session.commit()

    mock_github.seed_installation(
        "acme", "templates", installation_id=42, owner_id=111
    )
    mock_github.seed_repo("acme", "templates", repo_id=12345, owner_id=111)

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        result = await project_github_resolve(ctx, {"project_id": project_id})

    assert result == "completed"
    (
        owner,
        repo,
        owner_id,
        repo_id,
        installation_id,
    ) = await _fetch_project_github_ids(project_id)
    assert owner == "acme"
    assert repo == "templates"
    assert owner_id == 111
    assert repo_id == 12345
    assert installation_id == 42


@pytest.mark.asyncio
async def test_project_github_resolve_rerun_leaves_clock_alone(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A second resolve finding the same ids moves no project clock.

    Task #651: every PATCH of a bound project enqueues this worker
    again, so a resolve that re-reads the ids already stored used to
    stamp ``projects.date_updated`` a second time — a phantom change
    signal that hands every poller a full 200 with an unchanged body.
    The rerun still reports ``completed``: the row does carry the
    resolved ids, which is what ``skipped`` would wrongly deny.
    """
    async with db_session.begin():
        _org_id, project_id = await _seed_org_and_project(db_session)
        await db_session.commit()

    mock_github.seed_installation(
        "acme", "templates", installation_id=42, owner_id=111
    )
    mock_github.seed_repo("acme", "templates", repo_id=12345, owner_id=111)

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        first = await project_github_resolve(ctx, {"project_id": project_id})
        assert first == "completed"
        baseline = await _fetch_project_date_updated(project_id)

        second = await project_github_resolve(ctx, {"project_id": project_id})

    assert second == "completed"
    assert await _fetch_project_date_updated(project_id) == baseline


@pytest.mark.asyncio
async def test_project_github_resolve_skips_non_github_project(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A project with no GitHub binding short-circuits without API calls.

    Pins user story 14: projects whose source_url is non-GitHub never
    populate the structured columns, so a stray enqueue for one must
    not touch the row or open the network. ``mock_github.router``
    captures every HTTP call so the absence is asserted as a count.
    """
    async with db_session.begin():
        _org_id, project_id = await _seed_org_and_project(
            db_session,
            github_owner=None,
            github_repo=None,
        )
        await db_session.commit()

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        result = await project_github_resolve(ctx, {"project_id": project_id})

    assert result == "skipped"
    assert mock_github.router.calls.call_count == 0
    (
        owner,
        repo,
        owner_id,
        repo_id,
        installation_id,
    ) = await _fetch_project_github_ids(project_id)
    assert owner is None
    assert repo is None
    assert owner_id is None
    assert repo_id is None
    assert installation_id is None


@pytest.mark.asyncio
async def test_project_github_resolve_leaves_ids_null_on_github_404(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 404 is the expected "App not installed" state, not a failure.

    The GitHub App installation id is per-account, found via a repo the
    App can access — so a 404 from ``/repos/{owner}/{repo}/installation``
    means no installation grants the App access to this repo. That is an
    operator-recoverable state the ``installation`` webhook backfills, so
    the worker returns ``"not_installed"``, leaves the ids NULL, and must
    **not** page Sentry (which it did before this change). It also must
    not raise out of the worker — that would surface as an arq-level
    error rather than a per-row data-gap log line.
    """
    async with db_session.begin():
        _org_id, project_id = await _seed_org_and_project(db_session)
        await db_session.commit()

    mock_github.router.get(
        "https://api.github.com/repos/acme/templates/installation"
    ).mock(return_value=httpx.Response(404, json={"message": "Not Found"}))

    captured: list[BaseException] = []
    # The worker calls ``sentry_sdk.capture_exception``; patch the
    # module attribute (the string-path form fails because
    # ``project_github_resolve`` is a module, not a package).
    monkeypatch.setattr(sentry_sdk, "capture_exception", captured.append)

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        result = await project_github_resolve(ctx, {"project_id": project_id})

    assert result == "not_installed"
    assert captured == []
    (
        owner,
        repo,
        owner_id,
        repo_id,
        installation_id,
    ) = await _fetch_project_github_ids(project_id)
    assert owner == "acme"
    assert repo == "templates"
    assert owner_id is None
    assert repo_id is None
    assert installation_id is None


@pytest.mark.asyncio
async def test_project_github_resolve_captures_genuine_github_error(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The attempt that exhausts the budget fails and pages Sentry.

    Only a 404 is the expected "App not installed" state. Any other
    GitHub error (here a 500) is a failure — but task #656 makes the
    transient ones non-terminal until the retry budget runs out, so the
    terminal verdict is asserted on the *last* attempt: the worker
    leaves the ids NULL, returns ``"failed"``, and calls
    ``sentry_sdk.capture_exception`` exactly once so an operator is
    paged for the outcome rather than for each attempt.
    """
    async with db_session.begin():
        _org_id, project_id = await _seed_org_and_project(db_session)
        await db_session.commit()

    mock_github.router.get(
        "https://api.github.com/repos/acme/templates/installation"
    ).mock(return_value=httpx.Response(500, json={"message": "Server Error"}))

    captured: list[BaseException] = []
    # The worker calls ``sentry_sdk.capture_exception``; patch the
    # module attribute (the string-path form fails because
    # ``project_github_resolve`` is a module, not a package).
    monkeypatch.setattr(sentry_sdk, "capture_exception", captured.append)

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        ctx["job_try"] = PROJECT_GITHUB_RESOLVE_MAX_TRIES
        result = await project_github_resolve(ctx, {"project_id": project_id})

    assert result == "failed"
    assert len(captured) == 1
    (
        _owner,
        _repo,
        owner_id,
        repo_id,
        installation_id,
    ) = await _fetch_project_github_ids(project_id)
    assert owner_id is None
    assert repo_id is None
    assert installation_id is None


@pytest.mark.asyncio
async def test_project_github_resolve_skips_missing_project(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A payload for a non-existent project id is a clean skip.

    A project can be soft-deleted between enqueue and dequeue. The
    worker must not raise (so arq does not retry the job five times)
    and must not write to a row that does not exist.
    """
    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        result = await project_github_resolve(ctx, {"project_id": 999999})

    assert result == "skipped"
    assert mock_github.router.calls.call_count == 0


@pytest.mark.asyncio
async def test_project_github_resolve_retries_transient_github_error(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A GitHub 503 defers the job instead of burning the only attempt.

    Task #656: #651 gated the ``patch_project`` enqueue on an actual
    binding change, which removed the accidental self-heal a later
    PATCH used to provide. A transient GitHub outage must therefore be
    non-terminal inside the worker: the job raises :class:`arq.Retry`
    with a backoff ``defer`` so arq re-runs it, and it stays out of
    Sentry — nothing is broken yet, and paging on every attempt would
    turn one outage into a storm.
    """
    async with db_session.begin():
        _org_id, project_id = await _seed_org_and_project(db_session)
        await db_session.commit()

    mock_github.router.get(
        "https://api.github.com/repos/acme/templates/installation"
    ).mock(
        return_value=httpx.Response(
            503, json={"message": "Service Unavailable"}
        )
    )

    captured: list[BaseException] = []
    monkeypatch.setattr(sentry_sdk, "capture_exception", captured.append)

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        ctx["job_try"] = 1
        with pytest.raises(Retry) as exc_info:
            await project_github_resolve(ctx, {"project_id": project_id})

    assert exc_info.value.defer_score is not None
    assert exc_info.value.defer_score > 0
    assert captured == []


@pytest.mark.asyncio
async def test_project_github_resolve_backoff_grows_between_attempts(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """Each deferred attempt waits longer than the one before it.

    A fixed short delay would hammer a struggling GitHub for the whole
    budget and re-trip the secondary rate limits that caused the
    failure; the exponential spread is what makes four attempts cover
    an outage of minutes rather than seconds.
    """
    async with db_session.begin():
        _org_id, project_id = await _seed_org_and_project(db_session)
        await db_session.commit()

    mock_github.router.get(
        "https://api.github.com/repos/acme/templates/installation"
    ).mock(
        return_value=httpx.Response(
            503, json={"message": "Service Unavailable"}
        )
    )

    defers: list[int | None] = []
    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        for job_try in (1, 2):
            ctx["job_try"] = job_try
            with pytest.raises(Retry) as exc_info:
                await project_github_resolve(ctx, {"project_id": project_id})
            defers.append(exc_info.value.defer_score)

    first, second = defers
    assert first is not None
    assert second is not None
    assert second > first


@pytest.mark.asyncio
async def test_project_github_resolve_retries_connection_failure(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A transport failure defers too, not just an HTTP status.

    The resolve's two legs reach GitHub through the shared
    ``httpx.AsyncClient``, so a DNS blip or a dropped connection
    surfaces as an ``httpx`` transport error with no response behind
    it. That is the same "GitHub could not answer right now" condition
    a 503 is, and must earn the same deferral.
    """
    async with db_session.begin():
        _org_id, project_id = await _seed_org_and_project(db_session)
        await db_session.commit()

    mock_github.router.get(
        "https://api.github.com/repos/acme/templates/installation"
    ).mock(side_effect=httpx.ConnectError("connection refused"))

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        ctx["job_try"] = 1
        with pytest.raises(Retry):
            await project_github_resolve(ctx, {"project_id": project_id})


@pytest.mark.asyncio
async def test_project_github_resolve_does_not_retry_bad_credentials(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 401 is a verdict, so it fails on the first attempt.

    Credentials GitHub has rejected will be rejected again in a minute
    and in ten, so spending the retry budget on them only delays the
    Sentry event an operator needs to see. The worker pages and returns
    ``"failed"`` straight away, on the first of its four attempts.
    """
    async with db_session.begin():
        _org_id, project_id = await _seed_org_and_project(db_session)
        await db_session.commit()

    route = mock_github.router.get(
        "https://api.github.com/repos/acme/templates/installation"
    ).mock(
        return_value=httpx.Response(401, json={"message": "Bad credentials"})
    )

    captured: list[BaseException] = []
    monkeypatch.setattr(sentry_sdk, "capture_exception", captured.append)

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        ctx["job_try"] = 1
        result = await project_github_resolve(ctx, {"project_id": project_id})

    assert result == "failed"
    assert len(captured) == 1
    assert route.call_count == 1
