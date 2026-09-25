"""Project GitHub binding resolver worker function.

Resolves a project's GitHub App installation id, the numeric owner /
repo ids, and the repository's default branch (PRD #721)
opportunistically, after a project create or update that supplied a
``github`` sub-object. A terminal failure (no installation, a
credential GitHub will keep rejecting, etc.) is logged but never
re-raised — the columns stay NULL and a later install of the
GitHub App will backfill them through the ``installation`` webhook
(PRD #346 user stories 12 / 13).

A *transient* failure is different, and task #656 is why. Before #651
the ``patch_project`` handler re-enqueued this job on every PATCH, so
any later edit of the project accidentally healed a resolve that a
GitHub outage had dropped. #651 gated that enqueue on an actual
binding change — correct, because a no-op PATCH should not re-run the
resolve — which left the outage case with no self-heal at all: the
columns stay NULL, no periodic job backfills them, and the API reports
``installation_status: "not_installed"`` for a repo the App *is*
installed on. So retrying now lives here, where it belongs: a
transient error raises :class:`arq.Retry` and arq re-runs the job after
a backoff, up to :data:`PROJECT_GITHUB_RESOLVE_MAX_TRIES` attempts,
after which the last attempt records the same terminal failure this
function has always recorded.
"""

from __future__ import annotations

from typing import Any

import gidgethub
import httpx
import jwt.exceptions
import sentry_sdk
import structlog
from arq import Retry
from safir.dependencies.db_session import db_session_dependency

from docverse_server.storage._http_retry import (
    RETRYABLE_STATUS_CODES,
    RETRYABLE_TRANSPORT_ERRORS,
)
from docverse_server.storage.github import GitHubAppNotInstalledError

__all__ = [
    "PROJECT_GITHUB_RESOLVE_MAX_TRIES",
    "RETRY_BASE_DEFER_SECONDS",
    "RETRY_MAX_DEFER_SECONDS",
    "project_github_resolve",
]


PROJECT_GITHUB_RESOLVE_MAX_TRIES = 4
"""Attempts one resolve job gets, *including* the first.

Matches `docverse_server.storage._http_retry.DEFAULT_MAX_ATTEMPTS`, the
budget every in-process third-party HTTP retry already uses. With
:data:`RETRY_BASE_DEFER_SECONDS` doubling between attempts, four tries
ride out roughly seven minutes of GitHub unavailability — long enough
for a typical incident, short enough that a genuinely broken binding
reaches its ``"failed"`` verdict (and Sentry) the same working day.

``worker.main.MaintenanceWorkerSettings`` registers the function with
this value so the policy the code implements and the policy arq
enforces cannot drift.
"""

RETRY_BASE_DEFER_SECONDS = 60.0
"""Delay before the second attempt; doubles for each attempt after it.

An order of magnitude longer than the in-process
`~docverse_server.storage._http_retry` backoffs because nothing waits
on this job: it holds no transaction, no lock, and no request, so the
cost of waiting is only how stale ``installation_status`` reads in the
meantime. A minute also clears the common GitHub rate-limit and
secondary-limit windows, which sub-second retries merely re-trip.
"""

RETRY_MAX_DEFER_SECONDS = 600.0
"""Ceiling on any single backoff, in seconds."""


def _is_transient(exc: Exception) -> bool:
    """Report whether another attempt could plausibly succeed.

    Transient means "GitHub could not answer right now": a connection
    that never completed or timed out, a 5xx, or a rate limit. Those
    clear on their own, so the job earns a retry.

    Everything else is a verdict a retry cannot change — a private key
    that will not parse, a 401 from credentials GitHub has rejected, a
    403 for a permission the App does not hold — and must reach its
    terminal ``"failed"`` (and Sentry) on the first attempt rather than
    after four.

    Parameters
    ----------
    exc
        The exception raised by the resolve.

    Returns
    -------
    bool
        `True` when the error is worth another attempt.
    """
    if isinstance(exc, gidgethub.RateLimitExceeded):
        # A 403 carrying an exhausted rate limit, which the generic
        # status check below would (rightly, for every other 403)
        # refuse to retry.
        return True
    if isinstance(exc, gidgethub.HTTPException):
        return exc.status_code in RETRYABLE_STATUS_CODES
    if isinstance(exc, httpx.HTTPStatusError):
        # ``resolve_repository_metadata`` calls ``raise_for_status()``
        # on the plain-httpx ``GET /repos/{owner}/{repo}`` leg, so its
        # failures arrive as httpx rather than gidgethub errors.
        return exc.response.status_code in RETRYABLE_STATUS_CODES
    return isinstance(exc, RETRYABLE_TRANSPORT_ERRORS)


def _retry_defer_seconds(job_try: int) -> float:
    """Seconds to wait before re-running the job after ``job_try`` failed.

    Parameters
    ----------
    job_try
        The 1-based attempt that just failed, as arq reports it in
        ``ctx["job_try"]``.

    Returns
    -------
    float
        Exponential backoff from :data:`RETRY_BASE_DEFER_SECONDS`,
        clamped to :data:`RETRY_MAX_DEFER_SECONDS`.
    """
    multiplier: int = 2 ** (max(job_try, 1) - 1)
    return min(RETRY_MAX_DEFER_SECONDS, RETRY_BASE_DEFER_SECONDS * multiplier)


async def project_github_resolve(
    ctx: dict[str, Any], payload: dict[str, Any]
) -> str:
    """Resolve and persist a project's opportunistic GitHub metadata.

    Writes the three ``github_*_id`` columns and
    ``github_default_branch``. Each write is a no-op when GitHub reports
    what the row already holds, so a re-resolve leaves the project's
    clock alone.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``, ``http_client``,
        ``arq_queue``, and arq's own ``job_try``).
    payload
        Job payload with ``project_id``.

    Returns
    -------
    str
        ``"completed"`` on a successful resolve, ``"skipped"`` when the
        project has no GitHub binding (or has been deleted),
        ``"not_installed"`` when the GitHub App is not installed on the
        repository (an expected, operator-recoverable state — the ids
        stay NULL and the ``installation`` webhook backfills them once
        the App is installed), or ``"failed"`` when GitHub returned a
        genuine error or the columns could not be written.

    Raises
    ------
    arq.Retry
        When GitHub could not answer *this* attempt (transport failure,
        5xx, rate limit) and attempts remain within
        :data:`PROJECT_GITHUB_RESOLVE_MAX_TRIES`. The last attempt
        returns ``"failed"`` instead, so the terminal outcome is
        recorded exactly as it was before retries existed.
    """
    project_id: int = payload["project_id"]
    logger = structlog.get_logger(
        "docverse_server.worker.project_github_resolve"
    ).bind(project_id=project_id)

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        project_store = factory.create_project_store()

        async with session.begin():
            project = await project_store.get_by_id(project_id)
        if project is None:
            logger.info("Skipping resolve: project not found")
            return "skipped"

        owner = project.github_owner
        repo = project.github_repo
        if owner is None or repo is None:
            logger.info("Skipping resolve: project has no GitHub binding")
            return "skipped"

        logger = logger.bind(github_owner=owner, github_repo=repo)

        try:
            app_client = factory.create_github_app_client()
            metadata = await app_client.resolve_repository_metadata(
                owner=owner, repo=repo
            )
        except GitHubAppNotInstalledError:
            # Expected state, not a bug: no installation grants the App
            # access to this repo (not installed on the account,
            # installed without this repo selected, or a mistyped URL).
            # Leave the ids NULL and stay out of Sentry — the
            # ``installation`` webhook backfills them once an operator
            # installs the App. The API surfaces this as
            # ``installation_status: "not_installed"``.
            logger.info(
                "GitHub App not installed on repository; leaving ids NULL"
            )
            return "not_installed"
        except (
            httpx.HTTPError,
            gidgethub.GitHubException,
            jwt.exceptions.InvalidKeyError,
        ) as exc:
            job_try: int = ctx.get("job_try", 1)
            if (
                _is_transient(exc)
                and job_try < PROJECT_GITHUB_RESOLVE_MAX_TRIES
            ):
                defer = _retry_defer_seconds(job_try)
                # Deliberately no Sentry capture: nothing is broken
                # yet, and paging once per attempt would turn a single
                # GitHub incident into a burst of issues. The attempt
                # that exhausts the budget falls through to the
                # terminal branch below and pages exactly once.
                logger.info(
                    "Deferring project GitHub resolve after transient error",
                    error=str(exc),
                    error_type=type(exc).__name__,
                    job_try=job_try,
                    max_tries=PROJECT_GITHUB_RESOLVE_MAX_TRIES,
                    defer_seconds=defer,
                )
                raise Retry(defer=defer) from exc
            sentry_sdk.capture_exception(exc)
            logger.warning(
                "Failed to resolve project GitHub metadata",
                error=str(exc),
                error_type=type(exc).__name__,
                job_try=job_try,
            )
            return "failed"

        default_branch_changed = False
        async with session.begin():
            updated = await project_store.update_github_metadata(
                project_id=project_id,
                expected_owner=owner,
                expected_repo=repo,
                installation_id=metadata.installation_id,
                owner_id=metadata.owner_id,
                repo_id=metadata.repo_id,
            )
            # The default branch shares the ids' binding guard: when the
            # binding moved to another repo mid-resolve, this repo's
            # branch is as stale as its ids, so neither is written.
            if updated:
                default_branch_changed = (
                    await project_store.set_github_default_branch(
                        project_id=project_id, value=metadata.default_branch
                    )
                )
            await session.commit()

        if not updated:
            logger.info(
                "Skipping persist: project binding changed during resolve"
            )
            return "skipped"

        logger.info(
            "Resolved project GitHub metadata",
            github_installation_id=metadata.installation_id,
            github_owner_id=metadata.owner_id,
            github_repo_id=metadata.repo_id,
            github_default_branch=metadata.default_branch,
            default_branch_changed=default_branch_changed,
        )
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)
