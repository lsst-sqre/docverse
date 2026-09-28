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
from sqlalchemy.ext.asyncio import AsyncSession

from docverse_server.domain.project import Project
from docverse_server.factory import Factory
from docverse_server.metrics import (
    DocverseEvents,
    EditionLifecycleEvent,
    LifecycleAction,
    MetricsEditionKind,
)
from docverse_server.services.dashboard.enqueue import (
    try_enqueue_dashboard_build_by_slug,
)
from docverse_server.services.default_branch import (
    DefaultBranchOutcome,
    DefaultBranchTrigger,
)
from docverse_server.storage._http_retry import (
    RETRYABLE_STATUS_CODES,
    RETRYABLE_TRANSPORT_ERRORS,
)
from docverse_server.storage.github import (
    GitHubAppNotInstalledError,
    RepositoryNotAccessibleError,
    RepositoryRefFetchError,
)

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

_LIVE_REF_FETCH_ERRORS: tuple[type[Exception], ...] = (
    RepositoryNotAccessibleError,
    RepositoryRefFetchError,
    httpx.HTTPError,
    gidgethub.GitHubException,
    jwt.exceptions.InvalidKeyError,
)
"""What a first-learn live ref fetch may raise and the resolve absorbs.

The fetcher's two typed errors, plus what minting the installation
token for it can raise. The ref set is only extra evidence for the
default-branch rule, so none of these retries or fails the job.
"""


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

    Writes the three ``github_*_id`` columns, then applies the
    repository's default branch through ``DefaultBranchService``
    (PRD #721), which records ``github_default_branch`` and converges a
    ``__main`` still tracking the previous default branch — the one a
    ``PATCH`` rebind cleared, carried in the payload, or else the one
    the column held — or, when the column was ``NULL``, a ref missing
    from the repository's live ref set. Each write is a no-op when
    GitHub reports what the row already holds, so a re-resolve leaves
    the project's clock alone.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``, ``http_client``,
        ``arq_queue``, and arq's own ``job_try``).
    payload
        Job payload with ``project_id`` and, after a ``PATCH`` rebind of
        a project whose default branch was known, the
        ``previous_default_branch`` the rebind cleared.

    Returns
    -------
    str
        ``"completed"`` on a successful resolve; ``"skipped"`` when
        nothing was written because the project has no GitHub binding
        (or has been deleted, or was rebound while GitHub answered);
        ``"metadata_only"`` when the ids were committed but the project
        was rebound or deleted before the default branch could be
        applied (the rebind's own resolve records the new repository's
        branch); ``"not_installed"`` when the GitHub App is not
        installed on the repository (an expected, operator-recoverable
        state — the ids stay NULL and the ``installation`` webhook
        backfills them once the App is installed); or ``"failed"`` when
        GitHub returned a genuine error or the columns could not be
        written.

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
    previous_default_branch: str | None = payload.get(
        "previous_default_branch"
    )
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

        async with session.begin():
            updated = await project_store.update_github_metadata(
                project_id=project_id,
                expected_owner=owner,
                expected_repo=repo,
                installation_id=metadata.installation_id,
                owner_id=metadata.owner_id,
                repo_id=metadata.repo_id,
            )
            await session.commit()

        if not updated:
            logger.info(
                "Skipping persist: project binding changed during resolve"
            )
            return "skipped"

        outcome = await _apply_default_branch(
            ctx=ctx,
            factory=factory,
            session=session,
            project_id=project_id,
            owner=owner,
            repo=repo,
            repo_id=metadata.repo_id,
            default_branch=metadata.default_branch,
            previous_default_branch=previous_default_branch,
            logger=logger,
        )
        if outcome is None:
            # Unlike the pre-commit skip above, the ids *are* in the
            # row's history, so the outcome and the line say what was
            # written as well as what was not.
            logger.info(
                "Recorded GitHub ids but skipped default branch: project "
                "rebound or deleted after the ids were committed",
                github_installation_id=metadata.installation_id,
                github_owner_id=metadata.owner_id,
                github_repo_id=metadata.repo_id,
                github_default_branch=metadata.default_branch,
            )
            return "metadata_only"

        logger.info(
            "Resolved project GitHub metadata",
            github_installation_id=metadata.installation_id,
            github_owner_id=metadata.owner_id,
            github_repo_id=metadata.repo_id,
            github_default_branch=metadata.default_branch,
            default_branch_changed=outcome.column_changed,
            main_rewritten=outcome.main_rewritten,
        )
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _apply_default_branch(
    *,
    ctx: dict[str, Any],
    factory: Factory,
    session: AsyncSession,
    project_id: int,
    owner: str,
    repo: str,
    repo_id: int,
    default_branch: str,
    previous_default_branch: str | None,
    logger: structlog.stdlib.BoundLogger,
) -> DefaultBranchOutcome | None:
    """Converge the project on the default branch the resolve read.

    Routes the write through
    :class:`~docverse_server.services.default_branch.DefaultBranchService`
    (PRD #721) with ``old_default_branch`` set to
    ``previous_default_branch`` — the branch a ``PATCH`` rebind cleared
    from the column, carried in the job payload — or, without one, the
    column's current value, so a binding moved to a repository with a
    different default branch rewrites a ``__main`` still tracking the
    branch recorded for the old one.

    A first learn — the column still ``NULL``, on a fresh project or
    after a rebind cleared it — also fetches the repository's live ref
    set and passes it as the rule's other evidence, as the audit does.
    That is what converges a project created on a ``master`` repository
    with no ``main`` branch, whose ``__main`` got the ``main`` fallback,
    in its own resolve rather than at the next audit tick; a ``__main``
    on a ref that still exists is left alone either way. A failed fetch
    is logged and the branch is recorded without live refs, leaving the
    audit as the backstop. A later resolve spends no requests on the
    ref set: the column's previous value is its evidence.

    A rewritten ``__main`` is announced as a ``PATCH`` of it would be:
    its ``publish_edition`` job is handed to arq, then one
    ``edition_lifecycle`` ``update`` event and one ``dashboard_build``.

    Runs in its own transaction after the ids commit, because the
    service takes ``__main``'s ``EDITION_UPDATE`` advisory lock before
    it writes, and the ids ``UPDATE`` would otherwise hold the project
    row while waiting on it (advisory lock, then rows). The binding is
    re-read first so a project rebound between the two transactions
    does not record the old repository's branch; the rebind enqueued
    its own resolve. See :func:`_still_bound` for what counts as the
    same binding. The live ref fetch runs between that re-read and the
    write transaction, so no transaction is open across the GitHub
    round-trips.

    Returns
    -------
    DefaultBranchOutcome or None
        What the service changed, or ``None`` when the project is gone
        or no longer bound to the repository the resolve read.
    """
    project_store = factory.create_project_store()
    async with session.begin():
        project = await project_store.get_by_id(project_id)
    if project is None or not _still_bound(
        project, owner=owner, repo=repo, repo_id=repo_id
    ):
        return None
    live_refs = None
    if project.github_default_branch is None:
        live_refs = await _fetch_live_refs(
            factory=factory, project_id=project_id, logger=logger
        )

    async with session.begin():
        # Re-read under the write transaction: the binding may have
        # moved during the ref fetch's GitHub round-trips.
        project = await project_store.get_by_id(project_id)
        if project is None or not _still_bound(
            project, owner=owner, repo=repo, repo_id=repo_id
        ):
            return None
        outcome = await factory.create_default_branch_service().apply(
            project=project,
            default_branch=default_branch,
            trigger=DefaultBranchTrigger.resolve,
            old_default_branch=(
                previous_default_branch or project.github_default_branch
            ),
            live_refs=live_refs,
        )
        org = await factory.create_org_store().get_by_id(project.org_id)
        await session.commit()
    await factory.queue_dispatcher.dispatch()
    if not outcome.main_rewritten or org is None:
        return outcome
    events: DocverseEvents | None = ctx.get("events")
    if events is not None:
        await events.edition_lifecycle.publish(
            EditionLifecycleEvent(
                organization=org.slug,
                project=project.slug,
                action=LifecycleAction.update,
                edition_kind=MetricsEditionKind.main,
            )
        )
    await try_enqueue_dashboard_build_by_slug(
        factory=factory,
        session=session,
        logger=logger,
        org_slug=org.slug,
        project_slug=project.slug,
    )
    return outcome


async def _fetch_live_refs(
    *,
    factory: Factory,
    project_id: int,
    logger: structlog.stdlib.BoundLogger,
) -> frozenset[str] | None:
    """Fetch the project's live branches and tags for a first learn.

    Authenticates the way the audit does, through
    :class:`~docverse_server.services.project_github_binding.ProjectGitHubBindingResolver`:
    with the installation token for the id this resolve just committed,
    or anonymously for a project without one. The resolver owns its own
    short read transaction, so the caller must not hold one.

    Returns
    -------
    frozenset of str or None
        The live ref names, or ``None`` when the project is no longer
        bound or GitHub could not list its refs — "not known", which
        leaves ``__main`` alone rather than moving it off a ref that may
        still exist.
    """
    resolver = factory.create_project_github_binding_resolver()
    fetcher = factory.create_github_ref_set_fetcher()
    try:
        binding = await resolver.resolve(project_id)
        if binding is None:
            return None
        ref_set = await fetcher.fetch(
            owner=binding.owner,
            repo=binding.repo,
            auth=binding.auth,
            logger=logger,
        )
    except _LIVE_REF_FETCH_ERRORS as exc:
        logger.warning(
            "Resolve: GitHub ref fetch failed, recording default branch "
            "without live refs",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        return None
    return ref_set.all


def _still_bound(
    project: Project, *, owner: str, repo: str, repo_id: int
) -> bool:
    """Report whether a re-read project is bound to the resolved repo.

    The numeric repository id is the identity: the resolve committed
    ``repo_id`` a moment earlier, and ``repository.renamed`` /
    ``repository.transferred`` match the row on it and keep it while
    they rewrite ``github_owner`` / ``github_repo``. A row still holding
    ``repo_id`` is therefore the same repository under whatever name it
    has now, and its default branch is the one GitHub just reported —
    comparing names would mistake that rename for a rebind and drop the
    branch.

    A ``PATCH`` rebind clears the id, so only a row without one falls
    back to comparing names; a row holding a *different* id was rebound
    and already re-resolved.

    Parameters
    ----------
    project
        The project as re-read in the default-branch transaction.
    owner
        The owner login the resolve read the repository under.
    repo
        The repository name the resolve read it under.
    repo_id
        The numeric repository id the resolve committed.

    Returns
    -------
    bool
        `True` when the default branch the resolve read describes the
        repository the project is bound to now.
    """
    if project.github_repo_id is not None:
        return project.github_repo_id == repo_id
    return (project.github_owner, project.github_repo) == (owner, repo)
