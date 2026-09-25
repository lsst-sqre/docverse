"""arq worker function for the daily ``git_ref_audit`` per-org pass.

The discovery dispatcher (sibling task) writes one
``git_ref_audit_runs`` row per tick, then fans out one ``queue_jobs``
row per in-scope org with ``kind='git_ref_audit'`` and
``subject_label=org.slug`` (mirroring the ``lifecycle_eval`` per-org
worker so an operator inspecting the queue sees a meaningful
subject). This worker is the per-org body of that fan-out: for one
org it lists every non-deleted GitHub-bound project, resolves each
project's GitHub binding, fetches the live ref set and the default
branch against GitHub, converges each project's ``__main`` on its
default branch (PRD #721), runs :func:`evaluate_lifecycle` with
``live_refs`` populated, and soft-deletes the matched editions.

The worker owns the ``queue_jobs`` row lifecycle: it transitions to
``in_progress`` on entry and to ``completed`` /
``completed_with_errors`` / ``failed`` on exit, then calls
:func:`maybe_finalise_git_ref_audit_run` so the parent
``git_ref_audit_runs`` row rolls to its terminal status once every
per-org child is terminal.

A per-project fetch failure — of the ref set or of the default branch —
is caught, logged with org/project/error, and the per-org pass
continues with the next project — one
rate-limited installation cannot block the audit for every other
project. When at least one project failed, the queue-job row
transitions to ``completed_with_errors`` and ``aggregate_activity``
in the finaliser routes the parent run to ``partial_failure``;
otherwise the row transitions to ``completed`` and the parent rolls
to ``succeeded``.
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse_server.domain.edition import Edition
from docverse_server.domain.lifecycle import LifecycleRuleSet, RefDeletedRule
from docverse_server.domain.project import Project
from docverse_server.factory import Factory
from docverse_server.metrics import (
    DocverseEvents,
    EditionLifecycleEvent,
    LifecycleAction,
    LifecycleActionEvent,
    LifecycleActionTrigger,
    LifecycleReapAction,
    MetricsEditionKind,
)
from docverse_server.services.dashboard.enqueue import (
    try_enqueue_dashboard_build_by_slug,
)
from docverse_server.services.default_branch import DefaultBranchTrigger
from docverse_server.services.git_ref_audit_finalisation import (
    maybe_finalise_git_ref_audit_run,
)
from docverse_server.services.lifecycle.evaluator import (
    LifecycleEvaluationContext,
    evaluate_lifecycle,
    filter_rule_set,
    resolve_rule_set,
)
from docverse_server.storage.github import (
    RepositoryNotAccessibleError,
    RepositoryRefFetchError,
    RepositoryRefSet,
)
from docverse_server.storage.keeper_sync import TombstoneReason

__all__ = ["git_ref_audit"]


@dataclass(frozen=True, slots=True)
class _AuditSummary:
    """What one org's pass did, for the queue-job row and the summary log."""

    had_failures: bool = False
    """Whether any project's GitHub read failed this pass."""

    default_branch_updates: int = 0
    """Projects whose ``github_default_branch`` took a new value."""

    main_rewrites: int = 0
    """Projects whose ``__main`` was rewritten onto the default branch."""


@dataclass(slots=True)
class _ProjectFetches:
    """What the per-project GitHub reads returned, keyed by project id.

    A project appears in ``default_branches`` only when its ref set was
    also fetched: the ref set is the evidence the default-branch rule
    needs to judge whether ``__main``'s ref is gone.
    """

    refs_by_project: dict[int, RepositoryRefSet] = field(default_factory=dict)
    default_branches: dict[int, str] = field(default_factory=dict)
    had_failures: bool = False


async def git_ref_audit(ctx: dict[str, Any], payload: dict[str, Any]) -> str:
    """Run the daily ref audit for one org's GitHub-bound projects.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``).
    payload
        Job payload with ``org_id``, ``org_slug``, ``git_ref_audit_run_id``,
        and ``queue_job_id`` (the per-org ``queue_jobs`` row the
        discovery dispatcher created for this org).

    Returns
    -------
    str
        ``"completed"`` on a clean pass (every project's fetch
        succeeded), ``"completed_with_errors"`` when at least one
        project's fetch failed. Raises on hard failure after marking
        the queue job ``failed`` and rolling the parent run, mirroring
        ``lifecycle_eval``'s contract so arq logs the job as failed.
    """
    org_id: int = payload["org_id"]
    org_slug: str = payload["org_slug"]
    run_id: int = payload["git_ref_audit_run_id"]
    queue_job_id: int = payload["queue_job_id"]
    logger = structlog.get_logger("docverse_server.worker.git_ref_audit").bind(
        org=org_slug, git_ref_audit_run_id=run_id
    )

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        run_store = factory.create_git_ref_audit_run_store()

        async with session.begin():
            # Late-delivery guard (PRD #538): a reaper may have already
            # failed this row and finalised the parent run on its
            # behalf, or arq may have re-delivered a job another worker
            # is still running — re-auditing would double-count either
            # way.
            if await queue_job_store.start_if_queued(queue_job_id) is None:
                return "skipped"

        # Collected inside the soft-delete transaction and published only
        # after it commits below: one (project_slug, action) per reaped
        # edition. On the failure path ``_audit_org`` raises before its
        # transaction commits, so the partially-filled list is discarded
        # without ever being published (no phantom events for rolled-back
        # reaps).
        reaps: list[tuple[str, LifecycleReapAction]] = []
        try:
            summary = await _audit_org(
                session=session,
                factory=factory,
                org_id=org_id,
                org_slug=org_slug,
                reaps=reaps,
                events=ctx.get("events"),
                logger=logger,
            )
        except Exception as exc:
            logger.exception("Git ref audit failed for org")
            async with session.begin():
                await queue_job_store.fail(
                    queue_job_id,
                    errors={
                        "message": str(exc),
                        "type": type(exc).__name__,
                        "traceback": traceback.format_exc(),
                    },
                )
                await maybe_finalise_git_ref_audit_run(
                    run_store=run_store, run_id=run_id
                )
            raise

        async with session.begin():
            await queue_job_store.complete(
                queue_job_id, has_errors=summary.had_failures
            )
            await maybe_finalise_git_ref_audit_run(
                run_store=run_store, run_id=run_id
            )
        logger.info(
            "Git ref audit completed for org",
            had_failures=summary.had_failures,
            default_branch_updates=summary.default_branch_updates,
            main_rewrites=summary.main_rewrites,
        )
        # Publish one lifecycle_action per reaped edition after the commit.
        # Best-effort: production runs raise_on_error=False so a metrics
        # outage never fails the audit (no defensive try/except).
        await _publish_lifecycle_actions(
            ctx=ctx, org_slug=org_slug, reaps=reaps
        )
        return "completed_with_errors" if summary.had_failures else "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _audit_org(
    *,
    session: AsyncSession,
    factory: Factory,
    org_id: int,
    org_slug: str,
    reaps: list[tuple[str, LifecycleReapAction]],
    events: DocverseEvents | None,
    logger: structlog.stdlib.BoundLogger,
) -> _AuditSummary:
    """Audit every GitHub-bound project for the org.

    Splits into a single read transaction that loads the org + every
    GitHub-bound project + every project's editions in one batched
    read, then per-project fetches the live ref set and the default
    branch against GitHub (transaction-less network calls), then
    converges each project's ``__main`` on its default branch (one
    transaction per project, PRD #721), and finally a write transaction
    that flips ``date_deleted`` on every matched edition. The deletion
    transaction is one atomic commit per org so a crash mid-loop cannot
    leave the org half-deleted; the next day's discovery tick will
    re-evaluate from a consistent state.

    Returns the pass's :class:`_AuditSummary`; its ``had_failures`` is
    ``True`` if at least one project's fetch failed
    (``completed_with_errors`` for the parent queue-job row). Per-project
    fetch failures never bubble out of this function — the audit's
    failure-isolation contract is that one rate-limited installation
    cannot block the audit for every other project.
    """
    state = await _load_org_state(
        session=session, factory=factory, org_id=org_id
    )
    if state is None:
        logger.warning("Git ref audit skipped: organization not found")
        return _AuditSummary()
    org_rules, projects, editions_by_project = state

    if not projects:
        logger.debug("Git ref audit: no GitHub-bound projects for org")
        return _AuditSummary()

    fetches = await _fetch_per_project(
        factory=factory,
        projects=projects,
        logger=logger,
    )
    default_branch_updates, main_rewrites = await _converge_default_branches(
        session=session,
        factory=factory,
        projects=projects,
        fetches=fetches,
        org_slug=org_slug,
        events=events,
        logger=logger,
    )
    summary = _AuditSummary(
        had_failures=fetches.had_failures,
        default_branch_updates=default_branch_updates,
        main_rewrites=main_rewrites,
    )
    refs_by_project = fetches.refs_by_project

    if not refs_by_project:
        logger.debug("Git ref audit: no project ref sets fetched successfully")
        return summary

    matches_by_project = _evaluate_matches(
        projects=projects,
        refs_by_project=refs_by_project,
        editions_by_project=editions_by_project,
        org_rules=org_rules,
    )
    if not matches_by_project:
        logger.debug("Git ref audit: no matches across projects")
        return summary

    await _apply_deletions(
        session=session,
        factory=factory,
        projects=projects,
        matches_by_project=matches_by_project,
        editions_by_project=editions_by_project,
        org_id=org_id,
        org_slug=org_slug,
        reaps=reaps,
        logger=logger,
    )
    return summary


async def _fetch_per_project(
    *,
    factory: Factory,
    projects: list[Project],
    logger: structlog.stdlib.BoundLogger,
) -> _ProjectFetches:
    """Resolve binding + fetch refs and default branch, isolating failures.

    A project whose resolver returns ``None`` is treated as a graceful
    skip (no binding any more, or a race with a soft-delete) and does
    **not** flip ``had_failures``. A 404 from GitHub or a transport
    error on the ref set skips the project for this pass and flips
    ``had_failures`` so the per-org queue-job row transitions to
    ``completed_with_errors`` and the parent run rolls to
    ``partial_failure``.

    The default branch is read after the ref set, through the same
    installation-or-anonymous auth (PRD #721). A failure there flips
    ``had_failures`` the same way but keeps the ref set: the project's
    ``ref_deleted`` reaping still runs, and only its default-branch
    convergence waits for the next pass.
    """
    resolver = factory.create_project_github_binding_resolver()
    ref_fetcher = factory.create_github_ref_set_fetcher()
    fetches = _ProjectFetches()
    for project in projects:
        project_logger = logger.bind(
            project=project.slug, project_id=project.id
        )
        # ``resolver.resolve`` owns its own short read transaction and
        # mints the installation token (a GitHub network round-trip)
        # only after that transaction has closed; wrapping this call
        # in ``session.begin()`` would defeat that boundary and leave
        # the DB connection idle-in-transaction for every project's
        # token exchange.
        binding = await resolver.resolve(project.id)
        if binding is None:
            project_logger.debug(
                "Git ref audit: project has no GitHub binding, skipping"
            )
            continue
        try:
            ref_set = await ref_fetcher.fetch(
                owner=binding.owner,
                repo=binding.repo,
                auth=binding.auth,
                logger=project_logger,
            )
        except RepositoryNotAccessibleError as exc:
            project_logger.info(
                "Git ref audit: GitHub repository not accessible, "
                "skipping project for this pass",
                owner=exc.owner,
                repo=exc.repo,
                installation_id=binding.installation_id,
            )
            fetches.had_failures = True
            continue
        except RepositoryRefFetchError as exc:
            project_logger.warning(
                "Git ref audit: GitHub ref fetch failed, skipping "
                "project for this pass",
                owner=exc.owner,
                repo=exc.repo,
                installation_id=binding.installation_id,
                error=str(exc),
            )
            fetches.had_failures = True
            continue
        fetches.refs_by_project[project.id] = ref_set
        try:
            default_branch = await ref_fetcher.fetch_default_branch(
                owner=binding.owner,
                repo=binding.repo,
                auth=binding.auth,
                logger=project_logger,
            )
        except (RepositoryNotAccessibleError, RepositoryRefFetchError) as exc:
            project_logger.warning(
                "Git ref audit: GitHub repository metadata fetch failed, "
                "skipping default branch for this pass",
                owner=exc.owner,
                repo=exc.repo,
                installation_id=binding.installation_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            fetches.had_failures = True
            continue
        fetches.default_branches[project.id] = default_branch
    return fetches


async def _converge_default_branches(
    *,
    session: AsyncSession,
    factory: Factory,
    projects: list[Project],
    fetches: _ProjectFetches,
    org_slug: str,
    events: DocverseEvents | None,
    logger: structlog.stdlib.BoundLogger,
) -> tuple[int, int]:
    """Apply each fetched default branch to its project (PRD #721).

    Hands each branch, with the live ref set as evidence, to
    :class:`~docverse_server.services.default_branch.DefaultBranchService`,
    which records the column (the backfill for projects no webhook or
    resolve reached) and rewrites a ``__main`` whose tracked ref is
    absent from the set. A ``__main`` tracking a live branch or tag is
    left alone.

    One transaction per project, because the service holds that
    project's ``__main`` ``EDITION_UPDATE`` lock for its writes; an
    org-wide transaction would keep every earlier project's rows locked
    while waiting on a later project's lock. After each commit the
    deferred ``publish_edition`` job is handed to arq, and a rewritten
    ``__main`` gets what a ``PATCH`` of it would announce: one
    ``edition_lifecycle`` ``update`` event and one ``dashboard_build``.

    Returns ``(default_branch_updates, main_rewrites)``, the counts for
    the per-org summary log.
    """
    service = factory.create_default_branch_service()
    default_branch_updates = 0
    main_rewrites = 0
    for project in projects:
        default_branch = fetches.default_branches.get(project.id)
        ref_set = fetches.refs_by_project.get(project.id)
        if default_branch is None or ref_set is None:
            continue
        async with session.begin():
            outcome = await service.apply(
                project=project,
                default_branch=default_branch,
                trigger=DefaultBranchTrigger.audit,
                live_refs=ref_set.all,
            )
            await session.commit()
        await factory.queue_dispatcher.dispatch()
        if outcome.column_changed:
            default_branch_updates += 1
        if not outcome.main_rewritten:
            continue
        main_rewrites += 1
        if events is not None:
            await events.edition_lifecycle.publish(
                EditionLifecycleEvent(
                    organization=org_slug,
                    project=project.slug,
                    action=LifecycleAction.update,
                    edition_kind=MetricsEditionKind.main,
                )
            )
        await try_enqueue_dashboard_build_by_slug(
            factory=factory,
            session=session,
            logger=logger,
            org_slug=org_slug,
            project_slug=project.slug,
        )
    return default_branch_updates, main_rewrites


def _evaluate_matches(
    *,
    projects: list[Project],
    refs_by_project: dict[int, RepositoryRefSet],
    editions_by_project: dict[int, list[Edition]],
    org_rules: LifecycleRuleSet | None,
) -> dict[int, set[int]]:
    """Run :func:`evaluate_lifecycle` per project; collect edition matches.

    The resolved rule set is filtered down to ``RefDeletedRule`` before
    evaluation — this worker owns only that rule kind. Filtering makes
    the "each worker owns its concern" contract explicit and structural:
    the ``ref_deleted`` branch is the only one that can fire from this
    code path, even for an org that also configures other rule kinds
    (the hourly ``lifecycle_eval`` worker owns those). A project whose
    effective rule set carries no ``RefDeletedRule`` is skipped. The
    empty ``builds`` / ``edition_build_history`` lists are unused by the
    single remaining branch.
    """
    now = datetime.now(tz=UTC)
    matches_by_project: dict[int, set[int]] = {}
    for project in projects:
        ref_set = refs_by_project.get(project.id)
        if ref_set is None:
            continue
        rule_set = filter_rule_set(
            resolve_rule_set(
                org_rules=org_rules, project_rules=project.lifecycle_rules
            ),
            include=(RefDeletedRule,),
        )
        if not rule_set.root:
            continue
        decision = evaluate_lifecycle(
            rule_set=rule_set,
            context=LifecycleEvaluationContext(
                editions=editions_by_project.get(project.id, []),
                builds=[],
                edition_build_history=[],
                now=now,
                live_refs=ref_set.all,
            ),
        )
        if decision.edition_matches:
            matches_by_project[project.id] = set(decision.edition_matches)
    return matches_by_project


async def _apply_deletions(
    *,
    session: AsyncSession,
    factory: Factory,
    projects: list[Project],
    matches_by_project: dict[int, set[int]],
    editions_by_project: dict[int, list[Edition]],
    org_id: int,
    org_slug: str,
    reaps: list[tuple[str, LifecycleReapAction]],
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Soft-delete every matched edition in one transaction per org.

    Atomic per org: if the commit fails halfway, the next day's
    discovery tick re-evaluates from the rolled-back state. The
    dashboard rebuild enqueue happens after the soft-delete commit
    boundary, matching the lifecycle_eval worker's contract.
    """
    projects_with_deletes: list[str] = []
    editions_index = {
        e.id: e for editions in editions_by_project.values() for e in editions
    }
    async with session.begin():
        edition_service = factory.create_edition_service()
        publishing_service = factory.create_edition_publishing_service()
        for project in projects:
            matched_ids = matches_by_project.get(project.id)
            if not matched_ids:
                continue
            deleted_count = 0
            for edition_id in sorted(matched_ids):
                edition = editions_index.get(edition_id)
                if edition is None:
                    continue
                deleted = await edition_service.soft_delete(
                    org_id=org_id,
                    project_id=project.id,
                    edition_id=edition.id,
                    edition_slug=edition.slug,
                    reason=TombstoneReason.lifecycle_delete,
                )
                if not deleted:
                    continue
                deleted_count += 1
                reaps.append((project.slug, LifecycleReapAction.ref_deleted))
                await publishing_service.unpublish(
                    org_id=org_id,
                    project_slug=project.slug,
                    edition_slug=edition.slug,
                )
                deleted_ref = (
                    edition.tracking_params.get("git_ref")
                    if edition.tracking_params
                    else None
                )
                logger.info(
                    "Soft-deleted edition by git_ref_audit",
                    trigger="audit",
                    entity_type="edition",
                    entity_id=edition.id,
                    entity_slug=edition.slug,
                    deleted_ref=deleted_ref,
                    org_id=org_id,
                    org=org_slug,
                    project_id=project.id,
                    project=project.slug,
                    # _evaluate_matches filters to RefDeletedRule, so
                    # every match reaching here is a ref-deleted match.
                    rule_type="ref_deleted",
                )
            if deleted_count:
                projects_with_deletes.append(project.slug)

    for project_slug in projects_with_deletes:
        await try_enqueue_dashboard_build_by_slug(
            factory=factory,
            session=session,
            logger=logger,
            org_slug=org_slug,
            project_slug=project_slug,
        )


async def _load_org_state(
    *,
    session: AsyncSession,
    factory: Factory,
    org_id: int,
) -> (
    tuple[
        LifecycleRuleSet | None,
        list[Project],
        dict[int, list[Edition]],
    ]
    | None
):
    """Batch-load the data the audit needs for one org.

    Three round-trips total, regardless of project count: org,
    GitHub-bound projects, editions. Returns ``None`` when the org has
    been deleted between the dispatcher's pre-flight and the worker
    picking up the job. The org's ``lifecycle_rules`` is included so
    :func:`resolve_rule_set` can be called per project without an
    additional read per project.
    """
    org_store = factory.create_org_store()
    project_store = factory.create_project_store()
    edition_store = factory.create_edition_store()

    async with session.begin():
        org = await org_store.get_by_id(org_id)
        if org is None:
            return None
        projects = await project_store.list_github_bound_by_org(org_id)
        project_ids = [p.id for p in projects]
        editions = await edition_store.list_all_by_project_ids(project_ids)

    editions_by_project: dict[int, list[Edition]] = {
        pid: [] for pid in project_ids
    }
    for edition in editions:
        editions_by_project[edition.project_id].append(edition)
    return org.lifecycle_rules, projects, editions_by_project


async def _publish_lifecycle_actions(
    *,
    ctx: dict[str, Any],
    org_slug: str,
    reaps: list[tuple[str, LifecycleReapAction]],
) -> None:
    """Emit one ``lifecycle_action`` metric per ref-deleted reap.

    ``trigger`` is fixed to ``git_ref_audit`` (this worker) and ``action``
    is always ``ref_deleted`` (the only rule this worker honours), and
    ``success`` is ``True`` because each reap is published only after its
    atomic soft-delete transaction committed. Skips silently when the
    process has no event manager (tests that do not assert on metrics).
    """
    events = ctx.get("events")
    if events is None:
        return
    for project_slug, action in reaps:
        await events.lifecycle_action.publish(
            LifecycleActionEvent(
                organization=org_slug,
                project=project_slug,
                action=action,
                trigger=LifecycleActionTrigger.git_ref_audit,
                success=True,
            )
        )
