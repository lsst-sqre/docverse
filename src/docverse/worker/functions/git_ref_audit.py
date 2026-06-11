"""arq worker function for the daily ``git_ref_audit`` per-org pass.

The discovery dispatcher (sibling task) writes one
``git_ref_audit_runs`` row per tick, then fans out one ``queue_jobs``
row per in-scope org with ``kind='git_ref_audit'`` and
``subject_label=org.slug`` (mirroring the ``lifecycle_eval`` per-org
worker so an operator inspecting the queue sees a meaningful
subject). This worker is the per-org body of that fan-out: for one
org it lists every non-deleted GitHub-bound project, resolves each
project's GitHub binding, fetches the live ref set against GitHub,
runs :func:`evaluate_lifecycle` with ``live_refs`` populated, and
soft-deletes the matched editions.

The worker owns the ``queue_jobs`` row lifecycle: it transitions to
``in_progress`` on entry and to ``completed`` /
``completed_with_errors`` / ``failed`` on exit, then calls
:func:`maybe_finalise_git_ref_audit_run` so the parent
``git_ref_audit_runs`` row rolls to its terminal status once every
per-org child is terminal.

A per-project fetch failure is caught, logged with org/project/error,
and the per-org pass continues with the next project — one
rate-limited installation cannot block the audit for every other
project. When at least one project failed, the queue-job row
transitions to ``completed_with_errors`` and ``aggregate_activity``
in the finaliser routes the parent run to ``partial_failure``;
otherwise the row transitions to ``completed`` and the parent rolls
to ``succeeded``.
"""

from __future__ import annotations

import traceback
from datetime import UTC, datetime
from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.domain.edition import Edition
from docverse.domain.lifecycle import LifecycleRuleSet, RefDeletedRule
from docverse.domain.project import Project
from docverse.factory import Factory
from docverse.metrics import (
    LifecycleActionEvent,
    LifecycleActionTrigger,
    LifecycleReapAction,
)
from docverse.services.dashboard.enqueue import (
    try_enqueue_dashboard_build_by_slug,
)
from docverse.services.git_ref_audit_finalisation import (
    maybe_finalise_git_ref_audit_run,
)
from docverse.services.lifecycle.evaluator import (
    LifecycleEvaluationContext,
    evaluate_lifecycle,
    filter_rule_set,
    resolve_rule_set,
)
from docverse.storage.github import (
    RepositoryNotAccessibleError,
    RepositoryRefFetchError,
    RepositoryRefSet,
)
from docverse.storage.keeper_sync import TombstoneReason

__all__ = ["git_ref_audit"]


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
    logger = structlog.get_logger("docverse.worker.git_ref_audit").bind(
        org=org_slug, git_ref_audit_run_id=run_id
    )

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        run_store = factory.create_git_ref_audit_run_store()

        async with session.begin():
            await queue_job_store.start(queue_job_id)

        # Collected inside the soft-delete transaction and published only
        # after it commits below: one (project_slug, action) per reaped
        # edition. On the failure path ``_audit_org`` raises before its
        # transaction commits, so the partially-filled list is discarded
        # without ever being published (no phantom events for rolled-back
        # reaps).
        reaps: list[tuple[str, LifecycleReapAction]] = []
        try:
            had_failures = await _audit_org(
                session=session,
                factory=factory,
                org_id=org_id,
                org_slug=org_slug,
                reaps=reaps,
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
                queue_job_id, has_errors=had_failures
            )
            await maybe_finalise_git_ref_audit_run(
                run_store=run_store, run_id=run_id
            )
        logger.info(
            "Git ref audit completed for org",
            had_failures=had_failures,
        )
        # Publish one lifecycle_action per reaped edition after the commit.
        # Best-effort: production runs raise_on_error=False so a metrics
        # outage never fails the audit (no defensive try/except).
        await _publish_lifecycle_actions(
            ctx=ctx, org_slug=org_slug, reaps=reaps
        )
        return "completed_with_errors" if had_failures else "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _audit_org(  # noqa: PLR0913
    *,
    session: AsyncSession,
    factory: Factory,
    org_id: int,
    org_slug: str,
    reaps: list[tuple[str, LifecycleReapAction]],
    logger: structlog.stdlib.BoundLogger,
) -> bool:
    """Audit every GitHub-bound project for the org.

    Splits into a single read transaction that loads the org + every
    GitHub-bound project + every project's editions in one batched
    read, then per-project fetches the live ref set against GitHub
    (one transaction-less network call per project), and finally a
    write transaction that flips ``date_deleted`` on every matched
    edition. The write transaction is one atomic commit per org so a
    crash mid-loop cannot leave the org half-deleted; the next day's
    discovery tick will re-evaluate from a consistent state.

    Returns ``True`` if at least one project's fetch failed
    (``completed_with_errors`` for the parent queue-job row), else
    ``False`` (``completed``). Per-project fetch failures never bubble
    out of this function — the audit's failure-isolation contract is
    that one rate-limited installation cannot block the audit for
    every other project.
    """
    state = await _load_org_state(
        session=session, factory=factory, org_id=org_id
    )
    if state is None:
        logger.warning("Git ref audit skipped: organization not found")
        return False
    org_rules, projects, editions_by_project = state

    if not projects:
        logger.debug("Git ref audit: no GitHub-bound projects for org")
        return False

    refs_by_project, had_failures = await _fetch_refs_per_project(
        session=session,
        factory=factory,
        projects=projects,
        logger=logger,
    )

    if not refs_by_project:
        logger.debug("Git ref audit: no project ref sets fetched successfully")
        return had_failures

    matches_by_project = _evaluate_matches(
        projects=projects,
        refs_by_project=refs_by_project,
        editions_by_project=editions_by_project,
        org_rules=org_rules,
    )
    if not matches_by_project:
        logger.debug("Git ref audit: no matches across projects")
        return had_failures

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
    return had_failures


async def _fetch_refs_per_project(
    *,
    session: AsyncSession,
    factory: Factory,
    projects: list[Project],
    logger: structlog.stdlib.BoundLogger,
) -> tuple[dict[int, RepositoryRefSet], bool]:
    """Resolve binding + fetch refs for each project, isolating failures.

    Returns ``(refs_by_project, had_failures)``. A project whose
    resolver returns ``None`` is treated as a graceful skip (no
    binding any more, or a race with a soft-delete) and does **not**
    flip ``had_failures``. A 404 from GitHub or a transport error
    flips ``had_failures`` so the per-org queue-job row transitions
    to ``completed_with_errors`` and the parent run rolls to
    ``partial_failure``.
    """
    resolver = factory.create_project_github_binding_resolver()
    ref_fetcher = factory.create_github_ref_set_fetcher()
    refs_by_project: dict[int, RepositoryRefSet] = {}
    had_failures = False
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
            had_failures = True
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
            had_failures = True
            continue
        refs_by_project[project.id] = ref_set
    return refs_by_project, had_failures


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


async def _apply_deletions(  # noqa: PLR0913
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
