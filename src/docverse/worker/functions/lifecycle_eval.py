"""arq worker function for the ``lifecycle_eval`` per-org pass.

The dispatcher cron (sibling task) writes one ``lifecycle_eval_runs``
row per tick, then fans out one ``queue_jobs`` row per in-scope org
with ``kind='lifecycle_eval'`` and ``subject_label=org.slug`` (the
human-readable org slug — never the internal database id, mirroring
``keeper_sync_project``'s ``subject_label=ltd_slug`` convention so an
operator inspecting the queue sees a meaningful subject). This worker
is the per-org body of that fan-out: for one org it loads the org row,
every non-deleted project, every project's editions, builds, and
rollback-history rows in batched reads (no N+1), evaluates the
effective lifecycle rule set per project via the pure
:func:`docverse.services.lifecycle.evaluator.evaluate_lifecycle`
function, soft-deletes matched editions and builds, and emits one
structured log event per deletion identifying the matched rule.

The worker owns the ``queue_jobs`` row lifecycle: it transitions to
``in_progress`` on entry and to ``completed`` / ``failed`` on exit,
then calls
:func:`docverse.services.lifecycle_finalisation.maybe_finalise_lifecycle_run`
so the parent ``lifecycle_eval_runs`` row rolls to its terminal status
once every per-org child is terminal.
"""

from __future__ import annotations

import traceback
from datetime import UTC, datetime
from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.edition_build_history import EditionBuildHistory
from docverse.domain.lifecycle import LifecycleRule, LifecycleRuleSet
from docverse.domain.project import Project
from docverse.factory import Factory
from docverse.services.dashboard.enqueue import (
    try_enqueue_dashboard_build_by_slug,
)
from docverse.services.lifecycle.evaluator import (
    LifecycleDecision,
    evaluate_lifecycle,
    resolve_rule_set,
)
from docverse.services.lifecycle_finalisation import (
    maybe_finalise_lifecycle_run,
)
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore

__all__ = ["lifecycle_eval"]


async def lifecycle_eval(ctx: dict[str, Any], payload: dict[str, Any]) -> str:
    """Evaluate lifecycle rules for one org's projects and soft-delete matches.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``).
    payload
        Job payload with ``org_id``, ``org_slug``, ``lifecycle_eval_run_id``,
        and ``queue_job_id`` (the per-org ``queue_jobs`` row the
        dispatcher created for this org).

    Returns
    -------
    str
        ``"completed"`` on a clean pass (including the empty-rule-set
        no-op case). Raises on failure after marking the queue job
        failed and rolling the parent run, mirroring ``keeper_sync_
        project``'s contract so arq logs the job as failed.
    """
    org_id: int = payload["org_id"]
    org_slug: str = payload["org_slug"]
    run_id: int = payload["lifecycle_eval_run_id"]
    queue_job_id: int = payload["queue_job_id"]
    logger = structlog.get_logger("docverse.worker.lifecycle_eval").bind(
        org=org_slug, lifecycle_eval_run_id=run_id
    )

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        run_store = factory.create_lifecycle_eval_run_store()

        async with session.begin():
            await queue_job_store.start(queue_job_id)

        try:
            await _evaluate_org(
                session=session,
                factory=factory,
                org_id=org_id,
                org_slug=org_slug,
                logger=logger,
            )
        except Exception as exc:
            logger.exception("Lifecycle evaluation failed for org")
            async with session.begin():
                await queue_job_store.fail(
                    queue_job_id,
                    errors={
                        "message": str(exc),
                        "type": type(exc).__name__,
                        "traceback": traceback.format_exc(),
                    },
                )
                await maybe_finalise_lifecycle_run(
                    run_store=run_store, run_id=run_id
                )
            raise

        async with session.begin():
            await queue_job_store.complete(queue_job_id)
            await maybe_finalise_lifecycle_run(
                run_store=run_store, run_id=run_id
            )
        logger.info("Lifecycle evaluation completed for org")
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _evaluate_org(
    *,
    session: AsyncSession,
    factory: Factory,
    org_id: int,
    org_slug: str,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Load the org's state, evaluate per-project rules, and apply deletions.

    Splits into a single read transaction that loads org + projects +
    editions + builds + history (all batched), then a single write
    transaction that flips ``date_deleted`` on every matched row. The
    write transaction is one atomic commit per org so a crash mid-loop
    cannot leave the org half-deleted; the next dispatcher tick will
    re-evaluate from a consistent state.
    """
    state = await _load_org_state(
        session=session, factory=factory, org_id=org_id
    )
    if state is None:
        logger.warning("Lifecycle eval skipped: organization not found")
        return
    (
        org_rules,
        projects,
        editions_by_project,
        builds_by_project,
        history_by_project,
    ) = state

    if not projects:
        logger.debug("Lifecycle eval: no projects for org")
        return

    now = datetime.now(tz=UTC)
    decisions: list[tuple[Project, LifecycleRuleSet, LifecycleDecision]] = []
    for project in projects:
        rule_set = resolve_rule_set(
            org_rules=org_rules, project_rules=project.lifecycle_rules
        )
        if not rule_set.root:
            continue
        decision = evaluate_lifecycle(
            rule_set=rule_set,
            editions=editions_by_project.get(project.id, []),
            builds=builds_by_project.get(project.id, []),
            edition_build_history=history_by_project.get(project.id, []),
            now=now,
        )
        if decision.edition_matches or decision.build_matches:
            decisions.append((project, rule_set, decision))

    if not decisions:
        logger.debug("Lifecycle eval: no matches across projects")
        return

    edition_index = _index_editions_by_id(editions_by_project)
    build_index = _index_builds_by_id(builds_by_project)
    projects_with_edition_deletes: list[str] = []

    async with session.begin():
        edition_store = factory.create_edition_store()
        build_store = factory.create_build_store()
        for project, rule_set, decision in decisions:
            editions_deleted = await _apply_decision(
                edition_store=edition_store,
                build_store=build_store,
                project=project,
                rule_set=rule_set,
                decision=decision,
                edition_index=edition_index,
                build_index=build_index,
                org_id=org_id,
                org_slug=org_slug,
                logger=logger,
            )
            if editions_deleted:
                projects_with_edition_deletes.append(project.slug)

    # Refresh the cached dashboard artifact for each project whose
    # edition list actually shrank. Runs after the soft-delete commit
    # boundary so the helper opens its own transaction, matching the
    # user-initiated DELETE handler. Build-only deletions are skipped:
    # the cached dashboard renders from the edition list, not the build
    # list, so soft-deleting an orphan build would not change the
    # rendered output and a rebuild would be wasted work.
    for project_slug in projects_with_edition_deletes:
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
        dict[int, list[Build]],
        dict[int, list[EditionBuildHistory]],
    ]
    | None
):
    """Batch-load the data the evaluator needs for every project in one org.

    Five round-trips total, regardless of project count: org, projects,
    editions, builds, history. The ``IN (project_ids)`` and
    ``IN (edition_ids)`` filters do the per-project / per-edition
    grouping in memory after the load. Returns ``None`` when the org
    has been deleted between the dispatcher's pre-flight and the
    worker picking up the job.
    """
    org_store = factory.create_org_store()
    project_store = factory.create_project_store()
    edition_store = factory.create_edition_store()
    build_store = factory.create_build_store()
    history_store = factory.create_edition_build_history_store()

    async with session.begin():
        org = await org_store.get_by_id(org_id)
        if org is None:
            return None
        projects = await project_store.list_all_by_org(org_id)
        project_ids = [p.id for p in projects]
        editions = await edition_store.list_all_by_project_ids(project_ids)
        builds = await build_store.list_all_by_project_ids(project_ids)
        history = await history_store.list_by_edition_ids(
            [e.id for e in editions]
        )

    editions_by_project: dict[int, list[Edition]] = {
        pid: [] for pid in project_ids
    }
    for edition in editions:
        editions_by_project[edition.project_id].append(edition)
    builds_by_project: dict[int, list[Build]] = {
        pid: [] for pid in project_ids
    }
    for build in builds:
        builds_by_project[build.project_id].append(build)
    edition_to_project: dict[int, int] = {e.id: e.project_id for e in editions}
    history_by_project: dict[int, list[EditionBuildHistory]] = {
        pid: [] for pid in project_ids
    }
    for row in history:
        project_id = edition_to_project.get(row.edition_id)
        if project_id is not None:
            history_by_project[project_id].append(row)
    return (
        org.lifecycle_rules,
        projects,
        editions_by_project,
        builds_by_project,
        history_by_project,
    )


def _index_editions_by_id(
    editions_by_project: dict[int, list[Edition]],
) -> dict[int, Edition]:
    return {
        e.id: e for editions in editions_by_project.values() for e in editions
    }


def _index_builds_by_id(
    builds_by_project: dict[int, list[Build]],
) -> dict[int, Build]:
    return {b.id: b for builds in builds_by_project.values() for b in builds}


async def _apply_decision(  # noqa: PLR0913
    *,
    edition_store: EditionStore,
    build_store: BuildStore,
    project: Project,
    rule_set: LifecycleRuleSet,
    decision: LifecycleDecision,
    edition_index: dict[int, Edition],
    build_index: dict[int, Build],
    org_id: int,
    org_slug: str,
    logger: structlog.stdlib.BoundLogger,
) -> int:
    """Soft-delete every entity the decision matched and emit one log per row.

    The structured log line is the v1 audit trail (persistent
    ``delete_reason`` columns are deferred to DM-54914 per the PRD).
    Each event identifies the entity type and id, the project's
    ``(org_id, org_slug, project_id, project_slug)`` for cross-row
    correlation, the matched rule's ``type`` discriminator (read from
    ``decision.edition_matches`` / ``decision.build_matches`` so any
    future rule that matches editions or builds attributes correctly
    without code changes here), and the rule's resolved parameter
    dict so an operator can audit *why* a row was deleted from logs
    alone.

    Returns the number of editions that actually transitioned to
    soft-deleted in this call so the caller can decide whether a
    dashboard rebuild is warranted. Build soft-deletes are not
    counted: the cached dashboard renders from the edition list, not
    the build list.
    """
    rules_by_type: dict[str, LifecycleRule] = {
        rule.type: rule for rule in rule_set.root
    }
    editions_deleted = 0
    for edition_id in sorted(decision.edition_matches):
        edition = edition_index.get(edition_id)
        if edition is None:
            continue
        rule_type = decision.edition_matches[edition_id]
        rule = rules_by_type.get(rule_type)
        deleted = await edition_store.soft_delete(
            project_id=project.id, slug=edition.slug
        )
        if not deleted:
            continue
        editions_deleted += 1
        logger.info(
            "Soft-deleted entity by lifecycle rule",
            entity_type="edition",
            entity_id=edition.id,
            entity_slug=edition.slug,
            org_id=org_id,
            org=org_slug,
            project_id=project.id,
            project=project.slug,
            rule_type=rule_type,
            rule_params=(
                rule.model_dump(exclude={"type"}) if rule is not None else None
            ),
        )
    for build_id in sorted(decision.build_matches):
        build = build_index.get(build_id)
        if build is None:
            continue
        rule_type = decision.build_matches[build_id]
        rule = rules_by_type.get(rule_type)
        deleted = await build_store.soft_delete(build_id=build.id)
        if not deleted:
            continue
        logger.info(
            "Soft-deleted entity by lifecycle rule",
            entity_type="build",
            entity_id=build.id,
            org_id=org_id,
            org=org_slug,
            project_id=project.id,
            project=project.slug,
            rule_type=rule_type,
            rule_params=(
                rule.model_dump(exclude={"type"}) if rule is not None else None
            ),
        )
    return editions_deleted
