"""arq worker function for the ``purgatory_cleanup`` per-org sweep.

The daily dispatcher (see
:mod:`docverse_server.worker.functions.purgatory_cleanup_dispatcher`)
writes one ``queue_jobs`` row per organization holding a build past its
retention, with ``kind='purgatory_cleanup'`` and
``subject_label=org.slug``. This worker is the per-org body of that
fan-out: for one org it plans the tick's work, opens the org's staging
store, and permanently deletes each expired build's unpacked tree and
staged tarball, stamping ``date_purged`` on the row as each one goes.

Everything about the job's shape follows from one asymmetry: **the
object-store delete cannot be rolled back and the database write can.**
So the two are never in the same transaction, and they are ordered so
that a crash between them leaves the recoverable side wrong rather than
the unrecoverable one:

- The plan is read in one short transaction, which then closes. Holding
  it across hundreds of network round-trips would pin a connection
  idle-in-transaction for the whole run.
- Each build's reclamation happens with **no transaction open**.
- ``date_purged`` is stamped in its own short transaction immediately
  after. A crash here leaves objects gone and the row unstamped, so the
  next tick re-attempts a delete that is a no-op on both halves and then
  stamps — convergent. The reverse order would leave a stamped row whose
  content is still on the store with nothing pointing at it, which no
  later tick would ever revisit.

Two races are therefore possible against an admin restore, and each is
resolved rather than prevented:

- **Restore lands between planning and reclamation.** The per-build
  re-read catches it and the build is skipped; nothing is deleted.
- **Restore lands between reclamation and the stamp.** ``mark_purged``
  is conditional on the row still being deleted, so it reports zero
  rows. The build is counted as failed and logged at error level: the
  row is live again but its content is gone, and only an operator can
  reconcile that.

A per-build exception is counted and the loop continues — one build
whose prefix the store refuses must not strand every build behind it —
so the job ends ``completed`` or ``completed_with_errors``, never
``failed``. Only a failure that makes the whole tick impossible (the org
vanished, no staging store is configured, its credential will not
decrypt) fails the queue-job row and re-raises.

The same asymmetry shapes when the tick's metrics go out. Everything is
published after the final commit, never during the loop: the tally rides
on ``date_purged`` stamps that have already committed, so a crash
mid-loop publishes nothing at all rather than events for reclamations
the database never recorded. A tick that could not run publishes nothing
either — its record is the ``failed`` queue-job row.
"""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse_server.config import config
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.exceptions import NotFoundError
from docverse_server.factory import Factory
from docverse_server.metrics import (
    LifecycleActionEvent,
    LifecycleActionTrigger,
    LifecycleReapAction,
    PurgatoryCleanupCompletedEvent,
)
from docverse_server.services.purgatory import PurgatoryPlan
from docverse_server.storage.objectstore import ObjectStore

__all__ = ["PurgatoryCleanupOutcome", "purgatory_cleanup"]


@dataclass(slots=True)
class PurgatoryCleanupOutcome:
    """What one organization's sweep did, as the job reports it.

    Accumulated across the reclamation loop and then rendered two ways:
    into the ``queue_jobs`` row's ``progress`` JSONB, and into the
    org-scoped ``purgatory_cleanup_completed`` event that is the durable
    record of a run. Keeping one tally behind both means the number an
    operator reads off the job row and the number Sasquatch charts show
    can never diverge.
    """

    builds_purged: int = 0
    """Builds whose content went and whose row this job stamped."""

    builds_failed: int = 0
    """Builds this job attempted and could not complete."""

    builds_skipped_referenced: int = 0
    """Builds held back because a live edition still serves them."""

    objects_deleted: int = 0
    """Objects removed from under the purged builds' prefixes."""

    bytes_reclaimed: int = 0
    """Total size the purged builds recorded occupying."""

    capped: bool = False
    """Whether the per-job cap, not the backlog, ended the work list."""

    failed_build_ids: list[str] = field(default_factory=list)
    """Public ids of the builds counted under :attr:`builds_failed`."""

    skipped_build_ids: list[str] = field(default_factory=list)
    """Public ids of every build left unstamped for any reason.

    Wider than :attr:`builds_skipped_referenced`, which counts only the
    dominant reason: a build the plan offered but a restore reclaimed
    first appears here too, because what an operator wants from this
    list is "what did the sweep leave behind", not "why".
    """

    purged_project_slugs: list[str] = field(default_factory=list)
    """Owning project of each purged build, one entry per build.

    The ``lifecycle_action`` events are project-scoped while the tick's
    completion event is org-scoped, so this is the only per-build
    dimension the tally has to carry. Appended only once a build's
    ``mark_purged`` has committed, which is what keeps a reap from being
    published for a reclamation the database never recorded. Duplicated
    when several purged builds share a project — one reap is one build,
    not one project.

    Deliberately absent from :meth:`as_progress`: the queue row already
    names what the tick left behind, and repeating the projects it got
    through would grow unboundedly with the cap.
    """

    @property
    def has_errors(self) -> bool:
        """Whether the job should complete as ``completed_with_errors``."""
        return self.builds_failed > 0

    def as_progress(self) -> dict[str, Any]:
        """Render the tally as the ``queue_jobs.progress`` JSONB body."""
        return {
            "builds_purged": self.builds_purged,
            "builds_failed": self.builds_failed,
            "builds_skipped_referenced": self.builds_skipped_referenced,
            "objects_deleted": self.objects_deleted,
            "bytes_reclaimed": self.bytes_reclaimed,
            "capped": self.capped,
            "failed_build_ids": list(self.failed_build_ids),
            "skipped_build_ids": list(self.skipped_build_ids),
        }


async def purgatory_cleanup(
    ctx: dict[str, Any], payload: dict[str, Any]
) -> str:
    """Reclaim one organization's expired builds from its staging store.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``).
    payload
        Job payload with ``org_id``, ``org_slug``, and ``queue_job_id``
        (the per-org ``queue_jobs`` row the dispatcher created).

    Returns
    -------
    str
        ``"skipped"`` when the late-delivery guard refused the row,
        ``"completed"`` when every planned build was reclaimed, and
        ``"completed_with_errors"`` when at least one build failed. A
        failure that makes the whole tick impossible marks the queue job
        ``failed`` and re-raises so arq logs the job as failed.
    """
    org_id: int = payload["org_id"]
    org_slug: str = payload["org_slug"]
    queue_job_id: int = payload["queue_job_id"]
    logger = structlog.get_logger(
        "docverse_server.worker.purgatory_cleanup"
    ).bind(org=org_slug)
    started = time.monotonic()

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()

        async with session.begin():
            # Late-delivery guard (PRD #538): a reaper may already have
            # failed this row, or arq may be re-delivering a job another
            # worker is still running. Re-reclaiming is harmless on the
            # store but would double-count the tally, and running two
            # sweeps over one org concurrently is exactly what the
            # per-org mutex exists to prevent.
            if await queue_job_store.start_if_queued(queue_job_id) is None:
                return "skipped"

        limit = config.purgatory_cleanup_max_builds_per_job
        try:
            plan, project_slugs, object_store = await _prepare(
                session=session,
                factory=factory,
                org_id=org_id,
                limit=limit,
            )
            async with object_store:
                outcome = await _reclaim_plan(
                    session=session,
                    factory=factory,
                    plan=plan,
                    project_slugs=project_slugs,
                    object_store=object_store,
                    limit=limit,
                    logger=logger,
                )
        except Exception as exc:
            logger.exception("Purgatory cleanup failed for org")
            async with session.begin():
                await queue_job_store.fail(
                    queue_job_id,
                    errors={
                        "message": str(exc),
                        "type": type(exc).__name__,
                        "traceback": traceback.format_exc(),
                    },
                )
            raise

        progress = outcome.as_progress()
        async with session.begin():
            await queue_job_store.update_progress(queue_job_id, progress)
            await queue_job_store.complete(
                queue_job_id, has_errors=outcome.has_errors
            )
        logger.info("Purgatory cleanup completed for org", **progress)
        # Published from the same tally the queue row just recorded, and
        # only now that every stamp behind it is durable. Best-effort:
        # production runs raise_on_error=False so a metrics outage can
        # never fail a sweep (no defensive try/except).
        await _publish_sweep_events(
            ctx=ctx, org_slug=org_slug, outcome=outcome, started=started
        )
        return "completed_with_errors" if outcome.has_errors else "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _prepare(
    *,
    session: AsyncSession,
    factory: Factory,
    org_id: int,
    limit: int,
) -> tuple[PurgatoryPlan, dict[int, str], ObjectStore]:
    """Build the tick's work list and resolve the org's staging store.

    All of it in one short read transaction that closes before any
    object is touched: the plan is a handful of indexed reads, the slug
    lookup one more, and resolving the store is a service lookup plus a
    credential decrypt, whereas the work the plan describes is minutes
    of network calls.

    The store is the org's ``resolved_staging_store_label`` — where
    ``build_processing`` wrote both the unpacked tree and the staged
    tarball. An org with no such label has nowhere the sweep could
    look, so the job fails rather than reporting a clean tick that
    reclaimed nothing.

    The slug map is read here, with the plan, rather than per build
    later: it is the project dimension of the reap events, and reading
    it now costs one query on ids the plan already has. It covers the
    held-back builds too, which is one query fewer than partitioning it
    and costs nothing on a plan bounded by the cap.

    Returns
    -------
    tuple
        The plan, the slug of every project the plan touches by id
        (deleted projects included — the cascade is what puts most
        builds in purgatory), and the opened staging store.
    """
    org_store = factory.create_org_store()
    project_store = factory.create_project_store()
    purgatory_service = factory.create_purgatory_service()
    now = datetime.now(tz=UTC)
    async with session.begin():
        org = await org_store.get_by_id(org_id)
        if org is None:
            msg = f"Organization {org_id} not found"
            raise NotFoundError(msg)
        service_label = org.resolved_staging_store_label
        if service_label is None:
            msg = f"No object store service configured for org {org_id}"
            raise RuntimeError(msg)
        plan = await purgatory_service.plan(org=org, now=now, limit=limit)
        project_ids = {build.project_id for build in plan.purgeable}
        project_ids |= {held.build.project_id for held in plan.referenced}
        project_slugs = await project_store.list_slugs_by_ids(
            sorted(project_ids)
        )
        object_store = await factory.create_objectstore_for_org(
            org_id=org_id, service_label=service_label
        )
    return plan, project_slugs, object_store


async def _reclaim_plan(
    *,
    session: AsyncSession,
    factory: Factory,
    plan: PurgatoryPlan,
    project_slugs: dict[int, str],
    object_store: ObjectStore,
    limit: int,
    logger: structlog.stdlib.BoundLogger,
) -> PurgatoryCleanupOutcome:
    """Reclaim every purgeable build in the plan, isolating failures.

    ``capped`` reports that the per-job cap, rather than the org's
    backlog, is what ended this work list: the plan reads at most
    ``limit`` candidates, so a full plan means there may be more waiting.
    The rows the cap left are unstamped and the work list is ordered
    oldest deletion first, so the next tick resumes exactly where this
    one stopped.
    """
    outcome = PurgatoryCleanupOutcome(
        capped=len(plan.purgeable) + len(plan.referenced) >= limit
    )
    build_store = factory.create_build_store()
    purgatory_service = factory.create_purgatory_service()

    for held in plan.referenced:
        public_id = serialize_base32_id(held.build.public_id)
        outcome.builds_skipped_referenced += 1
        outcome.skipped_build_ids.append(public_id)
        logger.warning(
            "Holding back a deleted build a live edition still serves",
            build=public_id,
            editions=list(held.edition_slugs),
        )

    for build in plan.purgeable:
        public_id = serialize_base32_id(build.public_id)
        try:
            # Resolved before anything is deleted so a build whose
            # project somehow went missing fails with its content
            # intact, rather than after an unrecoverable delete.
            project_slug = project_slugs[build.project_id]
            # Re-read under a fresh transaction rather than trusting the
            # plan: an admin restore may have committed since, and this
            # is the last moment at which the objects are still there to
            # keep.
            async with session.begin():
                current = await build_store.get_by_id(build.id)
            if current is None or current.date_deleted is None:
                outcome.skipped_build_ids.append(public_id)
                logger.info(
                    "Skipping a build restored since the sweep planned it",
                    build=public_id,
                )
                continue

            reclaimed = await purgatory_service.reclaim(
                build=build, object_store=object_store
            )

            async with session.begin():
                stamped = await build_store.mark_purged(build_id=build.id)
            if stamped is None:
                # The row is live again and its content is not. Nothing
                # in the sweep can undo that, so it is surfaced as a
                # failure for an operator rather than counted as work.
                outcome.builds_failed += 1
                outcome.failed_build_ids.append(public_id)
                logger.error(
                    "Reclaimed a build that was restored mid-purge; its "
                    "content is gone but its row is live again",
                    build=public_id,
                )
                continue

            outcome.builds_purged += 1
            outcome.objects_deleted += reclaimed.objects_deleted
            outcome.bytes_reclaimed += reclaimed.bytes_reclaimed
            outcome.purged_project_slugs.append(project_slug)
            logger.info(
                "Reclaimed a build past its retention",
                build=public_id,
                objects_deleted=reclaimed.objects_deleted,
                bytes_reclaimed=reclaimed.bytes_reclaimed,
            )
        except Exception:
            outcome.builds_failed += 1
            outcome.failed_build_ids.append(public_id)
            logger.exception(
                "Failed to reclaim a build past its retention",
                build=public_id,
            )
    return outcome


async def _publish_sweep_events(
    *,
    ctx: dict[str, Any],
    org_slug: str,
    outcome: PurgatoryCleanupOutcome,
    started: float,
) -> None:
    """Emit the tick's completion gauge and one reap per purged build.

    Called once, after the queue row's terminal transition has
    committed, so every event here stands on a durable write. ``success``
    answers the same question the queue row's ``completed`` vs.
    ``completed_with_errors`` does — whether every build the tick
    attempted came through — rather than whether the tick ran, which is
    implied by the event existing at all.

    The reaps are the per-build half: ``retention_expired`` is what
    retired each one, and the trigger names this sweep so a consumer can
    tell the storage reclamation apart from the soft-deletes
    ``lifecycle_eval`` and ``git_ref_audit`` publish on the same event
    type. Skips silently when the process has no event manager (tests
    that do not assert on metrics).
    """
    events = ctx.get("events")
    if events is None:
        return
    await events.purgatory_cleanup_completed.publish(
        PurgatoryCleanupCompletedEvent(
            organization=org_slug,
            project=None,
            success=not outcome.has_errors,
            builds_purged=outcome.builds_purged,
            builds_failed=outcome.builds_failed,
            builds_skipped_referenced=outcome.builds_skipped_referenced,
            objects_deleted=outcome.objects_deleted,
            bytes_reclaimed=outcome.bytes_reclaimed,
            capped=outcome.capped,
            elapsed=timedelta(seconds=time.monotonic() - started),
        )
    )
    for project_slug in outcome.purged_project_slugs:
        await events.lifecycle_action.publish(
            LifecycleActionEvent(
                organization=org_slug,
                project=project_slug,
                action=LifecycleReapAction.retention_expired,
                trigger=LifecycleActionTrigger.purgatory_cleanup,
                success=True,
            )
        )
