"""arq worker function for the ``edition_reconcile`` per-org pass.

The dispatcher (task #615) writes one ``queue_jobs`` row per
organization with ``kind='edition_reconcile'`` and
``subject_label=org.slug``; this worker is the per-org body of that
fan-out. For one org it reads every edition's recorded publish state,
reads back what the org's CDN actually serves, and repairs each
disagreement — a lost or stalled publish goes back on the default queue,
a key that outlived its edition is deleted.

Two properties shape the job, and both come from what it does *not* do.

It never publishes anything itself. A republish is an enqueue, so the
ordinary publish path — with its edition lock, its deleted-build guard
and its own retries — remains the only thing that writes a pointer. That
is why the job can run with ``max_tries=1`` and no compensation logic: a
tick that dies halfway has applied a prefix of its plan and left the
rest for the next tick, which re-plans from current state rather than
resuming a stored one.

It also never *clears* a publish state. Nothing here writes ``failed``
or rolls a pair back; the only rows it touches are the ones
``enqueue_publish_for_edition`` writes on its behalf. So the worst a
buggy tick can do is enqueue a redundant publish, which the publish path
treats as an ordinary republish of the build the edition already points
at, or delete a key for an edition the database has already tombstoned.

The transaction shape follows :mod:`purgatory_cleanup`: the
late-delivery guard runs first and alone, the service owns its own short
read transaction, and the terminal transition is committed at the end.

What the tick tells the world (task #618) is three channels with three
audiences, all fed from the one tally and all after the terminal commit:
the ``queue_jobs`` row's ``progress`` for whoever is looking at this
job, an org-scoped ``edition_reconcile_completed`` event for the
dashboards, and — only when the tick actually repaired something — a
warning log naming the editions and one Sentry message per org tick.
The asymmetry is deliberate: the event is the loop's proof of life and
has to fire on every tick, while the log and the page are about a
failure elsewhere in the system that this tick happened to clean up
after, and there is nothing to say about an org that has not drifted.
"""

from __future__ import annotations

import time
import traceback
from datetime import timedelta
from typing import Any

import sentry_sdk
import structlog
from safir.dependencies.db_session import db_session_dependency

from docverse_server.config import config
from docverse_server.exceptions import NotFoundError
from docverse_server.metrics import EditionReconcileCompletedEvent
from docverse_server.sentry import capture_warning
from docverse_server.services.edition_reconcile import EditionReconcileOutcome

__all__ = ["RECONCILED_DRIFT_MESSAGE", "edition_reconcile"]

RECONCILED_DRIFT_MESSAGE = "Edition reconciliation repaired drifted editions"
"""Sentry issue title for a tick that had to repair something.

Constant, and therefore the grouping key: every drifted organization's
tick lands in one issue whose events an operator filters by the
``organization`` tag, rather than a new issue per org per half hour.
"""


async def edition_reconcile(
    ctx: dict[str, Any], payload: dict[str, Any]
) -> str:
    """Converge one organization's editions with what its CDN serves.

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
        ``"completed"`` when every planned action was applied, and
        ``"completed_with_errors"`` when at least one action failed. A
        failure that makes the whole tick impossible marks the queue job
        ``failed`` and re-raises so arq logs the job as failed.
    """
    org_id: int = payload["org_id"]
    org_slug: str = payload["org_slug"]
    queue_job_id: int = payload["queue_job_id"]
    logger = structlog.get_logger(
        "docverse_server.worker.edition_reconcile"
    ).bind(org=org_slug)
    started = time.monotonic()

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()

        async with session.begin():
            # Late-delivery guard, ahead of every read: a reaper may
            # already have failed this row and released the org's mutex,
            # in which case another reconciler may be running right now.
            if await queue_job_store.start_if_queued(queue_job_id) is None:
                return "skipped"

        try:
            async with session.begin():
                org = await factory.create_org_store().get_by_id(org_id)
            if org is None:
                msg = f"Organization {org_id} not found"
                raise NotFoundError(msg)
            service = factory.create_edition_reconcile_service()
            outcome = await service.reconcile_org(
                org, limit=config.edition_reconcile_max_actions_per_job
            )
        except Exception as exc:
            logger.exception("Edition reconciliation failed for org")
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

        async with session.begin():
            await queue_job_store.update_progress(
                queue_job_id, outcome.as_progress()
            )
            await queue_job_store.complete(
                queue_job_id, has_errors=outcome.has_errors
            )
        await _report_tick(
            ctx=ctx,
            org_slug=org_slug,
            outcome=outcome,
            started=started,
            logger=logger,
        )
        return "completed_with_errors" if outcome.has_errors else "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _report_tick(
    *,
    ctx: dict[str, Any],
    org_slug: str,
    outcome: EditionReconcileOutcome,
    started: float,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Log, page, and publish what this tick did.

    Called once the ``queue_jobs`` row's terminal transition has
    committed, so every channel here stands on a durable write and none
    of them can describe repairs the database never recorded.

    A tick that changed nothing is the steady state — every org, twice
    an hour — so it goes out at debug and reaches nobody. A tick that
    re-drove a publish or removed a stranded key means something else in
    the system lost work; the loop has already repaired what it can, but
    the *why* is beyond it, so that case earns both a warning naming the
    editions and one Sentry message. One message per org tick, never one
    per edition: an org that lost a hundred keys lost them to one cause,
    and a message each would bury it.
    """
    if outcome.acted:
        log_payload = outcome.as_log_payload()
        logger.warning("Reconciled drifted editions", **log_payload)
        capture_warning(
            RECONCILED_DRIFT_MESSAGE,
            tags={"organization": org_slug},
            # The counts and the edition names are a per-event snapshot,
            # so they ride in a context rather than as tags — the same
            # cardinality rule the ``to_sentry`` overrides follow.
            contexts={"edition_reconcile": log_payload},
        )
    else:
        logger.debug("No edition drift to reconcile", **outcome.as_progress())
    await _publish_tick_event(
        ctx=ctx,
        org_slug=org_slug,
        outcome=outcome,
        started=started,
        logger=logger,
    )


async def _publish_tick_event(
    *,
    ctx: dict[str, Any],
    org_slug: str,
    outcome: EditionReconcileOutcome,
    started: float,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Emit this tick's org-scoped completion event.

    Published from the same tally the queue row just recorded. Every
    tick publishes, drift or not: the loop's whole claim is that the
    edge still agrees with the database, and a reconciler that goes
    quiet when it finds nothing is indistinguishable from one that
    stopped running.

    Best-effort in the shape ``_publish_edition_published`` uses rather
    than the bare call ``purgatory_cleanup`` makes. The difference is
    what has already happened by this point: the tick's repairs are
    committed and its ``queue_jobs`` row already reads ``completed``, and
    the job runs ``max_tries=1``, so an exception escaping here would
    make arq record a failed run of a tick that finished — a permanent
    disagreement between the two places an operator looks, bought for a
    metrics hiccup. Reported the way any swallowed error is (Sentry plus
    an exception log) so the outage is still visible as itself. Skips
    silently when the process has no event manager (tests that do not
    assert on metrics).
    """
    events = ctx.get("events")
    if events is None:
        return
    try:
        await events.edition_reconcile_completed.publish(
            EditionReconcileCompletedEvent(
                organization=org_slug,
                project=None,
                editions_scanned=outcome.editions_scanned,
                pointers_read=outcome.pointers_read,
                republished=outcome.republished,
                unpublished=outcome.unpublished,
                in_flight_skipped=outcome.in_flight_skipped,
                failed_left_alone=outcome.failed_left_alone,
                unexpected_pointers=outcome.unexpected_pointers,
                capped=outcome.capped,
                cdn_checked=outcome.cdn_checked,
                elapsed=timedelta(seconds=time.monotonic() - started),
            )
        )
    except Exception as exc:
        sentry_sdk.capture_exception(exc)
        logger.exception("Failed to publish edition_reconcile_completed")
