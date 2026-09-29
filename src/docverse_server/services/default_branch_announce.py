"""Announce a ``__main`` rewrite the way a ``PATCH`` of it would be.

Every trigger that hands a repository's default branch to
:class:`~docverse_server.services.default_branch.DefaultBranchService`
— the ``repository.edited`` webhook, the ``project_github_resolve``
worker, and the daily ``git_ref_audit`` — follows the commit of a
rewrite with the same two steps. :func:`announce_main_rewrite` is those
steps, so the three cannot drift apart.

It lives beside the service rather than in it because it runs after the
caller's commit, outside the transaction the service writes in. It logs
nothing itself: the one line worth an operator's attention, a failed
``dashboard_build`` enqueue, comes from
:func:`~docverse_server.services.dashboard.enqueue.try_enqueue_dashboard_build_by_slug`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse_server.metrics import (
    DocverseEvents,
    EditionLifecycleEvent,
    LifecycleAction,
    MetricsEditionKind,
)
from docverse_server.services.dashboard.enqueue import (
    try_enqueue_dashboard_build_by_slug,
)
from docverse_server.services.default_branch import DefaultBranchOutcome

if TYPE_CHECKING:
    # ``factory`` imports ``services.default_branch`` at runtime.
    from docverse_server.factory import Factory

__all__ = ["announce_main_rewrite"]


async def announce_main_rewrite(
    *,
    factory: Factory,
    session: AsyncSession,
    events: DocverseEvents | None,
    logger: structlog.stdlib.BoundLogger,
    org_slug: str,
    project_slug: str,
    outcome: DefaultBranchOutcome,
) -> bool:
    """Publish the ``update`` event and rebuild the dashboard.

    Does nothing unless ``outcome.main_rewritten``: recording the column
    alone is not a change to ``__main``. For a rewrite, publishes one
    ``edition_lifecycle`` ``update`` event for the ``main`` edition, then
    enqueues one ``dashboard_build`` for the project, the latter in its
    own transaction so an enqueue failure cannot undo the convergence.

    The caller has already committed the convergence and handed the
    deferred ``publish_edition`` job to arq with
    ``queue_dispatcher.dispatch()``; that dispatch stays with the
    caller because it is owed whether or not ``__main`` was rewritten.

    Parameters
    ----------
    factory
        The caller's factory, for the dashboard enqueuer.
    session
        The caller's session, with no transaction open.
    events
        The metrics events, or ``None`` in a worker running without
        them, which skips the event and still enqueues the rebuild.
    logger
        Logger for a failed ``dashboard_build`` enqueue.
    org_slug
        The project's organization.
    project_slug
        The project whose ``__main`` was rewritten.
    outcome
        What the service's ``apply`` changed.

    Returns
    -------
    bool
        ``True`` when a ``dashboard_build`` was enqueued; ``False`` when
        ``__main`` was not rewritten, the project already had one
        queued or running, or the enqueue failed. Callers that count
        their enqueued jobs sum this.
    """
    if not outcome.main_rewritten:
        return False
    if events is not None:
        await events.edition_lifecycle.publish(
            EditionLifecycleEvent(
                organization=org_slug,
                project=project_slug,
                action=LifecycleAction.update,
                edition_kind=MetricsEditionKind.main,
            )
        )
    return await try_enqueue_dashboard_build_by_slug(
        factory=factory,
        session=session,
        logger=logger,
        org_slug=org_slug,
        project_slug=project_slug,
    )
