"""GitHub webhook endpoint dispatching events to per-event processors."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any

import gidgethub
from fastapi import APIRouter, Depends, HTTPException, Request, status
from gidgethub import sansio
from gidgethub.routing import Router as GidgethubRouter

from docverse.dependencies.context import RequestContext, context_dependency
from docverse.factory import WebhookDispatch
from docverse.services.dashboard_templates import (
    InstallationEventProcessor,
    PushEventProcessor,
    RenameEventProcessor,
)
from docverse.services.ref_deleted_processor import RefDeletedWebhookProcessor
from docverse.storage.github import GitHubAppNotConfiguredError

__all__ = ["router"]

router = APIRouter(include_in_schema=False)
"""FastAPI router for GitHub webhook endpoints.

Mounted under ``config.path_prefix`` from ``main.py`` so the public
URL is ``POST {path_prefix}/webhooks/github``. Excluded from the
OpenAPI schema because the API surface is GitHub's webhook contract,
not the Docverse REST API.
"""


_event_router = GidgethubRouter()
"""Module-level gidgethub router for event-type dispatch.

Using a fresh router per request would defeat gidgethub's intended
registration model and force every callback to re-bind on every
delivery. Holding it at module scope mirrors the pattern in
``safir.github`` and Times Square / Semaphore: register handlers
once, dispatch many.
"""


@_event_router.register("push")
async def _handle_push(
    event: sansio.Event,
    *,
    push: PushEventProcessor,
    context: RequestContext,
    **_unused: Any,
) -> None:
    """Translate a push event into ``dashboard_sync`` enqueues.

    The processor owns transaction-free DB writes through the
    enqueuer; the handler wraps both the binding lookup and the
    enqueue in a single ``session.begin()`` so a failure aborts the
    whole webhook delivery cleanly. ``**_unused`` absorbs the
    rename/installation processors that gidgethub's dispatcher passes
    to every callback uniformly.
    """
    async with context.session.begin():
        jobs = await push.process(event.data)
        await context.session.commit()
    context.logger.info("Processed push webhook", enqueued=len(jobs))


@_event_router.register("repository", action="renamed")
async def _handle_repository_renamed(
    event: sansio.Event,
    *,
    rename: RenameEventProcessor,
    context: RequestContext,
    **_unused: Any,
) -> None:
    """Rewrite display names on bindings + content rows for a renamed repo."""
    async with context.session.begin():
        await rename.process_repository_renamed(event.data)
        await context.session.commit()


@_event_router.register("repository", action="transferred")
async def _handle_repository_transferred(
    event: sansio.Event,
    *,
    rename: RenameEventProcessor,
    context: RequestContext,
    **_unused: Any,
) -> None:
    """Rewrite owner identity on rows for a transferred repo."""
    async with context.session.begin():
        await rename.process_repository_transferred(event.data)
        await context.session.commit()


@_event_router.register("organization", action="renamed")
async def _handle_organization_renamed(
    event: sansio.Event,
    *,
    rename: RenameEventProcessor,
    context: RequestContext,
    **_unused: Any,
) -> None:
    """Rewrite owner login on bindings + content rows for a renamed org."""
    async with context.session.begin():
        await rename.process_organization_renamed(event.data)
        await context.session.commit()


@_event_router.register("installation", action="created")
@_event_router.register("installation", action="deleted")
@_event_router.register("installation", action="suspend")
@_event_router.register("installation", action="unsuspend")
async def _handle_installation(
    event: sansio.Event,
    *,
    installation: InstallationEventProcessor,
    context: RequestContext,
    **_unused: Any,
) -> None:
    """Update binding reachability for installation lifecycle events."""
    async with context.session.begin():
        await installation.process(event.data)
        await context.session.commit()


@_event_router.register("installation_repositories", action="added")
@_event_router.register("installation_repositories", action="removed")
async def _handle_installation_repositories(
    event: sansio.Event,
    *,
    installation: InstallationEventProcessor,
    context: RequestContext,
    **_unused: Any,
) -> None:
    """Backfill project github_*_id when an install's repo scope changes."""
    async with context.session.begin():
        await installation.process_installation_repositories(event.data)
        await context.session.commit()


@_event_router.register("delete")
async def _handle_delete(
    event: sansio.Event,
    *,
    ref_deleted: RefDeletedWebhookProcessor,
    context: RequestContext,
    **_unused: Any,
) -> None:
    """Soft-delete draft editions tracking the deleted branch/tag.

    Wraps :meth:`RefDeletedWebhookProcessor.process` in the same
    ``context.session.begin()`` as the push and rename handlers so a
    failure mid-sweep rolls back the whole delivery's deletions
    atomically.
    """
    async with context.session.begin():
        await ref_deleted.process(event.data)
        await context.session.commit()


@router.post(
    "/webhooks/github",
    status_code=status.HTTP_200_OK,
    summary="GitHub App webhook receiver",
    name="github_webhook",
)
async def post_github_webhook(
    request: Request,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> dict[str, str]:
    """Receive a GitHub webhook delivery and dispatch by event type.

    Returns ``404`` when the GitHub App feature is not configured
    (any of ``github_app_id`` / ``github_app_private_key`` /
    ``github_webhook_secret`` unset). This keeps the URL effectively
    invisible to a misconfigured deployment without surfacing a 5xx
    that would page operators on every GitHub redelivery attempt.

    Returns ``401`` when the request is unsigned or the HMAC does
    not match the configured webhook secret. ``415`` is returned by
    ``gidgethub`` directly when the content-type is wrong.

    Returns ``200`` for all signed deliveries — including event
    types this app does not subscribe to — so GitHub's redelivery
    machinery does not retry deliveries we have intentionally
    chosen not to act on.
    """
    try:
        dispatch: WebhookDispatch = context.factory.create_webhook_dispatch()
    except GitHubAppNotConfiguredError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="GitHub App is not configured",
        ) from exc

    body = await request.body()
    try:
        event = sansio.Event.from_http(
            _lowercase_headers(request.headers),
            body,
            secret=dispatch.webhook_secret,
        )
    except gidgethub.ValidationFailure as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook signature",
        ) from exc

    context.rebind_logger(
        github_event=event.event, github_delivery_id=event.delivery_id
    )

    await _event_router.dispatch(
        event,
        push=dispatch.push,
        rename=dispatch.rename,
        installation=dispatch.installation,
        ref_deleted=dispatch.ref_deleted,
        context=context,
    )

    return {"status": "ok"}


def _lowercase_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Return a dict of request headers with lower-cased keys.

    ``gidgethub.sansio.Event.from_http`` expects a mapping that
    supports lower-cased keys; FastAPI's ``Request.headers`` already
    is case-insensitive but ``Event.from_http`` indexes via
    ``headers["x-github-event"]`` directly, so a plain dict with
    pre-lower-cased keys is the safest contract.
    """
    return {k.lower(): v for k, v in headers.items()}
