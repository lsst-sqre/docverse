"""GitHub webhook endpoint dispatching push events to the sync queue."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated

import gidgethub
from fastapi import APIRouter, Depends, HTTPException, Request, status
from gidgethub import sansio
from gidgethub.routing import Router as GidgethubRouter

from docverse.dependencies.context import RequestContext, context_dependency
from docverse.services.dashboard_templates import PushEventProcessor
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
    processor: PushEventProcessor,
    context: RequestContext,
) -> None:
    """Translate a push event into ``dashboard_sync`` enqueues.

    The processor owns transaction-free DB writes through the
    enqueuer; the handler wraps both the binding lookup and the
    enqueue in a single ``session.begin()`` so a failure aborts the
    whole webhook delivery cleanly.
    """
    async with context.session.begin():
        jobs = await processor.process(event.data)
        await context.session.commit()
    context.logger.info("Processed push webhook", enqueued=len(jobs))


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
        secret, processor = context.factory.create_webhook_dispatch()
    except GitHubAppNotConfiguredError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="GitHub App is not configured",
        ) from exc

    body = await request.body()
    try:
        event = sansio.Event.from_http(
            _lowercase_headers(request.headers), body, secret=secret
        )
    except gidgethub.ValidationFailure as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook signature",
        ) from exc

    context.rebind_logger(
        github_event=event.event, github_delivery_id=event.delivery_id
    )

    await _event_router.dispatch(event, processor=processor, context=context)

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
