"""Handler-side glue for conditional GET.

:mod:`docverse_server.domain.conditional_get` owns the :rfc:`7232`
semantics as pure functions. This module is the thin layer that binds
them to a request: it reads the two precondition headers, attaches the
validators to the outgoing response, records what happened, and hands
back a ready-made 304 when the caller already holds the current
representation.

Keeping it in one place is what makes "every conditional endpoint
answers the same way" true rather than aspirational — the header
spelling, the empty 304 body, the debug log, and the metrics event are
written once and shared by every endpoint that opts in.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import Response, status

from ..dependencies.context import RequestContext
from ..domain.conditional_get import evaluate_preconditions, format_http_date
from ..metrics import (
    ConditionalGetEndpoint,
    ConditionalGetEvent,
    ConditionalGetOutcome,
    ConditionalGetPrecondition,
)

__all__ = ["evaluate_conditional_get"]


async def evaluate_conditional_get(
    context: RequestContext,
    *,
    endpoint: ConditionalGetEndpoint,
    organization: str,
    project: str | None = None,
    etag: str,
    last_modified: datetime,
    now: datetime,
) -> Response | None:
    """Apply a request's preconditions and set the response validators.

    Call this as early in the handler as the validator material allows —
    the point of a conditional GET is to skip the expensive query, so a
    handler that computes its watermark, calls this, and returns the
    result when it is not ``None`` never pays for the representation it
    was about to throw away.

    Parameters
    ----------
    context
        The request context. ``ETag`` and ``Last-Modified`` are set on
        its response, so the 200 path needs no further header work.
    endpoint
        Which endpoint is being evaluated, for the metrics event.
    organization
        Slug of the organization the resource belongs to.
    project
        Slug of the project, for project-scoped endpoints.
    etag
        The entity-tag of the representation this request would return.
    last_modified
        The resource watermark, at full precision; it is truncated to
        the second on the way into the header.
    now
        The handler's current instant, timezone-aware. A date-only
        precondition is refused while the watermark's second is still
        open, because a write landing later in that same second would
        be invisible to a comparison of truncated dates; see
        :func:`~docverse_server.domain.conditional_get
        .evaluate_preconditions`. Passed in rather than read here so
        one handler's clock is one instant, whatever else it goes on
        to compare it against.

    Returns
    -------
    fastapi.Response or None
        A bodyless ``304 Not Modified`` carrying both validators, which
        the handler must return as-is; or ``None`` when the handler
        should go on and build the full representation.

    Notes
    -----
    The 304 sets its headers itself rather than relying on the ones
    just written to ``context.response``: FastAPI discards the injected
    response's headers whenever a handler returns a ``Response``
    object.

    The metrics event is published inside whatever transaction the
    handler is in. That is safe — publishing is best-effort in
    production (``raise_on_error=False``) and this is a read path — and
    it is the only way to emit before the 304 short-circuits.
    """
    http_date = format_http_date(last_modified)
    result = evaluate_preconditions(
        if_none_match=context.request.headers.get("If-None-Match"),
        if_modified_since=context.request.headers.get("If-Modified-Since"),
        etag=etag,
        last_modified=last_modified,
        now=now,
    )
    outcome = ConditionalGetOutcome.from_not_modified(
        not_modified=result.not_modified
    )
    context.logger.debug(
        "Evaluated conditional GET",
        endpoint=endpoint.value,
        organization=organization,
        project=project,
        outcome=outcome.value,
        precondition=(
            result.precondition.value
            if result.precondition is not None
            else None
        ),
        watermark=last_modified.isoformat(),
    )
    if result.precondition is not None:
        await context.events.conditional_get.publish(
            ConditionalGetEvent(
                organization=organization,
                project=project,
                endpoint=endpoint,
                outcome=outcome,
                precondition=ConditionalGetPrecondition.from_domain(
                    result.precondition
                ),
            )
        )

    context.response.headers["ETag"] = etag
    context.response.headers["Last-Modified"] = http_date
    if not result.not_modified:
        return None
    return Response(
        status_code=status.HTTP_304_NOT_MODIFIED,
        headers={"ETag": etag, "Last-Modified": http_date},
    )
