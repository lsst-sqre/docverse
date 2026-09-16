"""Handler-side glue for conditional GET.

:mod:`docverse_server.domain.conditional_get` owns the :rfc:`9110`
semantics as pure functions. This module is the thin layer that binds
them to a request: it reads the precondition header, attaches the
``ETag`` to the outgoing response, records what happened, and hands
back a ready-made 304 when the caller already holds the current
representation.

Keeping it in one place is what makes "every conditional endpoint
answers the same way" true rather than aspirational — the header
spelling, the empty 304 body, the debug log, and the metrics event are
written once and shared by every endpoint that opts in.
"""

from __future__ import annotations

from fastapi import Response, status

from ..dependencies.context import RequestContext
from ..domain.conditional_get import evaluate_preconditions
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
) -> Response | None:
    """Apply a request's preconditions and set the response validator.

    Call this as early in the handler as the validator material allows —
    the point of a conditional GET is to skip the expensive query, so a
    handler that computes its watermark, calls this, and returns the
    result when it is not ``None`` never pays for the representation it
    was about to throw away.

    Parameters
    ----------
    context
        The request context. ``ETag`` is set on its response, so the
        200 path needs no further header work.
    endpoint
        Which endpoint is being evaluated, for the metrics event.
    organization
        Slug of the organization the resource belongs to.
    project
        Slug of the project, for project-scoped endpoints.
    etag
        The entity-tag of the representation this request would return.
        The only validator Docverse publishes: no ``Last-Modified`` is
        sent and any ``If-Modified-Since`` is ignored, for the reasons
        in :mod:`docverse_server.domain.conditional_get`.

    Returns
    -------
    fastapi.Response or None
        A bodyless ``304 Not Modified`` carrying the ``ETag``, which
        the handler must return as-is; or ``None`` when the handler
        should go on and build the full representation.

    Notes
    -----
    The 304 sets its header itself rather than relying on the one just
    written to ``context.response``: FastAPI discards the injected
    response's headers whenever a handler returns a ``Response``
    object.

    The metrics event is published inside whatever transaction the
    handler is in. That is safe — publishing is best-effort in
    production (``raise_on_error=False``) and this is a read path — and
    it is the only way to emit before the 304 short-circuits.
    """
    result = evaluate_preconditions(
        if_none_match=context.request.headers.get("If-None-Match"), etag=etag
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
        etag=etag,
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
    if not result.not_modified:
        return None
    return Response(
        status_code=status.HTTP_304_NOT_MODIFIED, headers={"ETag": etag}
    )
