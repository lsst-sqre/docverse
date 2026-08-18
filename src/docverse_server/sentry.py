"""Docverse-specific Sentry initialization."""

from __future__ import annotations

import functools
from collections.abc import Mapping
from importlib.metadata import version
from typing import Any, Literal

import sentry_sdk
from arq.typing import WorkerCoroutine
from safir.sentry import initialize_sentry as _safir_initialize_sentry
from safir.sentry import should_enable_sentry
from sentry_sdk.consts import OP
from sentry_sdk.tracing import TransactionSource

__all__ = [
    "DocverseSentryComponent",
    "capture_warning",
    "initialize_sentry",
    "instrument_arq_task",
]


DocverseSentryComponent = Literal[
    "api", "worker", "worker-keeper-sync", "worker-maintenance", "cli"
]
"""Tag values for the ``component`` Sentry global tag.

One label per Docverse entry point so events from the FastAPI app, the two
arq worker pools, and ``docverse-admin`` can be filtered apart on Sentry.
"""


def initialize_sentry(component: DocverseSentryComponent) -> None:
    """Initialize Sentry for one Docverse process.

    A no-op when ``SENTRY_DSN`` is unset, so local development, CI, and
    ``nox -s test`` runs never report. When the env var is set, delegates
    to :func:`safir.sentry.initialize_sentry` with the
    ``setuptools_scm``-derived ``docverse-server`` package version as the
    Sentry ``release``, then attaches ``service`` and ``component`` global
    tags so
    every event from this process carries them.
    """
    if not should_enable_sentry():
        return
    _safir_initialize_sentry(release=version("docverse-server"))
    scope = sentry_sdk.get_global_scope()
    scope.set_tag("service", "docverse")
    scope.set_tag("component", component)


def capture_warning(
    message: str,
    *,
    tags: Mapping[str, str] | None = None,
    contexts: Mapping[str, dict[str, Any]] | None = None,
) -> None:
    """Capture a warning-level Sentry event for a non-exceptional condition.

    Docverse's other Sentry reports all start from an exception, where
    Safir's ``before_send_handler`` merges
    :meth:`~docverse_server.exceptions.DocverseSlackException.to_sentry`
    metadata onto the event automatically. Some conditions an operator
    still needs to see are not exceptions at all — the caller absorbed
    them deliberately — so this helper is the seam that reports them
    without inventing an exception class nobody raises.

    ``message`` becomes the Sentry issue title and is therefore also the
    grouping key: pass a **constant** string and put the per-event
    identifiers in ``tags`` / ``contexts``, or every occurrence opens its
    own issue. The tags/contexts split follows the same cardinality rule
    as the ``to_sentry`` overrides — low-cardinality, searchable values
    (states, kinds, queue names) in ``tags``; per-event snapshots
    (public ids, payload detail) in ``contexts``.

    A no-op when Sentry is uninitialised (no ``SENTRY_DSN``), like every
    other SDK call in the tree.

    Parameters
    ----------
    message
        Constant, human-readable summary; becomes the Sentry issue title.
    tags
        Low-cardinality searchable key/value pairs.
    contexts
        Named structured snapshots attached to the event.
    """
    with sentry_sdk.new_scope() as scope:
        for tag_key, tag_value in (tags or {}).items():
            scope.set_tag(tag_key, tag_value)
        for context_key, context_value in (contexts or {}).items():
            scope.set_context(context_key, context_value)
        sentry_sdk.capture_message(message, level="warning")


def instrument_arq_task(fn: WorkerCoroutine) -> WorkerCoroutine:
    """Wrap an arq task so Sentry events carry the function name.

    arq has no built-in Sentry integration (unlike the SDK-bundled
    integrations for Celery, RQ, Huey, etc.), so without this wrapper
    every captured event from a worker function lands under
    ``transaction: "unknown arq task"`` -- which Sentry also surfaces as
    the ``culprit`` -- and the only way to know which task fired is to
    read the breadcrumb logger name. The wrapper opens an isolation
    scope and a top-level transaction named after the wrapped function
    for the duration of one job; the arq ``job_id`` is attached as a tag
    so a captured event can be cross-referenced against pod logs.

    The wrapped function preserves ``__name__``, ``__qualname__``, and
    ``__module__`` via :func:`functools.wraps`, so :func:`arq.func` and
    arq's default registration both record the original task name as
    the Redis-side ``job_type`` -- no enqueue call sites need to change.
    """
    # mypy can't structurally match ``Callable[..., Awaitable[Any]]``
    # against the ``WorkerCoroutine`` Protocol without help, so the
    # wrapper is typed against the Protocol directly.
    fn_name = fn.__qualname__.rsplit(".", 1)[-1]

    @functools.wraps(fn)
    async def wrapper(ctx: dict[Any, Any], *args: Any, **kwargs: Any) -> Any:
        with sentry_sdk.isolation_scope() as scope:
            job_id = ctx.get("job_id")
            if job_id is not None:
                scope.set_tag("arq.job_id", str(job_id))
            job_try = ctx.get("job_try")
            if job_try is not None:
                scope.set_tag("arq.job_try", str(job_try))
            with sentry_sdk.start_transaction(
                name=fn_name,
                op=OP.QUEUE_TASK_ARQ,
                source=TransactionSource.TASK,
            ):
                try:
                    return await fn(ctx, *args, **kwargs)
                except Exception as exc:
                    # arq has no Sentry integration to capture uncaught
                    # exceptions, so the wrapper does it explicitly. Tasks
                    # whose outer except block already calls
                    # ``sentry_sdk.capture_exception`` (see
                    # ``worker/functions/keeper_sync.py``) are not
                    # double-counted: ``DedupeIntegration`` (a default
                    # ``sentry_sdk.init`` integration) drops the second
                    # capture of the same exception instance.
                    sentry_sdk.capture_exception(exc)
                    raise

    return wrapper
