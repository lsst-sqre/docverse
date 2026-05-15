"""Docverse-specific Sentry initialization."""

from __future__ import annotations

import functools
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
    "initialize_sentry",
    "instrument_arq_task",
]


DocverseSentryComponent = Literal[
    "api", "worker", "worker-keeper-sync", "worker-lifecycle-eval", "cli"
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
    ``setuptools_scm``-derived ``docverse`` package version as the Sentry
    ``release``, then attaches ``service`` and ``component`` global tags so
    every event from this process carries them.
    """
    if not should_enable_sentry():
        return
    _safir_initialize_sentry(release=version("docverse"))
    scope = sentry_sdk.get_global_scope()
    scope.set_tag("service", "docverse")
    scope.set_tag("component", component)


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
