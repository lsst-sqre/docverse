"""Flatten an ``asyncio.TaskGroup`` failure back to the error behind it."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

__all__ = ["first_real_error"]


def _iter_leaf_exceptions(
    group: BaseExceptionGroup[BaseException],
) -> Iterator[BaseException]:
    """Yield every non-group leaf exception in ``group``, depth-first."""
    for exc in group.exceptions:
        if isinstance(exc, BaseExceptionGroup):
            yield from _iter_leaf_exceptions(exc)
        else:
            yield exc


def first_real_error(
    group: BaseExceptionGroup[BaseException],
) -> BaseException:
    """Return the first non-cancellation leaf exception in ``group``.

    ``asyncio.TaskGroup`` reports failures as an exception group, but the
    fan-outs that run in one — the keeper-sync copier's worker pool and
    ``build_processing``'s member uploads — replaced ``asyncio.gather``
    calls whose callers (``except Exception`` isolation blocks, Sentry,
    ``queue_jobs.errors``) expect the underlying error itself, an S3
    ``AccessDenied`` say, exactly as ``gather`` raised it. So the group
    is flattened back down to its first real failure. Cancellations are
    skipped: they are the group tearing down siblings, not the cause.

    If two tasks fail close enough together that both land before
    cancellation does, only the first is returned. That matches the
    ``gather`` contract these replace, which likewise surfaced one
    error; the group itself is returned only when *every* leaf is a
    cancellation, which should not happen for a group a ``TaskGroup``
    raised.

    Re-raise the result ``from`` its own ``__cause__``: that suppresses
    the group as ``__context__`` while leaving any chain the raiser
    built deliberately intact, which plain ``from None`` would blank.
    """
    for exc in _iter_leaf_exceptions(group):
        if not isinstance(exc, asyncio.CancelledError):
            return exc
    return group
