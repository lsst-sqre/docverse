"""Cancel a worker function mid-flight the way arq does.

arq runs each job in its own task and cancels that task on its per-job
timeout or on a worker shutdown, so the job function sees
:exc:`asyncio.CancelledError` at whatever it was awaiting. Tests of the
cancellation helper's wiring (PRD #765) park a worker function on a
:class:`HangUntilCancelled` stand-in — an LTD route, a publisher, an arq
enqueue — then cancel its task with :func:`cancel_when_reached` once the
stand-in has been reached.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, NoReturn

import pytest

__all__ = ["HangUntilCancelled", "cancel_when_reached"]


class HangUntilCancelled:
    """An async callable that blocks until its caller is cancelled.

    Usable wherever a test needs a coroutine to park on: a respx
    ``side_effect``, a monkeypatched store or client method. Any
    arguments are accepted and ignored.
    """

    def __init__(self) -> None:
        self.reached = asyncio.Event()
        """Set once a call arrives, so a test can cancel on cue."""

    async def __call__(self, *args: Any, **kwargs: Any) -> NoReturn:
        self.reached.set()
        await asyncio.Event().wait()
        msg = "unreachable: the call is cancelled while it hangs"
        raise AssertionError(msg)


async def cancel_when_reached(
    task: asyncio.Task[Any],
    reached: asyncio.Event,
    *,
    before_cancel: Callable[[], Awaitable[None]] | None = None,
) -> None:
    """Cancel ``task`` once it reaches the call it hangs on.

    ``before_cancel`` runs between the two — say, to backdate the job's
    ``date_started`` so the cancel reads as a timeout. The
    ``CancelledError`` must come back out of the task, as arq needs it
    to record the job as failed.
    """
    waiter = asyncio.create_task(reached.wait())
    await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
    waiter.cancel()
    if task.done():
        # The function returned before it ever hung: surface why rather
        # than waiting forever for a signal that will not come.
        await task
        pytest.fail("the job returned before it was cancelled")
    if before_cancel is not None:
        await before_cancel()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
