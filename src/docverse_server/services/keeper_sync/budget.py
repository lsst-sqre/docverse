"""The cooperative time budget for one keeper-sync slice.

A project too large for one ``keeper_sync_project`` job (``pipelines``
lists ~2,900 LTD editions) syncs across a chain of jobs, each of which
stops walking editions once its :class:`SliceBudget` runs out and hands
the rest to the next job. The budget is *cooperative*:
:meth:`~docverse_server.services.keeper_sync.service.KeeperSyncService.sync_project`
reads it between editions only, so an edition that starts inside the
budget always finishes, however long its copy takes. That is why the
worker sizes the budget well inside the arq job timeout (see
``Configuration.keeper_sync_slice_budget_seconds``).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from typing import Self

__all__ = ["SliceBudget"]


@dataclass(frozen=True)
class SliceBudget:
    """A monotonic deadline that bounds one keeper-sync slice.

    Build one with :meth:`starting_now`. The deadline is a reading of
    ``clock``, :func:`time.monotonic` by default, so a wall-clock step
    (NTP, a VM resume) can neither cut a slice short nor stretch it.
    Tests inject a fake clock to move time by hand.
    """

    deadline: float
    """The ``clock`` reading at which the budget runs out."""

    clock: Callable[[], float] = monotonic
    """The monotonic clock the deadline is read against."""

    @classmethod
    def starting_now(
        cls, seconds: float, *, clock: Callable[[], float] = monotonic
    ) -> Self:
        """Return a budget of ``seconds`` that starts at the call."""
        return cls(deadline=clock() + seconds, clock=clock)

    def remaining(self) -> float:
        """Return the seconds left before the deadline, never negative."""
        return max(0.0, self.deadline - self.clock())

    def exhausted(self) -> bool:
        """Return ``True`` once the deadline has been reached."""
        return self.clock() >= self.deadline
