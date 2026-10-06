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

:class:`SliceProgress` is the walk's live position, which ``sync_project``
keeps current as it goes so a job interrupted part way through a slice
can still say how far it got.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from typing import Self

__all__ = ["SliceBudget", "SliceProgress"]


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


@dataclass
class SliceProgress:
    """How far one keeper-sync slice has walked LTD's edition list, live.

    Pass one to
    :meth:`~docverse_server.services.keeper_sync.service.KeeperSyncService.sync_project`
    and it is updated in place as the walk proceeds: the totals once the
    walk is planned, the visit counters after each edition. Unlike the
    walk fields of the
    :class:`~docverse_server.services.keeper_sync.service.ProjectSyncResult`
    that call returns, it can be read at any moment — which is what the
    worker's cancellation path needs, since a cancelled call returns
    nothing.
    """

    editions_total: int | None = None
    """How many editions LTD lists, ``None`` until the walk is planned."""

    editions_to_visit: int = 0
    """How many editions this slice's walk holds.

    LTD's list less the editions an earlier slice of the chain already
    visited (everything up to and including the resume cursor).
    """

    editions_visited: int = 0
    """How many editions the walk has dealt with so far."""

    last_visited_ltd_edition_id: int | None = None
    """LTD id of the last edition visited, ``None`` before the first."""

    @property
    def editions_remaining(self) -> int | None:
        """Editions of this walk not visited yet, ``None`` until planned.

        After a slice stopped at its budget, these are what the next
        slice of the chain picks up.
        """
        if self.editions_total is None:
            return None
        return self.editions_to_visit - self.editions_visited

    def start_walk(
        self, *, editions_total: int, editions_to_visit: int
    ) -> None:
        """Record the planned walk's size before its first edition."""
        self.editions_total = editions_total
        self.editions_to_visit = editions_to_visit

    def record_visit(self, ltd_edition_id: int) -> None:
        """Move past one edition the walk dealt with."""
        self.editions_visited += 1
        self.last_visited_ltd_edition_id = ltd_edition_id
