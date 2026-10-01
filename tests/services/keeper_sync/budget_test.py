"""Unit tests for :class:`SliceBudget`."""

from __future__ import annotations

from dataclasses import dataclass

from docverse_server.services.keeper_sync import SliceBudget


@dataclass
class _FakeClock:
    """A monotonic clock a test moves by hand."""

    now: float = 1000.0

    def __call__(self) -> float:
        return self.now


def test_budget_counts_down_from_its_start() -> None:
    clock = _FakeClock()
    budget = SliceBudget.starting_now(300, clock=clock)

    assert budget.remaining() == 300
    assert not budget.exhausted()

    clock.now += 120
    assert budget.remaining() == 180
    assert not budget.exhausted()


def test_budget_is_exhausted_at_its_deadline() -> None:
    clock = _FakeClock()
    budget = SliceBudget.starting_now(300, clock=clock)

    clock.now += 300
    assert budget.exhausted()
    assert budget.remaining() == 0


def test_budget_never_reports_negative_time_remaining() -> None:
    clock = _FakeClock()
    budget = SliceBudget.starting_now(300, clock=clock)

    clock.now += 900
    assert budget.exhausted()
    assert budget.remaining() == 0
