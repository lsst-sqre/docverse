"""Unit tests for ``tests.support.arq_testing`` helpers."""

from __future__ import annotations

import pytest
from safir.arq import MockArqQueue

from tests.support.arq_testing import (
    count_jobs_by_name,
    get_jobs_by_name,
    queue_names,
    register_queue,
)


def _two_queue_mock() -> MockArqQueue:
    """Build a mock with two registered queue names."""
    queue = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(queue, "other:queue")
    return queue


@pytest.mark.asyncio
async def test_helpers_filter_across_all_queues_by_default() -> None:
    """With ``queue_name=None``, helpers union across every queue."""
    queue = _two_queue_mock()
    await queue.enqueue("alpha", _queue_name="docverse:queue", payload={})
    await queue.enqueue("alpha", _queue_name="other:queue", payload={})
    await queue.enqueue("beta", _queue_name="docverse:queue", payload={})

    assert count_jobs_by_name(queue, "alpha") == 2
    assert count_jobs_by_name(queue, "beta") == 1
    assert count_jobs_by_name(queue, "missing") == 0
    assert {j.queue_name for j in get_jobs_by_name(queue, "alpha")} == {
        "docverse:queue",
        "other:queue",
    }


@pytest.mark.asyncio
async def test_helpers_restrict_to_named_queue() -> None:
    """With ``queue_name`` set, helpers only see that queue's jobs."""
    queue = _two_queue_mock()
    await queue.enqueue("alpha", _queue_name="docverse:queue", payload={})
    await queue.enqueue("alpha", _queue_name="other:queue", payload={})

    assert (
        count_jobs_by_name(queue, "alpha", queue_name="docverse:queue") == 1
    )
    assert count_jobs_by_name(queue, "alpha", queue_name="missing:queue") == 0
    other_only = get_jobs_by_name(queue, "alpha", queue_name="other:queue")
    assert [j.queue_name for j in other_only] == ["other:queue"]


@pytest.mark.asyncio
async def test_queue_names_returns_observed_queues() -> None:
    """``queue_names`` returns every queue the mock has stored jobs under.

    The default queue is registered on construction, so it is always
    present even before the first ``enqueue``.
    """
    queue = _two_queue_mock()
    assert queue_names(queue) == {"docverse:queue", "other:queue"}
    await queue.enqueue("alpha", _queue_name="docverse:queue", payload={})
    assert queue_names(queue) == {"docverse:queue", "other:queue"}
