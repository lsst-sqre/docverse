"""Per-test reset for the mocked metrics publishers.

``METRICS_MOCK`` resolves the application's event manager to a
`safir.metrics.MockEventManager`, whose publishers record every payload
they are handed so tests can assert on them. The manager is built once
during application startup, and startup now happens once per pytest
process rather than once per test — so without an explicit reset the
recorded payloads accumulate for the whole session and every
``assert len(publisher.published) == 1`` sees the entire suite's
history instead of its own test's.

Only the recorded payloads are dropped: the publisher objects keep their
identity, so a test that captured ``events.build_uploaded`` before
triggering work still reads back the right list.
"""

from __future__ import annotations

from safir.metrics import MockEventPublisher

from docverse_server.metrics.events import DocverseEvents

__all__ = ["reset_mock_event_publishers"]


def reset_mock_event_publishers(events: DocverseEvents) -> None:
    """Drop every payload recorded by an event maker's mock publishers.

    Publishers are assigned as instance attributes by
    :meth:`~docverse_server.metrics.events.DocverseEvents.initialize`, so
    iterating the instance dictionary covers every registered event type
    without this helper having to track the list.

    Parameters
    ----------
    events
        The initialized event maker whose publishers to clear.
    """
    for publisher in vars(events).values():
        if isinstance(publisher, MockEventPublisher):
            publisher.published.clear()
