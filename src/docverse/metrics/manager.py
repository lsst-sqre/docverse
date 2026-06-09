"""Construction of the Docverse metrics event manager.

:func:`build_event_manager` is the single place that turns the
application's :class:`~docverse.config.Configuration` into a started
:class:`safir.metrics.EventManager` plus a registered
:class:`~docverse.metrics.events.DocverseEvents`. Both the FastAPI
lifespan and each arq worker ``on_startup`` call it so the wiring stays
identical across every process that publishes events.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from safir.metrics import EventManager
from structlog.stdlib import BoundLogger

from .events import DocverseEvents

if TYPE_CHECKING:
    from ..config import Configuration

__all__ = ["build_event_manager"]


async def build_event_manager(
    config: Configuration,
    *,
    logger: BoundLogger | None = None,
) -> tuple[EventManager, DocverseEvents]:
    """Build and initialize the event manager and the Docverse events.

    Parameters
    ----------
    config
        The application configuration. Its ``metrics`` field selects the
        concrete manager (mock, disabled, or Kafka-backed).
    logger
        Logger for the manager's internal logging. Defaults to the
        ``safir.metrics`` logger when omitted.

    Returns
    -------
    tuple of (EventManager, DocverseEvents)
        The started manager and the events maker with every publisher
        registered. The caller owns the manager's lifetime and must
        ``await manager.aclose()`` on shutdown.
    """
    manager = config.metrics.make_manager(logger=logger)
    await manager.initialize()
    events = DocverseEvents()
    await events.initialize(manager)
    return manager, events
