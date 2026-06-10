"""The Docverse application-metrics event registry.

:class:`DocverseEvents` is the single :class:`safir.metrics.EventMaker`
for the whole application. Its :meth:`~DocverseEvents.initialize` is
called once per process — from the FastAPI lifespan and from each arq
worker ``on_startup`` — to register one publisher per event type against
the process-wide :class:`safir.metrics.EventManager`.
"""

from __future__ import annotations

from safir.dependencies.metrics import EventMaker
from safir.metrics import EventManager, EventPublisher

from .payloads import (
    BuildProcessedEvent,
    BuildUploadedEvent,
    DashboardBuiltEvent,
    EditionLifecycleEvent,
    EditionPublishedEvent,
    KeeperSyncRunCompletedEvent,
    MembershipChangedEvent,
    ProjectLifecycleEvent,
)

__all__ = ["DocverseEvents"]


class DocverseEvents(EventMaker):
    """Container of every Docverse metrics event publisher.

    The publisher attributes are assigned in :meth:`initialize`; until
    that runs they are absent, so a process must initialize the maker
    before publishing. The attribute names double as the Avro schema /
    event names registered with the schema manager.
    """

    build_uploaded: EventPublisher[BuildUploadedEvent]
    build_processed: EventPublisher[BuildProcessedEvent]
    dashboard_built: EventPublisher[DashboardBuiltEvent]
    edition_published: EventPublisher[EditionPublishedEvent]
    project_lifecycle: EventPublisher[ProjectLifecycleEvent]
    edition_lifecycle: EventPublisher[EditionLifecycleEvent]
    membership_changed: EventPublisher[MembershipChangedEvent]
    keeper_sync_run_completed: EventPublisher[KeeperSyncRunCompletedEvent]

    async def initialize(self, manager: EventManager) -> None:
        """Register a publisher for every Docverse event type.

        Parameters
        ----------
        manager
            An initialized :class:`safir.metrics.EventManager`.
        """
        self.build_uploaded = await manager.create_publisher(
            "build_uploaded", BuildUploadedEvent
        )
        self.build_processed = await manager.create_publisher(
            "build_processed", BuildProcessedEvent
        )
        self.dashboard_built = await manager.create_publisher(
            "dashboard_built", DashboardBuiltEvent
        )
        self.edition_published = await manager.create_publisher(
            "edition_published", EditionPublishedEvent
        )
        self.project_lifecycle = await manager.create_publisher(
            "project_lifecycle", ProjectLifecycleEvent
        )
        self.edition_lifecycle = await manager.create_publisher(
            "edition_lifecycle", EditionLifecycleEvent
        )
        self.membership_changed = await manager.create_publisher(
            "membership_changed", MembershipChangedEvent
        )
        self.keeper_sync_run_completed = await manager.create_publisher(
            "keeper_sync_run_completed", KeeperSyncRunCompletedEvent
        )
