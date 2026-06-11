"""Service for the daily resource-inventory census (SQR-112)."""

from __future__ import annotations

import structlog

from docverse.domain.inventory_census import InventoryCensus
from docverse.storage.inventory_census_store import InventoryCensusStore

__all__ = ["InventoryCensusService"]


class InventoryCensusService:
    """Take a read-only census of active Docverse resources.

    A thin wrapper over :class:`InventoryCensusStore`, mirroring the
    other read-only run services (e.g.
    :class:`docverse.services.keeper_sync_run.KeeperSyncRunService`): the
    store owns the grouped-aggregate SQL and this service is the
    factory-wired entry point the daily ``inventory_census`` worker
    calls.
    """

    def __init__(
        self,
        *,
        store: InventoryCensusStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._store = store
        self._logger = logger

    async def take_census(self) -> InventoryCensus:
        """Return one census snapshot of active orgs/projects/resources."""
        return await self._store.aggregate_inventory()
