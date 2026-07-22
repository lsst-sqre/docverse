"""Service for reading and writing an organization's LTD-sync config."""

from __future__ import annotations

import structlog

from docverse.client.models import KeeperSyncConfig, KeeperSyncConfigUpdate
from docverse.exceptions import NotFoundError
from docverse.storage.organization_store import OrganizationStore


class KeeperSyncConfigService:
    """Read/write the persisted LTD-sync config for an organization."""

    def __init__(
        self,
        org_store: OrganizationStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._org_store = org_store
        self._logger = logger

    async def get(self, org_slug: str) -> KeeperSyncConfig:
        """Return the persisted config, or a default-disabled instance.

        Raises
        ------
        NotFoundError
            If the organization does not exist.
        """
        org = await self._org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        return org.keeper_sync_config or KeeperSyncConfig()

    async def put(
        self, org_slug: str, config: KeeperSyncConfig
    ) -> KeeperSyncConfig:
        """Replace the persisted config and return the round-tripped value.

        Raises
        ------
        NotFoundError
            If the organization does not exist.
        """
        updated = await self._org_store.update_keeper_sync_config(
            slug=org_slug, config=config
        )
        if updated is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        self._logger.info(
            "Updated keeper_sync_config",
            org_slug=org_slug,
            enabled=config.enabled,
        )
        if updated.keeper_sync_config is None:
            msg = (
                f"keeper_sync_config unexpectedly None after PUT for "
                f"{org_slug!r}"
            )
            raise RuntimeError(msg)
        return updated.keeper_sync_config

    async def patch(
        self, org_slug: str, update: KeeperSyncConfigUpdate
    ) -> KeeperSyncConfig:
        """Apply a JSON-Merge-Patch to the persisted config.

        Fields left unset on ``update`` are carried over unchanged from the
        current (or default-disabled) config; ``project_slugs``, when
        provided, replaces the stored list wholesale. Returns the
        round-tripped merged value.

        Raises
        ------
        NotFoundError
            If the organization does not exist.
        """
        current = await self.get(org_slug=org_slug)
        changes = update.model_dump(exclude_unset=True)
        merged = current.model_copy(update=changes)
        return await self.put(org_slug=org_slug, config=merged)
