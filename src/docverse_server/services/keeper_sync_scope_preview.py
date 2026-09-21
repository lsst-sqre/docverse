"""Side-effect-free resolution of a keeper-sync scope against live LTD.

Backs ``POST /orgs/{org}/keeper-sync/scope-preview`` (PRD #667). Saving
a wider scope is not inert — the tier crons act on the stored config at
their next tick — so an operator syncing lsst.io in waves needs to see
what a candidate scope resolves to *before* it is saved. This service
merges the candidate over the stored config with exactly the merge
``PATCH`` uses, resolves it against the live LTD product listing, and
returns a report. It writes nothing and enqueues nothing.
"""

from __future__ import annotations

from typing import Protocol

import httpx
import structlog

from docverse.models import (
    KeeperSyncConfig,
    KeeperSyncConfigUpdate,
    KeeperSyncScopePreview,
)
from docverse_server.exceptions import NotFoundError, UpstreamServiceError
from docverse_server.services.keeper_sync_config import KeeperSyncConfigService
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
)
from docverse_server.storage.ltd.products_client import LtdProductsClient
from docverse_server.storage.organization_store import OrganizationStore

__all__ = ["KeeperSyncScopePreviewService", "LtdProductsClientFactory"]


class LtdProductsClientFactory(Protocol):
    """Callable minting an :class:`LtdProductsClient` for a base URL.

    Threaded in so the service pins the LTD base URL from the config it
    is previewing — which may be the *candidate* base URL, not the
    stored one. ``Factory.create_ltd_products_client`` already matches
    this shape; the indirection lets unit tests pass a fake client.
    """

    def __call__(self, *, base_url: str) -> LtdProductsClient:
        """Return an :class:`LtdProductsClient` for ``base_url``."""


class KeeperSyncScopePreviewService:
    """Resolve a candidate keeper-sync scope without persisting it."""

    def __init__(
        self,
        *,
        org_store: OrganizationStore,
        config_service: KeeperSyncConfigService,
        state_store: KeeperSyncStateStore,
        products_client_factory: LtdProductsClientFactory,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._org_store = org_store
        self._config_service = config_service
        self._state_store = state_store
        self._products_client_factory = products_client_factory
        self._logger = logger

    async def preview(
        self,
        *,
        org_slug: str,
        update: KeeperSyncConfigUpdate | None = None,
    ) -> KeeperSyncScopePreview:
        """Report what a candidate scope resolves to on the live LTD.

        Parameters
        ----------
        org_slug
            Organization whose stored config the candidate merges over.
        update
            Candidate partial config, merged exactly as ``PATCH`` merges
            it. ``None`` (an omitted request body) previews the stored
            config as-is.

        Returns
        -------
        KeeperSyncScopePreview
            Counts and slug lists describing the resolved scope. Nothing
            is written and nothing is enqueued.

        Raises
        ------
        NotFoundError
            If the organization does not exist.
        UpstreamServiceError
            If the LTD product listing could not be fetched. Surfaces as
            a 502 carrying LTD's status, so an LTD outage is never
            reported as a Docverse 500.

        Notes
        -----
        The preview deliberately does not check ``enabled``: an operator
        stages a scope before turning sync on, and a disabled org's
        scope is exactly as meaningful to resolve as an enabled one's.
        """
        org = await self._org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        stored = org.keeper_sync_config or KeeperSyncConfig()
        config = (
            stored
            if update is None
            else self._config_service.merge(stored, update)
        )

        ltd_slugs = await self._fetch_ltd_product_slugs(config)
        in_scope_slugs = config.filter_in_scope(ltd_slugs)

        # One query covers both derived lists: ``new_slugs`` needs every
        # project-resource row (a tombstoned slug is known, not new), so
        # the tombstoned rows have to come back too.
        state_rows = await self._state_store.list_for_org(
            org_id=org.id,
            resource_type=ResourceType.project,
            include_tombstoned=True,
        )
        known_slugs = {row.ltd_slug for row in state_rows}
        tombstoned = {
            row.ltd_slug
            for row in state_rows
            if row.date_tombstoned is not None
        }

        preview = KeeperSyncScopePreview(
            ltd_count=len(ltd_slugs),
            in_scope_count=len(in_scope_slugs),
            in_scope_slugs=in_scope_slugs,
            new_slugs=[s for s in in_scope_slugs if s not in known_slugs],
            tombstoned_slugs=[s for s in in_scope_slugs if s in tombstoned],
            unmatched_project_slugs=_unmatched_project_slugs(
                config, ltd_slugs
            ),
        )
        self._logger.info(
            "Previewed keeper-sync scope",
            org=org_slug,
            candidate=update is not None,
            ltd_count=preview.ltd_count,
            in_scope_count=preview.in_scope_count,
            new_count=len(preview.new_slugs),
            tombstoned_count=len(preview.tombstoned_slugs),
            unmatched_count=len(preview.unmatched_project_slugs),
        )
        return preview

    async def _fetch_ltd_product_slugs(
        self, config: KeeperSyncConfig
    ) -> list[str]:
        """Fetch the live LTD product listing, or raise a 502."""
        base_url = str(config.ltd_base_url)
        client = self._products_client_factory(base_url=base_url)
        try:
            return await client.list_product_slugs()
        except httpx.HTTPStatusError as exc:
            upstream_status = exc.response.status_code
            msg = (
                f"The LTD Keeper product listing at {base_url} returned"
                f" HTTP {upstream_status}; the keeper-sync scope cannot"
                " be previewed until LTD responds"
            )
            raise UpstreamServiceError(
                msg, upstream_status=upstream_status
            ) from exc
        except httpx.HTTPError as exc:
            msg = (
                f"The LTD Keeper product listing at {base_url} could not"
                f" be reached ({exc.__class__.__name__}); the keeper-sync"
                " scope cannot be previewed until LTD responds"
            )
            raise UpstreamServiceError(msg) from exc


def _unmatched_project_slugs(
    config: KeeperSyncConfig, ltd_slugs: list[str]
) -> list[str]:
    """Return configured exact slugs the LTD listing does not contain.

    The typo catcher: a misspelled ``project_slugs`` entry silently
    syncs nothing and a misspelled ``exclude_project_slugs`` entry
    silently excludes nothing, and neither is visible in the resolved
    scope. Only the two *exact*-slug fields are checked — a pattern
    matching nothing is a legitimate way to stage a future wave, and
    ``project_slugs == "*"`` names no slugs at all.

    Entries are reported in config order with ``project_slugs`` first,
    de-duplicated, so a slug listed in both fields is named once.
    """
    listed = config.project_slugs
    configured = [] if isinstance(listed, str) else list(listed)
    configured.extend(config.exclude_project_slugs)
    available = set(ltd_slugs)
    unmatched: list[str] = []
    seen: set[str] = set()
    for slug in configured:
        if slug in available or slug in seen:
            continue
        seen.add(slug)
        unmatched.append(slug)
    return unmatched
