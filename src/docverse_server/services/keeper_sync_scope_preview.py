"""Side-effect-free resolution of a keeper-sync scope against live LTD.

Backs ``POST /orgs/{org}/keeper-sync/scope-preview`` (PRD #667). Saving
a wider scope is not inert — the tier crons act on the stored config at
their next tick — so an operator syncing lsst.io in waves needs to see
what a candidate scope resolves to *before* it is saved. This service
merges the candidate over the stored config with exactly the merge
``PATCH`` uses, resolves it against the live LTD product listing, and
returns a report. It writes nothing and enqueues nothing.

The work is deliberately split into three steps rather than one
``preview()`` call, because the LTD fetch in the middle must not run
inside a database transaction: LTD sits behind an httpx timeout, and a
transaction held open across it pins a pooled Postgres connection
``idle in transaction`` for the whole timeout — under exactly the
retries a slow LTD provokes. Handlers own transactions (see
``CLAUDE.md``), so the handler runs :meth:`~KeeperSyncScopePreview
Service.load_plan` in one short transaction, awaits
:meth:`~KeeperSyncScopePreviewService.fetch_ltd_product_slugs` with
none open, and runs :meth:`~KeeperSyncScopePreviewService.report` in a
second.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import structlog

from docverse.models import (
    KeeperSyncConfig,
    KeeperSyncScopePreview,
    KeeperSyncScopePreviewRequest,
)
from docverse_server.exceptions import NotFoundError, UpstreamServiceError
from docverse_server.services.keeper_sync_config import KeeperSyncConfigService
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
)
from docverse_server.storage.ltd.client import LtdProductsError
from docverse_server.storage.ltd.products_client import LtdProductsClient
from docverse_server.storage.organization_store import OrganizationStore

__all__ = [
    "KeeperSyncScopePlan",
    "KeeperSyncScopePreviewService",
    "LtdProductsClientFactory",
]


class LtdProductsClientFactory(Protocol):
    """Callable minting an :class:`LtdProductsClient` for a base URL.

    Threaded in so the service can pin the LTD base URL it fetches from
    — always the *stored* config's, never a candidate's.
    ``Factory.create_ltd_products_client`` already matches this shape;
    the indirection lets unit tests pass a fake client.
    """

    def __call__(self, *, base_url: str) -> LtdProductsClient:
        """Return an :class:`LtdProductsClient` for ``base_url``."""


@dataclass(frozen=True, slots=True)
class KeeperSyncScopePlan:
    """Everything the preview reads from the database before it calls LTD.

    Carried across the untransacted LTD fetch, so the fetch itself needs
    no session and the two database reads on either side of it can each
    be their own short transaction. Every field is a plain value or a
    detached pydantic model — nothing here is a live ORM row that would
    re-open a transaction when touched after the first one commits.
    """

    org_id: int
    """Internal row id of the organization being previewed."""

    org_slug: str
    """Organization slug, for logging."""

    config: KeeperSyncConfig
    """Candidate config: the stored one with any candidate merged over."""

    ltd_base_url: str
    """LTD instance to fetch, taken from the **stored** config.

    Read off the stored config rather than the merged one, so the
    preview cannot be pointed at an operator-supplied host even if a
    future change lets a candidate body carry ``ltd_base_url`` again.
    :class:`~docverse.models.KeeperSyncScopePreviewRequest` rejects that
    field today; this is the second lock on the same door.
    """

    is_candidate: bool
    """Whether a candidate body was merged in, for logging."""


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

    async def load_plan(
        self,
        *,
        org_slug: str,
        update: KeeperSyncScopePreviewRequest | None = None,
    ) -> KeeperSyncScopePlan:
        """Read the org row and resolve the candidate config.

        The first of the preview's two database reads, and the only one
        that has to happen *before* LTD is called: the stored config is
        what names the LTD instance to fetch. Keep it in its own short
        transaction — see this module's docstring for why the fetch must
        not join it.

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
        KeeperSyncScopePlan
            The resolved candidate config plus the stored LTD base URL,
            in a form that survives the transaction's commit.

        Raises
        ------
        NotFoundError
            If the organization does not exist.

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
        return KeeperSyncScopePlan(
            org_id=org.id,
            org_slug=org_slug,
            config=config,
            ltd_base_url=str(stored.ltd_base_url),
            is_candidate=update is not None,
        )

    async def report(
        self, *, plan: KeeperSyncScopePlan, ltd_slugs: list[str]
    ) -> KeeperSyncScopePreview:
        """Resolve the plan against a fetched LTD listing.

        The preview's second database read, and the last step: call it
        in its own short transaction once
        :meth:`fetch_ltd_product_slugs` has returned. Nothing is written
        and nothing is enqueued.

        Parameters
        ----------
        plan
            What :meth:`load_plan` resolved.
        ltd_slugs
            The live LTD product slugs, in listing order.

        Returns
        -------
        KeeperSyncScopePreview
            Counts and slug lists describing the resolved scope.
        """
        config = plan.config
        in_scope_slugs = config.filter_in_scope(ltd_slugs)

        # One query covers both derived lists: ``new_slugs`` needs every
        # project-resource row (a tombstoned slug is known, not new), so
        # the tombstoned rows have to come back too.
        state_rows = await self._state_store.list_for_org(
            org_id=plan.org_id,
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
            org=plan.org_slug,
            candidate=plan.is_candidate,
            ltd_count=preview.ltd_count,
            in_scope_count=preview.in_scope_count,
            new_count=len(preview.new_slugs),
            tombstoned_count=len(preview.tombstoned_slugs),
            unmatched_count=len(preview.unmatched_project_slugs),
        )
        return preview

    async def fetch_ltd_product_slugs(
        self, plan: KeeperSyncScopePlan
    ) -> list[str]:
        """Fetch the live LTD product listing, or raise a 502.

        Touches no database session, and must be awaited with **no
        transaction open**: this is the call that can hang for an httpx
        timeout, and a transaction spanning it would pin a pooled
        Postgres connection for the duration.

        The listing always comes from ``plan.ltd_base_url`` — the
        *stored* config's LTD instance — so a preview body can never
        aim an outbound request at a host of the caller's choosing.

        Raises
        ------
        UpstreamServiceError
            If the LTD product listing could not be fetched *or read* —
            including a 200 whose body is not a usable listing, which
            is what a proxy or maintenance page in front of LTD serves.
            Surfaces as a 502 naming LTD's status, so an LTD outage is
            never reported as a Docverse 500.

        The fetch itself is shared with the worker's discovery and
        tier-cron passes — :meth:`LtdProductsClient.list_product_slugs`
        is the one path, and it normalises transport failures, non-2xx
        statuses, and a 200 whose body is not a usable listing into a
        single :class:`LtdProductsError`. What differs is the *policy*,
        which is why it stays here at the call site rather than in the
        client: this caller is an org admin watching the response, so
        the failure is theirs to read as a 502 and alerting per retry
        would be noise. The worker's call sites are unattended, so they
        let the same exception alert (see
        :func:`docverse_server.worker.functions.keeper_sync._fetch_ltd_product_slugs`).

        The underlying message is quoted into the 502 because it is the
        only thing that distinguishes "LTD returned 503" from "LTD
        returned 200 and an HTML maintenance page" — and an operator
        staging a migration wave needs to tell those apart without pod
        logs.
        """
        base_url = plan.ltd_base_url
        client = self._products_client_factory(base_url=base_url)
        try:
            return await client.list_product_slugs()
        except LtdProductsError as exc:
            msg = (
                f"The LTD Keeper product listing at {base_url} could not"
                f" be read ({exc}); the keeper-sync scope cannot be"
                " previewed until LTD serves a usable product listing"
            )
            raise UpstreamServiceError(
                msg, upstream_status=exc.status_code
            ) from exc


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
