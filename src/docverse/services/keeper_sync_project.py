"""Operator-facing reads of one keeper-sync project's status.

This service backs the ``GET /orgs/{org}/keeper-sync/projects/
{ltd_slug}`` admin endpoint. It is the read-side companion to the
one-shot ``POST .../refresh`` from #316: an operator inspects sync
state via this GET, and can choose to promote it via the POST.

The service joins together the project-resource ``keeper_sync_state``
row, per-tier planner explanations from
:mod:`docverse.services.keeper_sync.scheduler`, and Docverse-side
editions left-joined with their state rows. When called with
``include_ltd_diff=True`` it additionally fetches LTD's live edition
listing and emits ``missing_in_docverse`` / ``missing_in_ltd`` arrays
for deeper diagnostics.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol

import structlog

from docverse.client.models import (
    KeeperSyncEditionDiff,
    KeeperSyncEditionStatus,
    KeeperSyncProjectStateSummary,
    KeeperSyncProjectStatus,
    KeeperSyncTierName,
    KeeperSyncTierStatus,
)
from docverse.exceptions import NotFoundError
from docverse.services.keeper_sync.scheduler import (
    TIER_DISCOVERY_DORMANT_INTERVAL,
    TIER_DISCOVERY_DORMANT_JITTER,
    TIER_DISCOVERY_HOT_WINDOW,
    TIER_MAIN_DORMANT_INTERVAL,
    TIER_MAIN_DORMANT_JITTER,
    TIER_MAIN_HOT_WINDOW,
    TIER_OTHER_DORMANT_INTERVAL,
    TIER_OTHER_DORMANT_JITTER,
    TIER_OTHER_HOT_WINDOW,
    Tier,
    explain_tier_status,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.keeper_sync import (
    KeeperSyncState,
    KeeperSyncStateStore,
    ResourceType,
)
from docverse.storage.ltd.client import LtdClient, LtdClientError
from docverse.storage.organization_store import OrganizationStore

__all__ = ["KeeperSyncProjectService", "LtdClientFactory"]


class LtdClientFactory(Protocol):
    """Callable that mints an :class:`LtdClient` for a given base URL.

    Threaded into :class:`KeeperSyncProjectService` so the service can
    pin the LTD base URL from the org's persisted config rather than a
    constructor argument. ``Factory.create_ltd_client`` already matches
    this shape; the indirection lets unit tests pass a fake client.
    """

    def __call__(self, *, base_url: str) -> LtdClient:
        """Return an :class:`LtdClient` for ``base_url``."""


class KeeperSyncProjectService:
    """Read-only project-status service for the org-admin GET endpoint."""

    def __init__(
        self,
        *,
        org_store: OrganizationStore,
        edition_store: EditionStore,
        state_store: KeeperSyncStateStore,
        ltd_client_factory: LtdClientFactory,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._org_store = org_store
        self._edition_store = edition_store
        self._state_store = state_store
        self._ltd_client_factory = ltd_client_factory
        self._logger = logger

    async def get_project_status(
        self,
        *,
        org_slug: str,
        ltd_slug: str,
        include_ltd_diff: bool,
    ) -> KeeperSyncProjectStatus:
        """Return an operator-readable status of one project's sync state.

        Raises
        ------
        NotFoundError
            If the org does not exist, LTD sync is not enabled on it,
            or ``ltd_slug`` is not in the configured ``project_slugs``
            allowlist (and the allowlist is not ``"*"``). Issue #317
            specifies 404 for the disabled-sync and out-of-allowlist
            cases — the resource (a sync-eligible project on this org)
            does not exist.
        """
        org = await self._org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        config = org.keeper_sync_config
        if config is None or not config.enabled:
            msg = (
                f"LTD Keeper sync is not enabled for organization {org_slug!r}"
            )
            raise NotFoundError(msg)
        if (
            config.project_slugs != "*"
            and ltd_slug not in config.project_slugs
        ):
            msg = (
                f"LTD slug {ltd_slug!r} is not in the project_slugs"
                f" allowlist for organization {org_slug!r}"
            )
            raise NotFoundError(msg)

        now = datetime.now(tz=UTC)
        project_state = await self._state_store.get(
            org_id=org.id,
            resource_type=ResourceType.project,
            ltd_slug=ltd_slug,
        )
        tier_status = _explain_all_tiers(state=project_state, now=now)

        # ``keeper_sync_state`` has no project_id column, so scope the
        # edition state rows by walking through Docverse's editions
        # for this project (whose ``project_id`` is the linkage).
        editions: list[KeeperSyncEditionStatus] = []
        product_state_rows: list[KeeperSyncState] = []
        docverse_project_id = (
            project_state.docverse_id if project_state else None
        )
        if docverse_project_id is not None:
            (
                editions,
                product_state_rows,
            ) = await self._list_edition_status_and_state(
                org_id=org.id,
                docverse_project_id=docverse_project_id,
            )

        edition_diff: KeeperSyncEditionDiff | None = None
        if include_ltd_diff:
            edition_diff = await self._compute_edition_diff(
                product_state_rows=product_state_rows,
                ltd_slug=ltd_slug,
                ltd_base_url=str(config.ltd_base_url),
            )

        return KeeperSyncProjectStatus(
            org_slug=org_slug,
            ltd_slug=ltd_slug,
            project_state=_summarise_project_state(project_state),
            tier_status=tier_status,
            editions=editions,
            edition_diff=edition_diff,
        )

    async def _list_edition_status_and_state(
        self,
        *,
        org_id: int,
        docverse_project_id: int,
    ) -> tuple[list[KeeperSyncEditionStatus], list[KeeperSyncState]]:
        """Return Docverse-side editions left-joined with sync state.

        Single batched ``list_for_org`` query plus an in-memory map
        keyed on ``docverse_id`` keeps this O(1) round-trip on the
        keeper-sync side rather than O(N) per edition. Also returns
        the scoped state rows so the caller can compute an LTD diff
        without re-issuing the query.
        """
        editions = await self._edition_store.list_all_by_project(
            docverse_project_id
        )
        if not editions:
            return [], []
        edition_states = await self._state_store.list_for_org(
            org_id=org_id, resource_type=ResourceType.edition
        )
        state_by_docverse_id: dict[int, KeeperSyncState] = {
            state.docverse_id: state
            for state in edition_states
            if state.docverse_id is not None
        }
        results: list[KeeperSyncEditionStatus] = []
        scoped_state_rows: list[KeeperSyncState] = []
        for edition in editions:
            state = state_by_docverse_id.get(edition.id)
            if state is not None:
                scoped_state_rows.append(state)
            results.append(
                KeeperSyncEditionStatus(
                    docverse_edition_id=edition.id,
                    docverse_slug=edition.slug,
                    docverse_kind=str(edition.kind),
                    ltd_id=state.ltd_id if state else None,
                    ltd_slug=state.ltd_slug if state else None,
                    date_last_synced=(
                        state.date_last_synced if state else None
                    ),
                )
            )
        return results, scoped_state_rows

    async def _compute_edition_diff(
        self,
        *,
        product_state_rows: list[KeeperSyncState],
        ltd_slug: str,
        ltd_base_url: str,
    ) -> KeeperSyncEditionDiff:
        """Live-fetch LTD's editions and compute the missing-in-* sets.

        Joined on LTD edition id (LTD slugs are only unique within a
        product, but ids are globally unique). ``product_state_rows``
        is pre-scoped to this product via the Docverse-side
        ``editions.project_id`` join in
        :meth:`_list_edition_status_and_state`, so the symmetric
        difference here is product-scoped by construction without
        needing a project_id column on ``keeper_sync_state``.
        """
        ltd_client = self._ltd_client_factory(base_url=ltd_base_url)
        try:
            ltd_editions = await ltd_client.list_editions_for_product(ltd_slug)
        except LtdClientError as exc:
            # LTD-side errors surface to the operator as an empty diff
            # rather than tipping the whole GET into a 5xx; the rest
            # of the response is still useful for diagnostics.
            self._logger.info(
                "LTD edition listing failed during project status diff",
                ltd_slug=ltd_slug,
                error=str(exc),
            )
            return KeeperSyncEditionDiff(
                missing_in_docverse=[], missing_in_ltd=[]
            )
        ltd_slugs_by_id: dict[int, str] = {
            edition.ltd_id: edition.slug for edition in ltd_editions
        }
        state_slugs_by_id: dict[int, str] = {
            state.ltd_id: state.ltd_slug
            for state in product_state_rows
            if state.ltd_id is not None
        }
        missing_in_docverse_ids = set(ltd_slugs_by_id) - set(state_slugs_by_id)
        missing_in_ltd_ids = set(state_slugs_by_id) - set(ltd_slugs_by_id)
        return KeeperSyncEditionDiff(
            missing_in_docverse=sorted(
                ltd_slugs_by_id[ltd_id] for ltd_id in missing_in_docverse_ids
            ),
            missing_in_ltd=sorted(
                state_slugs_by_id[ltd_id] for ltd_id in missing_in_ltd_ids
            ),
        )


def _summarise_project_state(
    state: KeeperSyncState | None,
) -> KeeperSyncProjectStateSummary | None:
    """Project a state row into the operator-readable summary type."""
    if state is None:
        return None
    return KeeperSyncProjectStateSummary(
        docverse_project_id=state.docverse_id,
        ltd_slug=state.ltd_slug,
        date_last_synced=state.date_last_synced,
        date_rebuilt_seen=state.date_rebuilt_seen,
        annotations=state.annotations,
    )


def _explain_all_tiers(
    *,
    state: KeeperSyncState | None,
    now: datetime,
) -> list[KeeperSyncTierStatus]:
    """Compute the per-tier explainer for a project's state row.

    The per-tier ``hot_window`` / ``dormant_interval`` / ``jitter_
    window`` constants live in :mod:`scheduler`; this helper threads
    each tier's triple in so the explainer and the gate planner pull
    from the same source of truth.
    """
    return [
        _build_tier_status(
            tier=Tier.main,
            tier_name="main",
            state=state,
            now=now,
            hot_window_dormant_jitter=(
                TIER_MAIN_HOT_WINDOW,
                TIER_MAIN_DORMANT_INTERVAL,
                TIER_MAIN_DORMANT_JITTER,
            ),
        ),
        _build_tier_status(
            tier=Tier.discovery,
            tier_name="discovery",
            state=state,
            now=now,
            hot_window_dormant_jitter=(
                TIER_DISCOVERY_HOT_WINDOW,
                TIER_DISCOVERY_DORMANT_INTERVAL,
                TIER_DISCOVERY_DORMANT_JITTER,
            ),
        ),
        _build_tier_status(
            tier=Tier.other,
            tier_name="other",
            state=state,
            now=now,
            hot_window_dormant_jitter=(
                TIER_OTHER_HOT_WINDOW,
                TIER_OTHER_DORMANT_INTERVAL,
                TIER_OTHER_DORMANT_JITTER,
            ),
        ),
    ]


def _build_tier_status(
    *,
    tier: Tier,
    tier_name: KeeperSyncTierName,
    state: KeeperSyncState | None,
    now: datetime,
    hot_window_dormant_jitter: tuple[timedelta, timedelta, timedelta],
) -> KeeperSyncTierStatus:
    """Compose a :class:`KeeperSyncTierStatus` from the planner output."""
    hot_window, dormant_interval, jitter_window = hot_window_dormant_jitter
    explanation = explain_tier_status(
        state,
        now,
        tier=tier,
        hot_window=hot_window,
        dormant_interval=dormant_interval,
        jitter_window=jitter_window,
    )
    return KeeperSyncTierStatus(
        tier=tier_name,
        cohort=explanation.cohort,
        last_polled_at=explanation.last_polled_at,
        next_due_at=explanation.next_due_at,
    )
