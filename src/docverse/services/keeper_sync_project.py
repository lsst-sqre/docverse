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

The service is request-agnostic: it returns a
:class:`KeeperSyncProjectStatusResult` carrying raw domain rows so the
HTTP handler can mint URL-bearing response fields via FastAPI's
``request.url_for``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

import structlog
from docverse.client.models import (
    EditionKind,
    KeeperSyncEditionDiff,
    KeeperSyncProjectStateSummary,
    KeeperSyncTierName,
    KeeperSyncTierStatus,
)
from safir.database import CountedPaginatedList, PaginationCursor

from docverse.domain.edition import Edition
from docverse.exceptions import NotFoundError
from docverse.services.keeper_sync.scheduler import (
    TIER_DISCOVERY_CRON_INTERVAL,
    TIER_DISCOVERY_DORMANT_INTERVAL,
    TIER_DISCOVERY_DORMANT_JITTER,
    TIER_DISCOVERY_HOT_WINDOW,
    TIER_MAIN_CRON_INTERVAL,
    TIER_MAIN_DORMANT_INTERVAL,
    TIER_MAIN_DORMANT_JITTER,
    TIER_MAIN_HOT_WINDOW,
    TIER_OTHER_CRON_INTERVAL,
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
from docverse.storage.pagination import (
    KeeperSyncEditionSlugCursor,
    KeeperSyncProjectStateIdCursor,
)
from docverse.storage.project_store import ProjectStore

__all__ = [
    "KeeperSyncEditionListResult",
    "KeeperSyncEditionStatusRow",
    "KeeperSyncProjectListResult",
    "KeeperSyncProjectService",
    "KeeperSyncProjectStatusResult",
    "LtdClientFactory",
]


@dataclass(frozen=True, slots=True)
class KeeperSyncEditionStatusRow:
    """Raw ``(Edition, KeeperSyncState | None)`` row for handler composition.

    The handler turns this into a
    :class:`docverse.handlers.orgs.keeper_sync_models.KeeperSyncEditionStatus`
    via ``from_domain``, minting the canonical ``edition_url`` from
    ``request.url_for("get_edition", ...)``.
    """

    edition: Edition
    state: KeeperSyncState | None


@dataclass(frozen=True, slots=True)
class KeeperSyncProjectStatusResult:
    """Handler-agnostic result returned by ``get_project_status``.

    Carries everything the handler needs to compose the API response —
    including the resolved Docverse project slug so the handler can
    build per-edition URLs without re-issuing a project lookup. The
    full edition list is intentionally not embedded; operators
    paginate through it via the
    ``get_org_keeper_sync_project_editions`` endpoint.
    """

    org_slug: str
    ltd_slug: str
    docverse_project_slug: str | None
    project_state: KeeperSyncProjectStateSummary | None
    tier_status: list[KeeperSyncTierStatus]
    main_edition_row: KeeperSyncEditionStatusRow | None
    edition_diff: KeeperSyncEditionDiff | None


@dataclass(frozen=True, slots=True)
class KeeperSyncProjectListResult:
    """Handler-agnostic result for the paginated keeper-sync projects listing.

    Wraps the page of fully-composed
    :class:`KeeperSyncProjectStatusResult` entries plus the underlying
    paginated state-row page so the handler can emit ``Link`` and
    ``X-Total-Count`` headers from one place. The entries are
    pre-composed at the service layer so the handler only has to mint
    HATEOAS URLs via ``request.url_for``.
    """

    entries: list[KeeperSyncProjectStatusResult]
    page: CountedPaginatedList[KeeperSyncState, KeeperSyncProjectStateIdCursor]


@dataclass(frozen=True, slots=True)
class KeeperSyncEditionListResult:
    """Handler-agnostic result for the paginated editions collection.

    Wraps the storage layer's ``CountedPaginatedList`` plus the
    resolved Docverse project slug so the handler can mint per-edition
    URLs without re-issuing a project lookup.
    """

    org_slug: str
    docverse_project_slug: str
    page: CountedPaginatedList[Edition, PaginationCursor[Edition]]
    state_by_docverse_id: dict[int, KeeperSyncState]


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
        project_store: ProjectStore,
        edition_store: EditionStore,
        state_store: KeeperSyncStateStore,
        ltd_client_factory: LtdClientFactory,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._org_store = org_store
        self._project_store = project_store
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
    ) -> KeeperSyncProjectStatusResult:
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

        main_edition_row: KeeperSyncEditionStatusRow | None = None
        product_state_rows: list[KeeperSyncState] = []
        docverse_project_slug: str | None = None
        docverse_project_id = (
            project_state.docverse_id if project_state else None
        )
        if docverse_project_id is not None:
            project = await self._project_store.get_by_id(docverse_project_id)
            if project is not None:
                docverse_project_slug = project.slug
                main_edition_row = await self._lookup_main_edition_row(
                    org_id=org.id,
                    docverse_project_id=docverse_project_id,
                )
                # The LTD diff path scopes state rows to this project
                # via the editions.project_id linkage, so walk the
                # full edition list only when ``?ltd=true`` actually
                # needs it. The default response no longer pays this
                # cost.
                if include_ltd_diff:
                    product_state_rows = await self._scoped_edition_states(
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

        return KeeperSyncProjectStatusResult(
            org_slug=org_slug,
            ltd_slug=ltd_slug,
            docverse_project_slug=docverse_project_slug,
            project_state=_summarise_project_state(project_state),
            tier_status=tier_status,
            main_edition_row=main_edition_row,
            edition_diff=edition_diff,
        )

    async def list_project_editions(
        self,
        *,
        org_slug: str,
        ltd_slug: str,
        cursor: KeeperSyncEditionSlugCursor | None,
        limit: int,
    ) -> KeeperSyncEditionListResult:
        """Return a paginated page of editions for one keeper-sync project.

        Backs ``GET /orgs/{org}/keeper-sync/projects/{ltd_slug}/
        editions``. Enforces the same enable/allowlist 404 gate as
        :meth:`get_project_status`. When the Docverse project does not
        yet exist for the LTD slug, returns an empty page (rather than
        404) — the slug is sync-eligible, it just has no editions yet.

        Raises
        ------
        NotFoundError
            If the org does not exist, LTD sync is disabled on it, or
            ``ltd_slug`` is not in the configured ``project_slugs``
            allowlist (and the allowlist is not ``"*"``).
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

        project_state = await self._state_store.get(
            org_id=org.id,
            resource_type=ResourceType.project,
            ltd_slug=ltd_slug,
        )
        docverse_project_id = (
            project_state.docverse_id if project_state else None
        )
        docverse_project_slug: str | None = None
        if docverse_project_id is not None:
            project = await self._project_store.get_by_id(docverse_project_id)
            if project is not None:
                docverse_project_slug = project.slug
        if docverse_project_id is None or docverse_project_slug is None:
            return KeeperSyncEditionListResult(
                org_slug=org_slug,
                docverse_project_slug="",
                page=CountedPaginatedList(
                    entries=[],
                    next_cursor=None,
                    prev_cursor=None,
                    count=0,
                ),
                state_by_docverse_id={},
            )

        page = await self._edition_store.list_by_project(
            docverse_project_id,
            cursor_type=KeeperSyncEditionSlugCursor,
            cursor=cursor,
            limit=limit,
        )
        edition_states = await self._state_store.list_for_org(
            org_id=org.id,
            resource_type=ResourceType.edition,
            docverse_ids=[edition.id for edition in page.entries],
        )
        state_by_docverse_id: dict[int, KeeperSyncState] = {
            state.docverse_id: state
            for state in edition_states
            if state.docverse_id is not None
        }
        return KeeperSyncEditionListResult(
            org_slug=org_slug,
            docverse_project_slug=docverse_project_slug,
            page=page,
            state_by_docverse_id=state_by_docverse_id,
        )

    async def list_project_statuses(
        self,
        *,
        org_slug: str,
        cursor: KeeperSyncProjectStateIdCursor | None,
        limit: int,
    ) -> KeeperSyncProjectListResult:
        """Return a paginated page of every keeper-sync project for an org.

        Only projects with a ``keeper_sync_state`` row of
        ``resource_type=project`` for this org appear. Never-seen-but-
        allowlisted slugs are intentionally excluded: operators can
        still inspect them via :meth:`get_project_status`.

        Per-page cost is O(1) round-trips regardless of page size:
        Docverse projects and ``__main`` editions for the page are
        batch-loaded, and the org-wide edition state rows are fetched
        once and indexed in-memory for the main-edition left-join.

        Raises
        ------
        NotFoundError
            If the org does not exist or LTD sync is not enabled on it.
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

        page = await self._state_store.list_project_resources_for_org(
            org_id=org.id, cursor=cursor, limit=limit
        )
        project_ids = [
            row.docverse_id
            for row in page.entries
            if row.docverse_id is not None
        ]
        projects_by_id = {
            project.id: project
            for project in await self._project_store.list_by_ids(project_ids)
        }
        main_editions = await self._edition_store.list_by_project_ids_and_kind(
            project_ids=list(projects_by_id),
            kind=EditionKind.main,
        )
        main_edition_by_project_id = {
            edition.project_id: edition for edition in main_editions
        }
        edition_states = await self._state_store.list_for_org(
            org_id=org.id, resource_type=ResourceType.edition
        )
        edition_state_by_docverse_id: dict[int, KeeperSyncState] = {
            state.docverse_id: state
            for state in edition_states
            if state.docverse_id is not None
        }
        now = datetime.now(tz=UTC)

        entries: list[KeeperSyncProjectStatusResult] = []
        for state_row in page.entries:
            project = (
                projects_by_id.get(state_row.docverse_id)
                if state_row.docverse_id is not None
                else None
            )
            docverse_project_slug = (
                project.slug if project is not None else None
            )
            main_edition_row: KeeperSyncEditionStatusRow | None = None
            if project is not None:
                main_edition = main_edition_by_project_id.get(project.id)
                if main_edition is not None:
                    main_edition_row = KeeperSyncEditionStatusRow(
                        edition=main_edition,
                        state=edition_state_by_docverse_id.get(
                            main_edition.id
                        ),
                    )
            entries.append(
                KeeperSyncProjectStatusResult(
                    org_slug=org_slug,
                    ltd_slug=state_row.ltd_slug,
                    docverse_project_slug=docverse_project_slug,
                    project_state=_summarise_project_state(state_row),
                    tier_status=_explain_all_tiers(state=state_row, now=now),
                    main_edition_row=main_edition_row,
                    edition_diff=None,
                )
            )
        return KeeperSyncProjectListResult(entries=entries, page=page)

    async def _lookup_main_edition_row(
        self,
        *,
        org_id: int,
        docverse_project_id: int,
    ) -> KeeperSyncEditionStatusRow | None:
        """Return the project's ``__main`` edition + its state row.

        The ``ck_editions_main_slug_kind`` constraint makes ``kind ==
        EditionKind.main`` <=> ``slug == "__main"``, so a single slug
        lookup is sufficient. Returns ``None`` when no main edition
        has been created yet for the project (atypical — every
        project gets an auto-created ``__main`` on creation, but the
        method tolerates the gap defensively). The state-row lookup
        is an indexed single-row read keyed on ``docverse_id`` so the
        per-project GET no longer scans every edition state row for
        the org.
        """
        edition = await self._edition_store.get_by_slug(
            project_id=docverse_project_id, slug="__main"
        )
        if edition is None:
            return None
        state = await self._state_store.get_by_docverse_id(
            org_id=org_id,
            resource_type=ResourceType.edition,
            docverse_id=edition.id,
        )
        return KeeperSyncEditionStatusRow(edition=edition, state=state)

    async def _scoped_edition_states(
        self,
        *,
        org_id: int,
        docverse_project_id: int,
    ) -> list[KeeperSyncState]:
        """Return keeper-sync edition state rows scoped to one project.

        ``keeper_sync_state`` has no ``project_id`` column, so scope
        the rows by walking through Docverse's editions for this
        project (whose ``project_id`` is the linkage) and pushing the
        ``docverse_id`` set into ``list_for_org``'s WHERE clause.
        Only called on the ``?ltd=true`` path so the default GET
        stays cheap, but the ``docverse_ids`` filter still pulls the
        per-call cost from the org-wide row count down to this
        project's edition count.
        """
        editions = await self._edition_store.list_all_by_project(
            docverse_project_id
        )
        if not editions:
            return []
        return await self._state_store.list_for_org(
            org_id=org_id,
            resource_type=ResourceType.edition,
            docverse_ids=[edition.id for edition in editions],
        )

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
            cron_interval=TIER_MAIN_CRON_INTERVAL,
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
            cron_interval=TIER_DISCOVERY_CRON_INTERVAL,
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
            cron_interval=TIER_OTHER_CRON_INTERVAL,
        ),
    ]


def _build_tier_status(
    *,
    tier: Tier,
    tier_name: KeeperSyncTierName,
    state: KeeperSyncState | None,
    now: datetime,
    hot_window_dormant_jitter: tuple[timedelta, timedelta, timedelta],
    cron_interval: timedelta,
) -> KeeperSyncTierStatus:
    """Compose a :class:`KeeperSyncTierStatus` from the planner output."""
    hot_window, dormant_interval, jitter_window = hot_window_dormant_jitter
    explanation = explain_tier_status(
        state,
        now,
        tier=tier,
        hot_window=hot_window,
        dormant_interval=dormant_interval,
        cron_interval=cron_interval,
        jitter_window=jitter_window,
    )
    return KeeperSyncTierStatus(
        tier=tier_name,
        cohort=explanation.cohort,
        date_last_polled=explanation.last_polled_at,
        date_next_due=explanation.next_due_at,
    )
