"""Service for writing and reading keeper-sync tombstones.

A *sync tombstone* is a permanent veto recorded on the existing
``keeper_sync_state`` table that tells the keeper-sync engine "this
LTD resource has been deleted on the Docverse side; do not re-migrate
it." See PRD #332 / DM-54914 for the full design.

This service is the single, well-tested entrypoint every deletion path
goes through to write, check, list, or clear a tombstone. Manual
deletes, lifecycle-driven deletes, and the lifecycle-preemptive
short-circuit all write via :meth:`record`; the admin API reads via
:meth:`list_for_org` and recovers via :meth:`clear`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import structlog
from safir.database import CountedPaginatedList
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.edition import SqlEdition
from docverse.dbschema.keeper_sync_state import SqlKeeperSyncState
from docverse.dbschema.project import SqlProject
from docverse.storage.keeper_sync import (
    KeeperSyncState,
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse.storage.keeper_sync.state_store import _key_clauses
from docverse.storage.pagination import KeeperSyncProjectStateIdCursor

__all__ = [
    "ClearedTombstone",
    "KeeperSyncTombstoneListResult",
    "KeeperSyncTombstoneService",
]


@dataclass(frozen=True, slots=True)
class KeeperSyncTombstoneListResult:
    """Handler-agnostic result of :meth:`list_for_org`.

    Carries the paginated state-row page plus per-entry derived display
    paths so the handler can compose the API response without
    re-issuing project / edition lookups. ``display_path_by_state_id``
    maps each entry's ``state.id`` to a human-readable
    Docverse-side path (``project_slug`` for project rows;
    ``project_slug/edition_slug`` for edition rows when the related
    Docverse rows still exist) and falls back to the LTD slug when no
    Docverse row exists — i.e. for ``lifecycle_preemptive`` rows or
    rows whose docverse row has been hard-deleted out from under the
    tombstone.
    """

    page: CountedPaginatedList[KeeperSyncState, KeeperSyncProjectStateIdCursor]
    display_path_by_state_id: dict[int, str]


@dataclass(frozen=True, slots=True)
class ClearedTombstone:
    """Result of :meth:`clear` — the cleared row plus revive outcome.

    ``revived_docverse_row`` is ``True`` when the matching Docverse row
    (edition or project) was still soft-deleted at clear time and its
    ``date_deleted`` was cleared in the same transaction. This is the
    revive-on-clear behavior PRD #332 calls out — without it the next
    sync iteration would crash on the slug clash because the
    soft-deleted row still occupies the unique index slot.
    """

    state: KeeperSyncState
    revived_docverse_row: bool


class KeeperSyncTombstoneService:
    """Write and read sync tombstones on ``keeper_sync_state`` rows."""

    def __init__(
        self,
        *,
        session: AsyncSession,
        state_store: KeeperSyncStateStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._state_store = state_store
        self._logger = logger

    async def record(  # noqa: PLR0913
        self,
        *,
        org_id: int,
        resource_type: ResourceType,
        reason: TombstoneReason,
        ltd_id: int | None = None,
        ltd_slug: str | None = None,
        note: str | None = None,
    ) -> KeeperSyncState:
        """Tombstone the matching ``keeper_sync_state`` row.

        The veto is written by stamping ``date_tombstoned``,
        ``tombstone_reason``, and ``tombstone_note`` on the row keyed
        by ``(org_id, resource_type, ltd_id|ltd_slug)``. If no such
        row exists — the ``lifecycle_preemptive`` case may fire
        against an LTD edition that was never imported — a fresh row
        with ``docverse_id=NULL`` is created carrying the tombstone
        fields directly.

        ``ltd_slug`` is required for ``project`` rows and ``ltd_id``
        for ``edition`` / ``build`` rows; see
        :func:`docverse.storage.keeper_sync.state_store._key_clauses`.
        """
        clauses = _key_clauses(
            org_id=org_id,
            resource_type=resource_type,
            ltd_id=ltd_id,
            ltd_slug=ltd_slug,
        )
        existing = await self._session.execute(
            select(SqlKeeperSyncState).where(*clauses)
        )
        row = existing.scalar_one_or_none()
        now = datetime.now(tz=UTC)
        if row is None:
            # Lifecycle-preemptive path: no Docverse row exists yet, so
            # synthesise a state row carrying only the tombstone fields.
            row = SqlKeeperSyncState(
                org_id=org_id,
                resource_type=resource_type.value,
                ltd_id=ltd_id,
                ltd_slug=ltd_slug if ltd_slug is not None else str(ltd_id),
                date_tombstoned=now,
                tombstone_reason=reason.value,
                tombstone_note=note,
            )
            self._session.add(row)
        else:
            row.date_tombstoned = now
            row.tombstone_reason = reason.value
            row.tombstone_note = note
        await self._session.flush()
        await self._session.refresh(row)
        self._logger.info(
            "Sync tombstone recorded",
            org_id=org_id,
            resource_type=resource_type.value,
            ltd_id=ltd_id,
            ltd_slug=ltd_slug,
            reason=reason.value,
        )
        return KeeperSyncState.model_validate(row)

    async def is_tombstoned(
        self,
        *,
        org_id: int,
        resource_type: ResourceType,
        ltd_id: int | None = None,
        ltd_slug: str | None = None,
    ) -> bool:
        """Return True when the matching state row is tombstoned.

        A row is tombstoned iff ``date_tombstoned IS NOT NULL``. The
        absence of a row is treated as "not tombstoned" — the LTD
        resource has never been seen and is not vetoed.
        """
        row = await self._state_store.get(
            org_id=org_id,
            resource_type=resource_type,
            ltd_id=ltd_id,
            ltd_slug=ltd_slug,
            include_tombstoned=True,
        )
        return row is not None and row.date_tombstoned is not None

    async def list_for_org(
        self,
        *,
        org_id: int,
        cursor: KeeperSyncProjectStateIdCursor | None,
        limit: int,
        resource_type: ResourceType | None = None,
        tombstone_reason: TombstoneReason | None = None,
    ) -> KeeperSyncTombstoneListResult:
        """Return a paginated page of tombstoned rows for an org.

        Backs the admin ``GET /orgs/{org}/keeper-sync/tombstones``
        endpoint. Filters by ``resource_type`` and ``tombstone_reason``
        stack and may both be ``None`` (no filter). Each entry's
        ``display_path`` is computed in one extra round-trip per table:
        editions in the page are bulk-fetched (including soft-deleted)
        and their parent projects (including soft-deleted) are
        bulk-fetched once, so the response is composed without N+1
        per-row lookups.
        """
        page = await self._state_store.list_tombstones_for_org(
            org_id=org_id,
            cursor=cursor,
            limit=limit,
            resource_type=resource_type,
            tombstone_reason=(
                tombstone_reason.value
                if tombstone_reason is not None
                else None
            ),
        )

        edition_ids: set[int] = set()
        project_ids: set[int] = set()
        for entry in page.entries:
            if entry.docverse_id is None:
                continue
            if entry.resource_type == ResourceType.edition.value:
                edition_ids.add(entry.docverse_id)
            elif entry.resource_type == ResourceType.project.value:
                project_ids.add(entry.docverse_id)

        editions_by_id: dict[int, SqlEdition] = {}
        if edition_ids:
            edition_result = await self._session.execute(
                select(SqlEdition).where(SqlEdition.id.in_(edition_ids))
            )
            editions_by_id = {
                row.id: row for row in edition_result.scalars().all()
            }
            project_ids.update(
                row.project_id for row in editions_by_id.values()
            )

        projects_by_id: dict[int, SqlProject] = {}
        if project_ids:
            project_result = await self._session.execute(
                select(SqlProject).where(SqlProject.id.in_(project_ids))
            )
            projects_by_id = {
                row.id: row for row in project_result.scalars().all()
            }

        display_path_by_state_id: dict[int, str] = {
            entry.id: _derive_display_path(
                entry,
                editions_by_id=editions_by_id,
                projects_by_id=projects_by_id,
            )
            for entry in page.entries
        }
        return KeeperSyncTombstoneListResult(
            page=page,
            display_path_by_state_id=display_path_by_state_id,
        )

    async def clear(
        self,
        *,
        state_id: int,
        org_id: int,
    ) -> ClearedTombstone:
        """Clear the tombstone on a state row, reviving its Docverse row.

        Backs ``DELETE /orgs/{org}/keeper-sync/tombstones/{state_id}``.
        Looks up the state row by ``(state_id, org_id)`` so a guessed
        id from another org is invisible. The state row's
        ``date_tombstoned`` / ``tombstone_reason`` / ``tombstone_note``
        are cleared in-place. When the row carries a non-null
        ``docverse_id`` and the matching ``editions`` / ``projects``
        row is still soft-deleted, that row's ``date_deleted`` is
        cleared in the *same* transaction — otherwise the next sync
        iteration would crash on the slug clash because the
        soft-deleted row still occupies the
        ``uq_editions_project_lower_slug`` index slot. Soft-delete on
        builds is not modelled in this codebase, so the build branch
        only clears the tombstone fields.

        Raises
        ------
        NotFoundError
            When no row matches ``(state_id, org_id)`` *or* the
            matched row is not tombstoned. (Clearing an already-clear
            row is treated as "no such tombstone" — the admin URL is
            meaningful only on a tombstoned row.)
        """
        from docverse.exceptions import NotFoundError  # noqa: PLC0415

        state = await self._state_store.get_by_id_for_org(
            state_id=state_id, org_id=org_id
        )
        if state is None or state.date_tombstoned is None:
            msg = f"No tombstone found for state_id={state_id}"
            raise NotFoundError(msg)

        await self._session.execute(
            update(SqlKeeperSyncState)
            .where(SqlKeeperSyncState.id == state_id)
            .values(
                date_tombstoned=None,
                tombstone_reason=None,
                tombstone_note=None,
            )
        )

        revived = False
        if state.docverse_id is not None:
            if state.resource_type == ResourceType.edition.value:
                edition_revive = await self._session.execute(
                    update(SqlEdition)
                    .where(
                        SqlEdition.id == state.docverse_id,
                        SqlEdition.date_deleted.is_not(None),
                    )
                    .values(date_deleted=None)
                    .returning(SqlEdition.id)
                )
                revived = edition_revive.scalar_one_or_none() is not None
            elif state.resource_type == ResourceType.project.value:
                project_revive = await self._session.execute(
                    update(SqlProject)
                    .where(
                        SqlProject.id == state.docverse_id,
                        SqlProject.date_deleted.is_not(None),
                    )
                    .values(date_deleted=None)
                    .returning(SqlProject.id)
                )
                revived = project_revive.scalar_one_or_none() is not None

        await self._session.flush()

        cleared = await self._state_store.get_by_id_for_org(
            state_id=state_id, org_id=org_id
        )
        assert cleared is not None  # we just cleared it; row still exists
        self._logger.info(
            "Sync tombstone cleared",
            org_id=org_id,
            state_id=state_id,
            resource_type=state.resource_type,
            ltd_id=state.ltd_id,
            ltd_slug=state.ltd_slug,
            previous_reason=state.tombstone_reason,
            revived_docverse_row=revived,
        )
        return ClearedTombstone(state=cleared, revived_docverse_row=revived)


def _derive_display_path(
    state: KeeperSyncState,
    *,
    editions_by_id: dict[int, SqlEdition],
    projects_by_id: dict[int, SqlProject],
) -> str:
    """Compose a Docverse-side display path for a tombstoned state row.

    For project rows this is the Docverse project slug; for edition
    rows it is ``<project_slug>/<edition_slug>``. Falls back to the
    LTD-side slug when no Docverse row is linked — i.e. for
    ``lifecycle_preemptive`` rows that veto an LTD edition that was
    never imported.
    """
    if state.resource_type == ResourceType.edition.value:
        if state.docverse_id is not None:
            edition = editions_by_id.get(state.docverse_id)
            if edition is not None:
                project = projects_by_id.get(edition.project_id)
                if project is not None:
                    return f"{project.slug}/{edition.slug}"
                return edition.slug
        return state.ltd_slug
    if state.resource_type == ResourceType.project.value:
        if state.docverse_id is not None:
            project = projects_by_id.get(state.docverse_id)
            if project is not None:
                return project.slug
        return state.ltd_slug
    # builds (not used by the admin UI today but kept defensive)
    return state.ltd_slug
