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

from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.dbschema.keeper_sync_state import SqlKeeperSyncState
from docverse_server.dbschema.project import SqlProject
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.storage._public_id import (
    insert_with_time_ordered_public_id,
)
from docverse_server.storage.keeper_sync import (
    KeeperSyncState,
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse_server.storage.keeper_sync.state_store import _key_clauses
from docverse_server.storage.pagination import (
    KeeperSyncStateDateTombstonedCursor,
)

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

    page: CountedPaginatedList[
        KeeperSyncState, KeeperSyncStateDateTombstonedCursor
    ]
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

    The flag speaks only for the addressed row. Reviving a project also
    revives the editions and builds its delete cascaded to, and how many
    of each came back — and which purged builds could not — is reported
    in the log, not here: no caller branches on those counts.
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

    async def record(
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
        :func:`docverse_server.storage.keeper_sync.state_store._key_clauses`.
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
            # Every state row carries a time-ordered ``public_id``, minted
            # here with the same collision-retry helper the state store's
            # upsert uses.
            resolved_slug = ltd_slug if ltd_slug is not None else str(ltd_id)

            def _make_row(public_id: int) -> SqlKeeperSyncState:
                return SqlKeeperSyncState(
                    public_id=public_id,
                    org_id=org_id,
                    resource_type=resource_type.value,
                    ltd_id=ltd_id,
                    ltd_slug=resolved_slug,
                    date_tombstoned=now,
                    tombstone_reason=reason.value,
                    tombstone_note=note,
                )

            row = await insert_with_time_ordered_public_id(
                self._session, _make_row
            )
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
        cursor: KeeperSyncStateDateTombstonedCursor | None,
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
        public_id: int,
        org_id: int,
    ) -> ClearedTombstone:
        """Clear the tombstone on a state row, reviving its Docverse row.

        Backs ``DELETE /orgs/{org}/keeper-sync/tombstones/{tombstone}``.
        Looks up the state row by ``(public_id, org_id)`` — its Base32
        public id scoped to the org — so a guessed id from another org
        is invisible. The state row's ``date_tombstoned`` /
        ``tombstone_reason`` / ``tombstone_note`` are cleared in-place.
        When the row carries a non-null ``docverse_id`` and the
        matching ``editions`` / ``projects`` row is still soft-deleted,
        that row's ``date_deleted`` is cleared in the *same*
        transaction — otherwise the next sync iteration would crash on
        the slug clash because the soft-deleted row still occupies the
        ``uq_editions_project_lower_slug`` index slot. A project revive
        reaches further, into the editions and builds its delete
        cascaded to; see :meth:`_revive_project`. A ``build`` state row
        has no revive branch: builds are addressed by the admin restore
        endpoint, not by keeper-sync recovery.

        Raises
        ------
        NotFoundError
            When no row matches ``(public_id, org_id)`` *or* the
            matched row is not tombstoned. (Clearing an already-clear
            row is treated as "no such tombstone" — the admin URL is
            meaningful only on a tombstoned row.)
        """
        from docverse_server.exceptions import (  # noqa: PLC0415
            KeeperSyncInvariantError,
            NotFoundError,
        )

        state = await self._state_store.get_by_public_id_for_org(
            public_id=public_id, org_id=org_id
        )
        if state is None or state.date_tombstoned is None:
            msg = f"No tombstone found for public_id={public_id}"
            raise NotFoundError(msg)
        state_id = state.id

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
                revived = await self._revive_project(
                    org_id=org_id, project_id=state.docverse_id
                )

        await self._session.flush()

        cleared = await self._state_store.get_by_id_for_org(
            state_id=state_id, org_id=org_id
        )
        if cleared is None:
            msg = (
                f"State row state_id={state_id} vanished after clearing its "
                "tombstone"
            )
            raise KeeperSyncInvariantError(msg)
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

    async def _revive_project(self, *, org_id: int, project_id: int) -> bool:
        """Revive a soft-deleted project along with its cascade siblings.

        ``ProjectStore.soft_delete`` stamps the project's live editions
        and builds in the same transaction, so all three tables land on
        one ``func.now()`` instant. Reviving the project row alone would
        leave those siblings deleted: the project would come back
        serving nothing, and the next keeper-sync iteration would hit
        the very slug clash the revive exists to prevent, one level
        down.

        That shared timestamp is also how this tells a cascade sibling
        from a row an operator deleted on its own: only rows whose
        ``date_deleted`` equals the project's came back through this
        delete, so an edition or build deleted by hand earlier keeps its
        own timestamp and stays deleted. Undeleting those would silently
        reverse a decision this tombstone never spoke to.

        The editions' ``keeper_sync_state`` tombstones are cleared with
        them. The cascade goes through
        :meth:`~docverse_server.storage.edition_store.EditionStore.soft_delete_all_by_project`,
        which stamps every edition's state row exactly as a per-edition
        delete would, and a tombstoned state row makes
        :meth:`~docverse_server.services.keeper_sync.service.KeeperSyncService.sync_edition`
        short-circuit forever. Reviving the rows without clearing those
        stamps would bring an LTD-imported project back live but
        permanently unsynced, and leave the operator looking at N
        edition tombstones they never created. The same shared instant
        that identifies a cascade sibling identifies its tombstone, so
        an edition an operator tombstoned by hand earlier keeps it.

        A build the ``purgatory_cleanup`` sweep already purged stays
        deleted whatever its timestamp says — its tree and tarball are
        gone, so a live row would point at nothing — and is named at
        warning level, together with any revived edition still pointing
        at it, so the operator learns both what the revive could not
        bring back and which editions need rolling back before they
        serve 404s. Editions have no purge of their own; they are
        pointers, and the build they point at is checked when something
        tries to serve it.

        Returns
        -------
        bool
            True when the project was soft-deleted and is now live,
            False when it was already live (or no longer exists).
        """
        project_deleted = (
            await self._session.execute(
                select(SqlProject.date_deleted).where(
                    SqlProject.id == project_id,
                    SqlProject.date_deleted.is_not(None),
                )
            )
        ).scalar_one_or_none()
        if project_deleted is None:
            return False

        # Read before the update: once ``date_deleted`` is cleared the
        # sibling predicate can no longer find these rows.
        purged_rows = (
            await self._session.execute(
                select(SqlBuild.id, SqlBuild.public_id).where(
                    SqlBuild.project_id == project_id,
                    SqlBuild.date_deleted == project_deleted,
                    SqlBuild.date_purged.is_not(None),
                )
            )
        ).all()
        purged_ids = [row.id for row in purged_rows]
        purged_public_ids = [row.public_id for row in purged_rows]

        await self._session.execute(
            update(SqlProject)
            .where(SqlProject.id == project_id)
            .values(date_deleted=None)
        )
        edition_ids = (
            (
                await self._session.execute(
                    update(SqlEdition)
                    .where(
                        SqlEdition.project_id == project_id,
                        SqlEdition.date_deleted == project_deleted,
                    )
                    .values(date_deleted=None)
                    .returning(SqlEdition.id)
                )
            )
            .scalars()
            .all()
        )
        if edition_ids:
            # The mirror of the single-edition clear branch above: the
            # cascade stamped ``date_tombstoned`` with the same
            # transaction-stable ``func.now()`` as the project's
            # ``date_deleted``, so this equality reaches exactly the
            # tombstones this delete wrote and leaves a hand-tombstoned
            # edition's own stamp alone.
            await self._session.execute(
                update(SqlKeeperSyncState)
                .where(
                    SqlKeeperSyncState.org_id == org_id,
                    SqlKeeperSyncState.resource_type
                    == ResourceType.edition.value,
                    SqlKeeperSyncState.docverse_id.in_(edition_ids),
                    SqlKeeperSyncState.date_tombstoned == project_deleted,
                )
                .values(
                    date_tombstoned=None,
                    tombstone_reason=None,
                    tombstone_note=None,
                )
            )
        build_ids = (
            (
                await self._session.execute(
                    update(SqlBuild)
                    .where(
                        SqlBuild.project_id == project_id,
                        SqlBuild.date_deleted == project_deleted,
                        SqlBuild.date_purged.is_(None),
                    )
                    .values(date_deleted=None)
                    .returning(SqlBuild.id)
                )
            )
            .scalars()
            .all()
        )

        if purged_ids:
            # Named the way the sweep names the editions pinning a
            # build: these come back live with a CDN pointer at a tree
            # that is gone, so every URL under them 404s until an
            # operator rolls them back.
            dangling_slugs: list[str] = []
            if edition_ids:
                dangling_slugs = list(
                    (
                        await self._session.execute(
                            select(SqlEdition.slug)
                            .where(
                                SqlEdition.id.in_(edition_ids),
                                SqlEdition.current_build_id.in_(purged_ids),
                            )
                            .order_by(SqlEdition.slug)
                        )
                    )
                    .scalars()
                    .all()
                )
            self._logger.warning(
                "Left purged builds deleted while reviving their project",
                org_id=org_id,
                project_id=project_id,
                build_public_ids=[
                    serialize_base32_id(public_id)
                    for public_id in purged_public_ids
                ],
                edition_slugs=dangling_slugs,
            )
        self._logger.info(
            "Revived a project and its cascade-deleted rows",
            org_id=org_id,
            project_id=project_id,
            editions_revived=len(edition_ids),
            builds_revived=len(build_ids),
            builds_left_purged=len(purged_public_ids),
        )
        return True


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
