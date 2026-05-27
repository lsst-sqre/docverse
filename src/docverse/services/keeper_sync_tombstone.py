"""Service for writing and reading keeper-sync tombstones.

A *sync tombstone* is a permanent veto recorded on the existing
``keeper_sync_state`` table that tells the keeper-sync engine "this
LTD resource has been deleted on the Docverse side; do not re-migrate
it." See PRD #332 / DM-54914 for the full design.

This service is the single, well-tested entrypoint every deletion path
goes through to write or check a tombstone. Subsequent slices of the
PRD wire individual call sites (manual delete, lifecycle delete, the
lifecycle-preemptive short-circuit, the admin API) through this
service; this slice ships only the storage primitives.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.keeper_sync_state import SqlKeeperSyncState
from docverse.storage.keeper_sync import (
    KeeperSyncState,
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse.storage.keeper_sync.state_store import _key_clauses

__all__ = ["KeeperSyncTombstoneService"]


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
