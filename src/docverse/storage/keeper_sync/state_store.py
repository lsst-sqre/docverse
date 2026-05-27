"""Async data-access for the ``keeper_sync_state`` table.

One row per LTD↔Docverse pairing for a project / edition / build. The
idempotency key the sync engine uses is per-resource: project rows are
keyed on ``(org_id, ltd_slug)`` because LTD's product API has no
integer id, while edition and build rows are keyed on
``(org_id, resource_type, ltd_id)`` because LTD's edition / build
slugs are only unique within a product.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import BaseModel, ConfigDict
from safir.database import CountedPaginatedList, CountedPaginatedQueryRunner
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import ColumnElement

from docverse.dbschema.keeper_sync_state import SqlKeeperSyncState

if TYPE_CHECKING:
    from docverse.storage.pagination import KeeperSyncProjectStateIdCursor

__all__ = [
    "KeeperSyncState",
    "KeeperSyncStateStore",
    "ResourceType",
    "TombstoneReason",
]


class ResourceType(StrEnum):
    """LTD resource types tracked in ``keeper_sync_state``."""

    project = "project"
    edition = "edition"
    build = "build"


class TombstoneReason(StrEnum):
    """Why a ``keeper_sync_state`` row was tombstoned.

    Mirrors the SQL ``CheckConstraint`` on ``tombstone_reason``. The
    values are stable wire identifiers — admin API responses and
    operator-facing logs use them as-is.
    """

    manual_delete = "manual_delete"
    """Operator-driven soft-delete of the Docverse-side resource."""

    lifecycle_delete = "lifecycle_delete"
    """Automated soft-delete by ``lifecycle_eval`` / ``git_ref_audit`` /
    the ``ref_deleted`` webhook."""

    lifecycle_preemptive = "lifecycle_preemptive"
    """Sync itself short-circuited an LTD edition that the lifecycle
    rules would immediately delete, before the build content was
    copied. No matching Docverse row exists in this case."""


class KeeperSyncState(BaseModel):
    """Domain representation of a ``keeper_sync_state`` row.

    Documented ``annotations`` keys
    -------------------------------
    Project-resource rows
        ``main_edition_url`` / ``main_edition_ltd_id`` — the resolved
        LTD ``main`` edition pointer cached by
        :func:`docverse.worker.functions.keeper_sync._tier_main_for_org`
        so subsequent ticks issue one ``GET /editions/<id>`` instead of
        walking the project's edition URL list.

        ``date_main_last_polled`` — ISO-8601 timestamp of the last LTD
        fetch issued by ``_tier_main_for_org`` for this project,
        consumed by
        :func:`docverse.services.keeper_sync.scheduler.should_poll_main_for_project`
        so dormant projects (those whose LTD ``main`` rebuild predates
        the hot window) cap their LTD load at one fetch per
        ``TIER_MAIN_DORMANT_INTERVAL``.
    Edition-resource rows
        ``ltd_mode`` / ``ltd_tracked_refs`` — the LTD-side edition
        mode / refs preserved for reversibility (used by the ``manual``
        mapper path).
    """

    model_config = ConfigDict(from_attributes=True)

    id: int
    org_id: int
    resource_type: str
    ltd_id: int | None = None
    ltd_slug: str
    docverse_id: int | None = None
    date_last_synced: datetime | None = None
    date_rebuilt_seen: datetime | None = None
    last_seen_etag: str | None = None
    content_hash: str | None = None
    annotations: dict[str, Any] | None = None
    date_tombstoned: datetime | None = None
    tombstone_reason: str | None = None
    tombstone_note: str | None = None


def _key_clauses(
    *,
    org_id: int,
    resource_type: ResourceType,
    ltd_id: int | None,
    ltd_slug: str | None,
) -> list[ColumnElement[bool]]:
    """Build the WHERE clause for the per-resource idempotency key.

    Project rows are slug-keyed (LTD products have no integer id);
    edition / build rows are id-keyed (LTD edition / build slugs are
    only unique within a product, but ``keeper_sync_state`` rows are
    org-scoped). Callers must supply ``ltd_slug`` for projects and
    ``ltd_id`` for editions / builds; passing the other variant for a
    given resource type is a programming error.
    """
    if resource_type is ResourceType.project:
        if ltd_slug is None:
            msg = "ltd_slug is required for project lookups"
            raise ValueError(msg)
        if ltd_id is not None:
            msg = "ltd_id must be None for project lookups (slug-keyed)"
            raise ValueError(msg)
        return [
            SqlKeeperSyncState.org_id == org_id,
            SqlKeeperSyncState.resource_type == resource_type.value,
            SqlKeeperSyncState.ltd_slug == ltd_slug,
        ]
    if ltd_id is None:
        msg = f"ltd_id is required for {resource_type.value} lookups"
        raise ValueError(msg)
    return [
        SqlKeeperSyncState.org_id == org_id,
        SqlKeeperSyncState.resource_type == resource_type.value,
        SqlKeeperSyncState.ltd_id == ltd_id,
    ]


class KeeperSyncStateStore:
    """Direct database operations for ``keeper_sync_state`` rows."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def get(
        self,
        *,
        org_id: int,
        resource_type: ResourceType,
        ltd_id: int | None = None,
        ltd_slug: str | None = None,
        include_tombstoned: bool = False,
    ) -> KeeperSyncState | None:
        """Fetch a row by its per-resource idempotency key.

        Pass ``ltd_slug`` for projects and ``ltd_id`` for editions /
        builds; see :func:`_key_clauses`. Tombstoned rows are filtered
        out by default so existing convergence / short-circuit callers
        treat a tombstoned LTD resource as "not present"; pass
        ``include_tombstoned=True`` for the tombstone-service write
        path and admin endpoints that need to see them.
        """
        clauses = _key_clauses(
            org_id=org_id,
            resource_type=resource_type,
            ltd_id=ltd_id,
            ltd_slug=ltd_slug,
        )
        if not include_tombstoned:
            clauses.append(SqlKeeperSyncState.date_tombstoned.is_(None))
        stmt = select(SqlKeeperSyncState).where(*clauses)
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return KeeperSyncState.model_validate(row)

    async def get_by_docverse_id(
        self,
        *,
        org_id: int,
        resource_type: ResourceType,
        docverse_id: int,
    ) -> KeeperSyncState | None:
        """Fetch a row by ``(org_id, resource_type, docverse_id)``.

        ``keeper_sync_state.docverse_id`` is the Docverse-side row id
        for the paired project / edition / build. Callers that already
        know the Docverse id (e.g. the per-project status endpoint
        looking up the ``__main`` edition's state row) can use this
        indexed single-row lookup instead of scanning every state row
        for the org with :meth:`list_for_org` and filtering in memory.
        Returns ``None`` when no matching row exists.
        """
        stmt = select(SqlKeeperSyncState).where(
            SqlKeeperSyncState.org_id == org_id,
            SqlKeeperSyncState.resource_type == resource_type.value,
            SqlKeeperSyncState.docverse_id == docverse_id,
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return KeeperSyncState.model_validate(row)

    async def list_for_org(
        self,
        *,
        org_id: int,
        resource_type: ResourceType,
        ltd_ids: Iterable[int] | None = None,
        docverse_ids: Iterable[int] | None = None,
        include_tombstoned: bool = False,
    ) -> list[KeeperSyncState]:
        """Return every state row for ``(org_id, resource_type)``.

        Replaces the per-edition ``get`` round-trips the tier-cron
        worker functions used to make: callers fetch the org's rows
        once and resolve presence / staleness via an in-memory dict
        keyed on ``ltd_id``. Pass ``ltd_ids`` to scope the query to a
        known LTD-side id set (used by tier_other so the WHERE clause
        only spans editions LTD currently lists). Pass ``docverse_ids``
        to scope to a known Docverse-side id set (used by the
        per-project read paths whose scope is "this project's
        editions" — ``keeper_sync_state`` has no ``project_id``
        column, so the Docverse edition ids are the natural project
        scope). Passing an empty ``ltd_ids`` or ``docverse_ids``
        returns ``[]`` without hitting the database.

        Tombstoned rows are filtered out by default so the
        convergence and short-circuit callers see a tombstoned LTD
        resource as "not present"; admin endpoints and the
        tombstone-service write path opt in with
        ``include_tombstoned=True``.
        """
        if ltd_ids is not None:
            ltd_id_list = list(ltd_ids)
            if not ltd_id_list:
                return []
        else:
            ltd_id_list = None
        if docverse_ids is not None:
            docverse_id_list = list(docverse_ids)
            if not docverse_id_list:
                return []
        else:
            docverse_id_list = None
        clauses: list[ColumnElement[bool]] = [
            SqlKeeperSyncState.org_id == org_id,
            SqlKeeperSyncState.resource_type == resource_type.value,
        ]
        if ltd_id_list is not None:
            clauses.append(SqlKeeperSyncState.ltd_id.in_(ltd_id_list))
        if docverse_id_list is not None:
            clauses.append(
                SqlKeeperSyncState.docverse_id.in_(docverse_id_list)
            )
        if not include_tombstoned:
            clauses.append(SqlKeeperSyncState.date_tombstoned.is_(None))
        stmt = select(SqlKeeperSyncState).where(*clauses)
        result = await self._session.execute(stmt)
        return [
            KeeperSyncState.model_validate(row)
            for row in result.scalars().all()
        ]

    async def list_project_resources_for_org(
        self,
        *,
        org_id: int,
        cursor: KeeperSyncProjectStateIdCursor | None,
        limit: int,
        include_tombstoned: bool = False,
    ) -> CountedPaginatedList[KeeperSyncState, KeeperSyncProjectStateIdCursor]:
        """Return a paginated page of project-resource rows for an org.

        Used by the org-scoped paginated keeper-sync projects listing.
        Ordered by ``id DESC`` via
        :class:`docverse.storage.pagination.KeeperSyncProjectStateIdCursor`
        so newest-discovered projects appear first.

        Tombstoned rows are filtered out by default so the operator
        projects listing does not show LTD products that have been
        deleted on the Docverse side; pass ``include_tombstoned=True``
        from the admin tombstones endpoint.
        """
        from docverse.storage.pagination import (  # noqa: PLC0415
            KeeperSyncProjectStateIdCursor,
        )

        clauses: list[ColumnElement[bool]] = [
            SqlKeeperSyncState.org_id == org_id,
            SqlKeeperSyncState.resource_type == ResourceType.project.value,
        ]
        if not include_tombstoned:
            clauses.append(SqlKeeperSyncState.date_tombstoned.is_(None))
        stmt = select(SqlKeeperSyncState).where(*clauses)
        runner = CountedPaginatedQueryRunner(
            entry_type=KeeperSyncState,
            cursor_type=KeeperSyncProjectStateIdCursor,
        )
        return await runner.query_object(
            self._session, stmt, cursor=cursor, limit=limit
        )

    async def upsert(  # noqa: PLR0913
        self,
        *,
        org_id: int,
        resource_type: ResourceType,
        ltd_slug: str,
        ltd_id: int | None = None,
        docverse_id: int | None = None,
        date_last_synced: datetime | None = None,
        date_rebuilt_seen: datetime | None = None,
        last_seen_etag: str | None = None,
        content_hash: str | None = None,
        annotations: dict[str, Any] | None = None,
    ) -> KeeperSyncState:
        """Insert or update a row by its per-resource idempotency key.

        The key is ``(org_id, ltd_slug)`` for projects and
        ``(org_id, resource_type, ltd_id)`` for editions / builds. On
        update, only non-``None`` fields overwrite the existing row;
        ``None`` arguments preserve whatever value is already stored.
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
        if row is None:
            row = SqlKeeperSyncState(
                org_id=org_id,
                resource_type=resource_type.value,
                ltd_id=ltd_id,
                ltd_slug=ltd_slug,
                docverse_id=docverse_id,
                date_last_synced=date_last_synced,
                date_rebuilt_seen=date_rebuilt_seen,
                last_seen_etag=last_seen_etag,
                content_hash=content_hash,
                annotations=annotations,
            )
            self._session.add(row)
        else:
            row.ltd_slug = ltd_slug
            if docverse_id is not None:
                row.docverse_id = docverse_id
            if date_last_synced is not None:
                row.date_last_synced = date_last_synced
            if date_rebuilt_seen is not None:
                row.date_rebuilt_seen = date_rebuilt_seen
            if last_seen_etag is not None:
                row.last_seen_etag = last_seen_etag
            if content_hash is not None:
                row.content_hash = content_hash
            if annotations is not None:
                row.annotations = annotations
        await self._session.flush()
        await self._session.refresh(row)
        return KeeperSyncState.model_validate(row)
