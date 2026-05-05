"""Async data-access for the ``keeper_sync_state`` table.

One row per LTD↔Docverse pairing for a project / edition / build. The
idempotency key the sync engine uses is per-resource: project rows are
keyed on ``(org_id, ltd_slug)`` because LTD's product API has no
integer id, while edition and build rows are keyed on
``(org_id, resource_type, ltd_id)`` because LTD's edition / build
slugs are only unique within a product.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

import structlog
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import ColumnElement

from docverse.dbschema.keeper_sync_state import SqlKeeperSyncState

__all__ = [
    "KeeperSyncState",
    "KeeperSyncStateStore",
    "ResourceType",
]


class ResourceType(StrEnum):
    """LTD resource types tracked in ``keeper_sync_state``."""

    project = "project"
    edition = "edition"
    build = "build"


class KeeperSyncState(BaseModel):
    """Domain representation of a ``keeper_sync_state`` row."""

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
    ) -> KeeperSyncState | None:
        """Fetch a row by its per-resource idempotency key.

        Pass ``ltd_slug`` for projects and ``ltd_id`` for editions /
        builds; see :func:`_key_clauses`.
        """
        clauses = _key_clauses(
            org_id=org_id,
            resource_type=resource_type,
            ltd_id=ltd_id,
            ltd_slug=ltd_slug,
        )
        stmt = select(SqlKeeperSyncState).where(*clauses)
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return KeeperSyncState.model_validate(row)

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
