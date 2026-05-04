"""Async data-access for the ``keeper_sync_state`` table.

One row per LTD↔Docverse pairing for a project / edition / build. The
``(org_id, resource_type, ltd_id)`` triple is the idempotency key the
sync engine uses to either short-circuit a re-import (state matches
LTD) or resume one (state differs).
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

import structlog
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

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
    ltd_id: int
    ltd_slug: str
    docverse_id: int | None = None
    date_last_synced: datetime | None = None
    date_rebuilt_seen: datetime | None = None
    last_seen_etag: str | None = None
    content_hash: str | None = None
    annotations: dict[str, Any] | None = None


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
        ltd_id: int,
    ) -> KeeperSyncState | None:
        """Fetch the row keyed by ``(org_id, resource_type, ltd_id)``."""
        stmt = select(SqlKeeperSyncState).where(
            SqlKeeperSyncState.org_id == org_id,
            SqlKeeperSyncState.resource_type == resource_type.value,
            SqlKeeperSyncState.ltd_id == ltd_id,
        )
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
        ltd_id: int,
        ltd_slug: str,
        docverse_id: int | None = None,
        date_last_synced: datetime | None = None,
        date_rebuilt_seen: datetime | None = None,
        last_seen_etag: str | None = None,
        content_hash: str | None = None,
        annotations: dict[str, Any] | None = None,
    ) -> KeeperSyncState:
        """Insert or update the row for ``(org_id, resource_type, ltd_id)``.

        On update, only non-``None`` fields overwrite the existing row;
        ``None`` arguments preserve whatever value is already stored.
        """
        existing = await self._session.execute(
            select(SqlKeeperSyncState).where(
                SqlKeeperSyncState.org_id == org_id,
                SqlKeeperSyncState.resource_type == resource_type.value,
                SqlKeeperSyncState.ltd_id == ltd_id,
            )
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
