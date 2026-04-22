"""Database operations for dashboard template content + content files.

Owns both ``dashboard_template_contents`` and
``dashboard_template_content_files`` because the upsert-by-key
operation writes to both tables atomically — when the GitHub ETag
changes, the content row is updated and its files are replaced as a
single unit.
"""

from __future__ import annotations

from dataclasses import dataclass

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.dashboard_template_content import (
    SqlDashboardTemplateContent,
)
from docverse.dbschema.dashboard_template_content_file import (
    SqlDashboardTemplateContentFile,
)
from docverse.domain.dashboard_template_content import DashboardTemplateContent
from docverse.domain.dashboard_template_content_file import (
    DashboardTemplateContentFile,
)


@dataclass(frozen=True)
class ContentKey:
    """The dedup key for a content row."""

    github_owner: str
    github_repo: str
    github_ref: str
    root_path: str


@dataclass(frozen=True)
class ContentFileInput:
    """One file's bytes for a content upsert."""

    relative_path: str
    is_text: bool
    data: bytes


@dataclass(frozen=True)
class UpsertResult:
    """Outcome of an :meth:`DashboardTemplateContentStore.upsert` call.

    ``changed`` is ``False`` when the upsert was a no-op because the
    ETag matched the existing row — useful for callers that want to
    short-circuit a fan-out when nothing actually moved.
    """

    content: DashboardTemplateContent
    changed: bool


class DashboardTemplateContentStore:
    """Direct database operations for template content + content files."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def get_by_id(
        self, content_id: int
    ) -> DashboardTemplateContent | None:
        """Fetch a content row by internal ID."""
        row = await self._get_row_by_id(content_id)
        if row is None:
            return None
        return DashboardTemplateContent.model_validate(row)

    async def get_by_key(
        self, key: ContentKey
    ) -> DashboardTemplateContent | None:
        """Fetch a content row by its dedup key."""
        row = await self._get_row_by_key(key)
        if row is None:
            return None
        return DashboardTemplateContent.model_validate(row)

    async def upsert(
        self,
        *,
        key: ContentKey,
        commit_sha: str,
        etag: str,
        template_toml: bytes,
        files: list[ContentFileInput],
    ) -> UpsertResult:
        """Insert or update a content row + its files.

        Idempotency: if a row with the dedup ``key`` already exists and
        its ``etag`` matches the supplied value, no rows are written
        and the existing row is returned with ``changed=False``. When
        the ETag differs (or the row is absent), the content row is
        inserted/updated and *all* of its existing file rows are
        replaced with the supplied list.
        """
        existing = await self._get_row_by_key(key)
        if existing is not None and existing.etag == etag:
            return UpsertResult(
                content=DashboardTemplateContent.model_validate(existing),
                changed=False,
            )

        if existing is None:
            row = SqlDashboardTemplateContent(
                github_owner=key.github_owner,
                github_repo=key.github_repo,
                github_ref=key.github_ref,
                root_path=key.root_path,
                commit_sha=commit_sha,
                etag=etag,
                template_toml=template_toml,
            )
            self._session.add(row)
            await self._session.flush()
        else:
            row = existing
            row.commit_sha = commit_sha
            row.etag = etag
            row.template_toml = template_toml
            await self._session.flush()
            await self._session.execute(
                delete(SqlDashboardTemplateContentFile).where(
                    SqlDashboardTemplateContentFile.content_id == row.id,
                )
            )
            await self._session.flush()

        for file_input in files:
            self._session.add(
                SqlDashboardTemplateContentFile(
                    content_id=row.id,
                    relative_path=file_input.relative_path,
                    is_text=file_input.is_text,
                    data=file_input.data,
                    size_bytes=len(file_input.data),
                )
            )
        await self._session.flush()
        await self._session.refresh(row)
        return UpsertResult(
            content=DashboardTemplateContent.model_validate(row),
            changed=True,
        )

    async def list_files(
        self, content_id: int
    ) -> list[DashboardTemplateContentFile]:
        """List every file row for a content id, ordered by path."""
        result = await self._session.execute(
            select(SqlDashboardTemplateContentFile)
            .where(SqlDashboardTemplateContentFile.content_id == content_id)
            .order_by(SqlDashboardTemplateContentFile.relative_path)
        )
        return [
            DashboardTemplateContentFile.model_validate(r)
            for r in result.scalars().all()
        ]

    async def get_file(
        self, *, content_id: int, relative_path: str
    ) -> DashboardTemplateContentFile | None:
        """Fetch a single file by content id and relative path."""
        result = await self._session.execute(
            select(SqlDashboardTemplateContentFile).where(
                SqlDashboardTemplateContentFile.content_id == content_id,
                SqlDashboardTemplateContentFile.relative_path == relative_path,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return DashboardTemplateContentFile.model_validate(row)

    async def _get_row_by_id(
        self, content_id: int
    ) -> SqlDashboardTemplateContent | None:
        result = await self._session.execute(
            select(SqlDashboardTemplateContent).where(
                SqlDashboardTemplateContent.id == content_id,
            )
        )
        return result.scalar_one_or_none()

    async def _get_row_by_key(
        self, key: ContentKey
    ) -> SqlDashboardTemplateContent | None:
        result = await self._session.execute(
            select(SqlDashboardTemplateContent).where(
                SqlDashboardTemplateContent.github_owner == key.github_owner,
                SqlDashboardTemplateContent.github_repo == key.github_repo,
                SqlDashboardTemplateContent.github_ref == key.github_ref,
                SqlDashboardTemplateContent.root_path == key.root_path,
            )
        )
        return result.scalar_one_or_none()
