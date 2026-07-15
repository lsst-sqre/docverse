"""Database operations for GitHub template + template file rows.

Owns both ``dashboard_github_templates`` and
``dashboard_github_template_files`` because the upsert-by-key operation
writes to both tables atomically — when the GitHub ETag changes, the
template row is updated and its files are replaced as a single unit.
"""

from __future__ import annotations

from dataclasses import dataclass

import structlog
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.dashboard_github_template import (
    SqlDashboardGitHubTemplate,
)
from docverse.dbschema.dashboard_github_template_file import (
    SqlDashboardGitHubTemplateFile,
)
from docverse.domain.dashboard_github_template import (
    DashboardGitHubTemplate,
    DashboardGitHubTemplateFile,
)

from .source import GitHubTemplateSource

__all__ = [
    "DashboardGitHubTemplateStore",
    "GitHubTemplateFileInput",
    "GitHubTemplateKey",
    "UpsertResult",
]


@dataclass(frozen=True)
class GitHubTemplateKey:
    """The dedup key for a GitHub template row."""

    github_owner: str
    github_repo: str
    github_ref: str
    root_path: str


@dataclass(frozen=True)
class GitHubTemplateFileInput:
    """One file's bytes for a GitHub template upsert."""

    relative_path: str
    is_text: bool
    data: bytes


@dataclass(frozen=True)
class UpsertResult:
    """Outcome of a :meth:`DashboardGitHubTemplateStore.upsert` call.

    ``changed`` is ``False`` when the upsert was a no-op because the
    ETag matched the existing row — useful for callers that want to
    short-circuit a fan-out when nothing actually moved.
    """

    template: DashboardGitHubTemplate
    changed: bool


class DashboardGitHubTemplateStore:
    """Direct database operations for GitHub templates + template files."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def get_by_id(
        self, template_id: int
    ) -> DashboardGitHubTemplate | None:
        """Fetch a template row by internal ID."""
        row = await self._get_row_by_id(template_id)
        if row is None:
            return None
        return DashboardGitHubTemplate.model_validate(row)

    async def get_by_key(
        self, key: GitHubTemplateKey
    ) -> DashboardGitHubTemplate | None:
        """Fetch a template row by its dedup key."""
        row = await self._get_row_by_key(key)
        if row is None:
            return None
        return DashboardGitHubTemplate.model_validate(row)

    async def upsert(
        self,
        *,
        key: GitHubTemplateKey,
        commit_sha: str,
        etag: str,
        template_toml: bytes,
        files: list[GitHubTemplateFileInput],
        github_owner_id: int | None = None,
        github_repo_id: int | None = None,
    ) -> UpsertResult:
        """Insert or update a template row + its files.

        Idempotency: if a row with the dedup ``key`` already exists and
        its ``etag`` matches the supplied value, no rows are written
        and the existing row is returned with ``changed=False``. When
        the ETag differs (or the row is absent), the template row is
        inserted/updated and *all* of its existing file rows are
        replaced with the supplied list.

        ``github_owner_id`` / ``github_repo_id`` are only assigned when
        provided (non-``None``); passing ``None`` on an update leaves
        any previously-captured IDs in place.
        """
        existing = await self._get_row_by_key(key)
        if existing is not None and existing.etag == etag:
            return UpsertResult(
                template=DashboardGitHubTemplate.model_validate(existing),
                changed=False,
            )

        if existing is None:
            row = SqlDashboardGitHubTemplate(
                github_owner=key.github_owner,
                github_repo=key.github_repo,
                github_ref=key.github_ref,
                root_path=key.root_path,
                commit_sha=commit_sha,
                etag=etag,
                template_toml=template_toml,
                github_owner_id=github_owner_id,
                github_repo_id=github_repo_id,
            )
            self._session.add(row)
            await self._session.flush()
        else:
            row = existing
            row.commit_sha = commit_sha
            row.etag = etag
            row.template_toml = template_toml
            if github_owner_id is not None:
                row.github_owner_id = github_owner_id
            if github_repo_id is not None:
                row.github_repo_id = github_repo_id
            await self._session.flush()
            await self._session.execute(
                delete(SqlDashboardGitHubTemplateFile).where(
                    SqlDashboardGitHubTemplateFile.github_template_id
                    == row.id,
                )
            )
            await self._session.flush()

        for file_input in files:
            self._session.add(
                SqlDashboardGitHubTemplateFile(
                    github_template_id=row.id,
                    relative_path=file_input.relative_path,
                    is_text=file_input.is_text,
                    data=file_input.data,
                    size_bytes=len(file_input.data),
                )
            )
        await self._session.flush()
        await self._session.refresh(row)
        return UpsertResult(
            template=DashboardGitHubTemplate.model_validate(row),
            changed=True,
        )

    async def load_preloaded_source(
        self, template_id: int
    ) -> GitHubTemplateSource:
        """Construct and preload a :class:`GitHubTemplateSource`.

        The returned source has already had its async ``preload`` step
        run, so the synchronous :class:`TemplateSource` reads
        (``load_config`` / ``read_template`` / ``read_asset``) serve
        from the in-memory cache without further I/O.

        Raises
        ------
        LookupError
            If no template row exists for ``template_id``.
        """
        source = GitHubTemplateSource(
            template_id=template_id, session=self._session
        )
        try:
            await source.preload()
        except LookupError as exc:
            msg = (
                f"DashboardGitHubTemplate {template_id} not found while "
                "loading preloaded source"
            )
            raise LookupError(msg) from exc
        return source

    async def list_files(
        self, template_id: int
    ) -> list[DashboardGitHubTemplateFile]:
        """List every file row for a template id, ordered by path."""
        result = await self._session.execute(
            select(SqlDashboardGitHubTemplateFile)
            .where(
                SqlDashboardGitHubTemplateFile.github_template_id
                == template_id,
            )
            .order_by(SqlDashboardGitHubTemplateFile.relative_path)
        )
        return [
            DashboardGitHubTemplateFile.model_validate(r)
            for r in result.scalars().all()
        ]

    async def get_file(
        self, *, template_id: int, relative_path: str
    ) -> DashboardGitHubTemplateFile | None:
        """Fetch a single file by template id and relative path."""
        result = await self._session.execute(
            select(SqlDashboardGitHubTemplateFile).where(
                SqlDashboardGitHubTemplateFile.github_template_id
                == template_id,
                SqlDashboardGitHubTemplateFile.relative_path == relative_path,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return DashboardGitHubTemplateFile.model_validate(row)

    async def rename_repo_by_repo_id(
        self,
        *,
        github_repo_id: int,
        new_repo: str,
    ) -> list[int]:
        """Rewrite ``github_repo`` on all content rows keyed by stable repo ID.

        The synced bytes themselves don't change — only the dedup-key
        component that carries the GitHub display name. Keeps the
        binding's ETag short-circuit on the next sync from re-fetching
        a tree it already has.
        """
        stmt = (
            update(SqlDashboardGitHubTemplate)
            .where(
                SqlDashboardGitHubTemplate.github_repo_id == github_repo_id,
            )
            .values(github_repo=new_repo)
            .returning(SqlDashboardGitHubTemplate.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return [row[0] for row in result.all()]

    async def transfer_repo_by_repo_id(
        self,
        *,
        github_repo_id: int,
        new_owner: str,
        new_owner_id: int,
        new_repo: str,
    ) -> list[int]:
        """Rewrite owner + repo strings + ``github_owner_id`` on transfer.

        Mirror of :meth:`DashboardGitHubTemplateBindingStore
        .transfer_repo_by_repo_id` for the synced content row whose
        dedup key includes the owner login.
        """
        stmt = (
            update(SqlDashboardGitHubTemplate)
            .where(
                SqlDashboardGitHubTemplate.github_repo_id == github_repo_id,
            )
            .values(
                github_owner=new_owner,
                github_owner_id=new_owner_id,
                github_repo=new_repo,
            )
            .returning(SqlDashboardGitHubTemplate.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return [row[0] for row in result.all()]

    async def rename_owner_by_owner_id(
        self,
        *,
        github_owner_id: int,
        new_owner: str,
    ) -> list[int]:
        """Rewrite ``github_owner`` on content rows keyed by owner ID."""
        stmt = (
            update(SqlDashboardGitHubTemplate)
            .where(
                SqlDashboardGitHubTemplate.github_owner_id == github_owner_id,
            )
            .values(github_owner=new_owner)
            .returning(SqlDashboardGitHubTemplate.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return [row[0] for row in result.all()]

    async def _get_row_by_id(
        self, template_id: int
    ) -> SqlDashboardGitHubTemplate | None:
        result = await self._session.execute(
            select(SqlDashboardGitHubTemplate).where(
                SqlDashboardGitHubTemplate.id == template_id,
            )
        )
        return result.scalar_one_or_none()

    async def _get_row_by_key(
        self, key: GitHubTemplateKey
    ) -> SqlDashboardGitHubTemplate | None:
        result = await self._session.execute(
            select(SqlDashboardGitHubTemplate).where(
                SqlDashboardGitHubTemplate.github_owner == key.github_owner,
                SqlDashboardGitHubTemplate.github_repo == key.github_repo,
                SqlDashboardGitHubTemplate.github_ref == key.github_ref,
                SqlDashboardGitHubTemplate.root_path == key.root_path,
            )
        )
        return result.scalar_one_or_none()
