"""Template source backed by a synced GitHub template row."""

from __future__ import annotations

from typing import NoReturn

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.dashboard_github_template import (
    SqlDashboardGitHubTemplate,
)
from docverse.dbschema.dashboard_github_template_file import (
    SqlDashboardGitHubTemplateFile,
)

from ..template_source import ParsedTemplateConfig, parse_template_toml

__all__ = ["GitHubTemplateSource"]


class GitHubTemplateSource:
    """Template source backed by a ``dashboard_github_templates`` row.

    Construction is cheap; the actual file bytes are loaded once via
    :meth:`preload`. The :class:`TemplateSource` protocol's methods are
    synchronous, so all DB I/O is funneled through the async ``preload``
    step and the read methods then serve from an in-memory cache.
    """

    def __init__(self, *, template_id: int, session: AsyncSession) -> None:
        self._template_id = template_id
        self._session = session
        self._template_toml: bytes | None = None
        self._files: dict[str, bytes] = {}
        self._config: ParsedTemplateConfig | None = None

    async def preload(self) -> None:
        """Load the template row and its files into memory.

        Raises
        ------
        LookupError
            If no template row exists for ``template_id``.
        """
        template_result = await self._session.execute(
            select(SqlDashboardGitHubTemplate).where(
                SqlDashboardGitHubTemplate.id == self._template_id,
            )
        )
        template_row = template_result.scalar_one_or_none()
        if template_row is None:
            msg = f"DashboardGitHubTemplate {self._template_id} not found"
            raise LookupError(msg)
        self._template_toml = template_row.template_toml

        files_result = await self._session.execute(
            select(SqlDashboardGitHubTemplateFile).where(
                SqlDashboardGitHubTemplateFile.github_template_id
                == self._template_id,
            )
        )
        self._files = {
            row.relative_path: row.data for row in files_result.scalars().all()
        }

    def load_config(self) -> ParsedTemplateConfig:
        """Parse and cache the synced ``template.toml``."""
        if self._template_toml is None:
            self._raise_not_preloaded()
        if self._config is None:
            self._config = parse_template_toml(self._template_toml)
        return self._config

    def read_template(self, name: str) -> str:
        """Read a Jinja template by name from the synced tree."""
        return self._read_bytes(name).decode("utf-8")

    def read_asset(self, path: str) -> bytes:
        """Read an asset by relative path from the synced tree."""
        return self._read_bytes(path)

    def _read_bytes(self, relative: str) -> bytes:
        if self._template_toml is None:
            self._raise_not_preloaded()
        try:
            return self._files[relative]
        except KeyError as exc:
            msg = (
                f"File {relative!r} not found in dashboard GitHub template "
                f"{self._template_id}"
            )
            raise FileNotFoundError(msg) from exc

    def _raise_not_preloaded(self) -> NoReturn:
        msg = (
            "GitHubTemplateSource has not been preloaded; call "
            "`await source.preload()` before reading."
        )
        raise RuntimeError(msg)
