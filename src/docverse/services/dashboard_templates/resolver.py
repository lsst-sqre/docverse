"""Resolve a project's effective dashboard template at render time."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.domain.project import Project
from docverse.storage.dashboard_templates.builtin import BuiltInTemplateSource
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
    GitHubTemplateSource,
)
from docverse.storage.dashboard_templates.template_source import TemplateSource

__all__ = [
    "ResolvedTemplate",
    "ResolvedTemplateOrigin",
    "TemplateResolver",
]


class ResolvedTemplateOrigin(StrEnum):
    """Which resolution layer produced a :class:`ResolvedTemplate`."""

    project_override = "project_override"
    org_default = "org_default"
    builtin = "builtin"


@dataclass(frozen=True)
class ResolvedTemplate:
    """The template source selected for a single project's render.

    ``source`` is a :class:`TemplateSource` ready for synchronous reads
    by the renderer pipeline. :class:`GitHubTemplateSource` instances
    returned here have already been ``preload``-ed so the sync read
    methods do not raise.
    """

    source: TemplateSource
    origin: ResolvedTemplateOrigin


class TemplateResolver:
    """Resolve a project's effective dashboard template.

    The resolution order, per PRD #232, is:

    1. Project override binding (if it has a synced template).
    2. Org default binding (if it has a synced template).
    3. :class:`BuiltInTemplateSource` as the universal last-resort
       fallback.

    A binding whose ``github_template_id`` is ``None`` is treated as
    "not found" for resolution purposes — initial-sync-pending bindings
    fall through to the next layer so dashboards keep rendering while
    the first sync is still in flight (or has failed outright).
    """

    def __init__(
        self,
        *,
        binding_store: DashboardGitHubTemplateBindingStore,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._binding_store = binding_store
        self._session = session
        self._logger = logger

    async def resolve_for_project(self, project: Project) -> ResolvedTemplate:
        """Return the template source for rendering ``project``."""
        override = await self._binding_store.get_project_override(
            org_id=project.org_id, project_id=project.id
        )
        if override is not None and override.github_template_id is not None:
            source = await self._load_github_source(
                override.github_template_id
            )
            return ResolvedTemplate(
                source=source,
                origin=ResolvedTemplateOrigin.project_override,
            )

        default = await self._binding_store.get_org_default(project.org_id)
        if default is not None and default.github_template_id is not None:
            source = await self._load_github_source(default.github_template_id)
            return ResolvedTemplate(
                source=source,
                origin=ResolvedTemplateOrigin.org_default,
            )

        return ResolvedTemplate(
            source=BuiltInTemplateSource(),
            origin=ResolvedTemplateOrigin.builtin,
        )

    async def _load_github_source(
        self, template_id: int
    ) -> GitHubTemplateSource:
        source = GitHubTemplateSource(
            template_id=template_id, session=self._session
        )
        await source.preload()
        return source
