"""Resolve a project's effective dashboard template at render time."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

import structlog

from docverse.storage.dashboard_templates.builtin import BuiltInTemplateSource
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
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
    methods do not raise. ``preload`` loads the template's
    ``template.toml`` bytes plus every template-file row into an
    in-memory cache keyed by relative path, so synchronous
    ``read_template`` / ``read_asset`` calls never raise.
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

    The FK
    ``dashboard_github_template_bindings.github_template_id ->
    dashboard_github_templates.id`` uses ``ON DELETE SET NULL``
    (see ``src/docverse/dbschema/dashboard_github_template_binding.py``),
    so within a single transaction a binding with a non-null
    ``github_template_id`` always points at an existing template row.
    Across transaction boundaries, a concurrent template delete
    transitions the binding's ``github_template_id`` to ``NULL`` rather
    than leaving a dangling reference, so the next resolve call simply
    falls through to the null-handling branch instead of raising on a
    missing row.
    """

    def __init__(
        self,
        *,
        binding_store: DashboardGitHubTemplateBindingStore,
        template_store: DashboardGitHubTemplateStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._binding_store = binding_store
        self._template_store = template_store
        self._logger = logger

    async def resolve(
        self, *, org_id: int, project_id: int
    ) -> ResolvedTemplate:
        """Return the template source for rendering the given project."""
        override = await self._binding_store.get_project_override(
            org_id=org_id, project_id=project_id
        )
        if override is not None and override.github_template_id is not None:
            source = await self._template_store.load_preloaded_source(
                override.github_template_id
            )
            return ResolvedTemplate(
                source=source,
                origin=ResolvedTemplateOrigin.project_override,
            )

        default = await self._binding_store.get_org_default(org_id)
        if default is not None and default.github_template_id is not None:
            source = await self._template_store.load_preloaded_source(
                default.github_template_id
            )
            return ResolvedTemplate(
                source=source,
                origin=ResolvedTemplateOrigin.org_default,
            )

        return ResolvedTemplate(
            source=BuiltInTemplateSource(),
            origin=ResolvedTemplateOrigin.builtin,
        )
