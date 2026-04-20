"""Render and upload one project's dashboard artifacts."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime

import structlog

from docverse.config import Configuration
from docverse.domain.dashboard_context import DashboardContext
from docverse.exceptions import NotFoundError
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.objectstore import ObjectStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

from .dashboard_asset_inliner import AssetInliner
from .dashboard_context import DashboardContextBuilder
from .dashboard_renderers import (
    DashboardHtmlRenderer,
    ErrorPageRenderer,
    SwitcherJsonRenderer,
)
from .dashboard_template_source import BuiltInTemplateSource, TemplateSource

__all__ = [
    "DashboardPublisher",
    "DashboardUploadProgress",
    "ObjectStoreProvider",
]

ObjectStoreProvider = Callable[[], Awaitable[ObjectStore]]
"""Async factory for an unopened :class:`ObjectStore`."""


@dataclass(frozen=True)
class DashboardUploadProgress:
    """Counters for objects uploaded by one publish call."""

    object_count: int
    total_size_bytes: int


class DashboardPublisher:
    """Orchestrate one project's dashboard render + upload.

    The publisher composes the context builder, the template source,
    and the four renderers, then writes the rendered artifacts to the
    project's object store. The MVP slice publishes the dashboard HTML,
    the switcher JSON, and the 404 error page; the per-edition JSON
    files are deferred to a follow-up ticket.
    """

    def __init__(  # noqa: PLR0913
        self,
        *,
        org_store: OrganizationStore,
        project_store: ProjectStore,
        edition_store: EditionStore,
        build_store: BuildStore,
        config: Configuration,
        logger: structlog.stdlib.BoundLogger,
        template_source: TemplateSource | None = None,
    ) -> None:
        self._org_store = org_store
        self._project_store = project_store
        self._edition_store = edition_store
        self._build_store = build_store
        self._config = config
        self._logger = logger
        self._template_source = template_source or BuiltInTemplateSource()

    async def build_context(
        self,
        *,
        org_id: int,
        project_id: int,
        rendered_at: datetime | None = None,
    ) -> DashboardContext:
        """Build the :class:`DashboardContext` for a single render."""
        builder = DashboardContextBuilder(
            org_store=self._org_store,
            project_store=self._project_store,
            edition_store=self._edition_store,
            build_store=self._build_store,
            config=self._config,
            logger=self._logger,
        )
        return await builder.build(
            org_id=org_id,
            project_id=project_id,
            rendered_at=rendered_at,
        )

    async def render_and_upload(
        self,
        *,
        context: DashboardContext,
        object_store: ObjectStore,
    ) -> DashboardUploadProgress:
        """Render the artifacts and upload them to the object store."""
        config = self._template_source.load_config()

        inliner = AssetInliner(template_source=self._template_source)
        dashboard_assets = inliner.inline(
            css=config.dashboard.css,
            js=config.dashboard.js,
            images=config.dashboard.images,
        )
        dashboard_context = replace(context, assets=dashboard_assets)

        html_renderer = DashboardHtmlRenderer(
            template_source=self._template_source
        )
        switcher_renderer = SwitcherJsonRenderer()
        error_renderer = ErrorPageRenderer(
            template_source=self._template_source
        )

        html_bytes = html_renderer.render(dashboard_context)
        switcher_bytes = switcher_renderer.render(
            dashboard_context, switcher_config=config.switcher
        )

        # The 404 page consumes its own asset set when configured;
        # the packaged fallback template is self-contained.
        if config.error_404 is not None:
            error_assets = inliner.inline(
                css=config.error_404.css,
                js=config.error_404.js,
                images=config.error_404.images,
            )
            error_context = replace(context, assets=error_assets)
        else:
            error_context = dashboard_context
        error_bytes = error_renderer.render(error_context)

        project_slug = context.project.slug
        artifacts = [
            (
                f"{project_slug}/__dashboard.html",
                html_bytes,
                "text/html; charset=utf-8",
            ),
            (
                f"{project_slug}/__404.html",
                error_bytes,
                "text/html; charset=utf-8",
            ),
            (
                f"{project_slug}/__switcher.json",
                switcher_bytes,
                "application/json; charset=utf-8",
            ),
        ]

        total = 0
        for key, data, content_type in artifacts:
            await object_store.upload_object(
                key=key, data=data, content_type=content_type
            )
            total += len(data)

        self._logger.info(
            "Uploaded dashboard artifacts",
            project=project_slug,
            object_count=len(artifacts),
            total_size_bytes=total,
        )
        return DashboardUploadProgress(
            object_count=len(artifacts), total_size_bytes=total
        )

    async def publish(
        self,
        *,
        org_id: int,
        project_id: int,
        object_store_provider: ObjectStoreProvider,
        rendered_at: datetime | None = None,
    ) -> tuple[DashboardContext, DashboardUploadProgress]:
        """Build context, render, and upload in one call.

        Returns the rendered context (for callers that want to record
        ``rendered_at`` or counts in a queue job) along with the upload
        progress.

        Raises
        ------
        NotFoundError
            If the org or project cannot be loaded.
        """
        context = await self.build_context(
            org_id=org_id,
            project_id=project_id,
            rendered_at=rendered_at,
        )
        if context.project.slug == "":
            msg = f"Project {project_id} has no slug"
            raise NotFoundError(msg)
        object_store = await object_store_provider()
        async with object_store:
            progress = await self.render_and_upload(
                context=context, object_store=object_store
            )
        return context, progress
