"""Render and upload one project's dashboard artifacts."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, replace
from datetime import datetime

import structlog
from rubin.repertoire import DiscoveryClient

from docverse_server.domain.dashboard_context import (
    DashboardContext,
    EditionContext,
)
from docverse_server.services.dashboard_templates.resolver import (
    ResolvedTemplate,
    TemplateResolver,
)
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.objectstore import ObjectStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.taskgroups import first_real_error

from .asset_inliner import AssetInliner
from .context import DashboardContextBuilder
from .renderers import (
    DashboardHtmlRenderer,
    EditionJsonRenderer,
    ErrorPageRenderer,
    SwitcherJsonRenderer,
)

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


def _iter_editions(context: DashboardContext) -> Iterator[EditionContext]:
    """Yield every edition present in the context exactly once."""
    if context.editions.main is not None:
        yield context.editions.main
    yield from context.editions.releases
    yield from context.editions.drafts
    yield from context.editions.major
    yield from context.editions.minor
    yield from context.editions.alternates


class DashboardPublisher:
    """Orchestrate one project's dashboard render + upload.

    The publisher composes the context builder, the template source,
    and the four renderers, then writes the rendered artifacts to the
    project's object store: the dashboard HTML, the switcher JSON, the
    404 error page, and one per-edition metadata JSON per non-deleted
    edition.
    """

    def __init__(
        self,
        *,
        org_store: OrganizationStore,
        project_store: ProjectStore,
        edition_store: EditionStore,
        build_store: BuildStore,
        discovery: DiscoveryClient,
        logger: structlog.stdlib.BoundLogger,
        template_resolver: TemplateResolver,
    ) -> None:
        self._org_store = org_store
        self._project_store = project_store
        self._edition_store = edition_store
        self._build_store = build_store
        self._discovery = discovery
        self._logger = logger
        self._template_resolver = template_resolver

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
            discovery=self._discovery,
            logger=self._logger,
        )
        return await builder.build(
            org_id=org_id,
            project_id=project_id,
            rendered_at=rendered_at,
        )

    async def resolve_template(
        self, *, org_id: int, project_id: int
    ) -> ResolvedTemplate:
        """Resolve the effective template for one project render.

        Callers that want to split the resolve (DB-bound) and
        render-and-upload (pure CPU + object store) steps across separate
        transaction scopes can call this first and then pass the result
        into :meth:`render_and_upload`.
        """
        return await self._template_resolver.resolve(
            org_id=org_id, project_id=project_id
        )

    async def render_and_upload(
        self,
        *,
        context: DashboardContext,
        object_store: ObjectStore,
        resolved: ResolvedTemplate,
        upload_concurrency: int,
    ) -> DashboardUploadProgress:
        """Render the artifacts and upload them to the object store.

        Accepts a pre-resolved :class:`ResolvedTemplate` so callers can
        close their resolve-time DB transaction before the upload loop
        opens the object store. ``resolved.source`` is expected to be
        fully preloaded (GitHub-backed sources are preloaded by
        :class:`TemplateResolver`), so no DB reads run here.

        Every artifact is rendered before the first upload. The three
        project-level artifacts — the dashboard HTML, the 404 page and
        the switcher JSON — then upload one at a time, in that order, so
        they have all landed before any edition file does. The
        per-edition JSONs follow concurrently, at most
        ``upload_concurrency`` in flight (see :meth:`_upload_editions`).

        Parameters
        ----------
        context
            The rendered project's context.
        object_store
            Open store to upload into.
        resolved
            The project's pre-resolved template.
        upload_concurrency
            Most edition uploads in flight at once. The worker passes
            ``keeper_sync_upload_concurrency``, the size of the
            process-wide upload limiter its store holds per PUT.

        Raises
        ------
        Exception
            The first upload failure, as the store raised it; the edition
            uploads still in flight are cancelled first.
        """
        template_source = resolved.source
        logger = self._logger.bind(template_origin=resolved.origin.value)

        config = template_source.load_config()

        inliner = AssetInliner(template_source=template_source)
        dashboard_assets = inliner.inline(
            css=config.dashboard.css,
            js=config.dashboard.js,
            images=config.dashboard.images,
        )
        dashboard_context = replace(context, assets=dashboard_assets)

        html_renderer = DashboardHtmlRenderer(template_source=template_source)
        switcher_renderer = SwitcherJsonRenderer(
            template_source=template_source
        )
        error_renderer = ErrorPageRenderer(template_source=template_source)
        edition_renderer = EditionJsonRenderer()

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
        project_artifacts: list[tuple[str, bytes, str]] = [
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

        # One per-edition JSON per non-deleted edition. Every edition is
        # re-rendered on every publish so a change to __main's
        # canonical_url propagates without per-edition bookkeeping.
        edition_artifacts: list[tuple[str, bytes, str]] = [
            (
                f"{project_slug}/__editions/{edition.slug}.json",
                edition_renderer.render(edition, context),
                "application/json; charset=utf-8",
            )
            for edition in _iter_editions(context)
        ]

        # The project-level artifacts land first, in order, so the
        # landing page, 404 and switcher are in place before any edition
        # file they describe.
        for key, data, content_type in project_artifacts:
            await object_store.upload_object(
                key=key, data=data, content_type=content_type
            )
        await self._upload_editions(
            edition_artifacts,
            object_store=object_store,
            upload_concurrency=upload_concurrency,
        )

        artifacts = project_artifacts + edition_artifacts
        total = sum(len(data) for _, data, _ in artifacts)
        logger.info(
            "Uploaded dashboard artifacts",
            project=project_slug,
            object_count=len(artifacts),
            total_size_bytes=total,
        )
        return DashboardUploadProgress(
            object_count=len(artifacts), total_size_bytes=total
        )

    @staticmethod
    async def _upload_editions(
        artifacts: list[tuple[str, bytes, str]],
        *,
        object_store: ObjectStore,
        upload_concurrency: int,
    ) -> None:
        """Upload the per-edition JSONs, one upload window at a time.

        A slot of the window is taken *before* each upload's task is
        created and given back once the upload has landed, so no more
        than ``upload_concurrency`` edition PUTs are in flight at once,
        however many editions the project has. One PUT at a time was
        ~1.5-2 s each from prod, so a project of a thousand editions
        outran arq's job timeout on every rebuild (#863).

        The window mirrors ``build_processing``'s: in the worker it is
        the size of the process-wide upload limiter the store holds
        around each PUT, which caps PUTs across every job in the
        process; the window keeps this one job from queueing its whole
        edition set on that limiter ahead of every other upload.

        The uploads run in an :class:`asyncio.TaskGroup`, so the first
        one to fail stops the loop and cancels the others still in
        flight, and its error is raised as itself rather than as an
        exception group
        (:func:`~docverse_server.taskgroups.first_real_error`).
        """
        window = asyncio.Semaphore(upload_concurrency)

        async def _upload(key: str, data: bytes, content_type: str) -> None:
            try:
                await object_store.upload_object(
                    key=key, data=data, content_type=content_type
                )
            finally:
                window.release()

        try:
            async with asyncio.TaskGroup() as uploads:
                for key, data, content_type in artifacts:
                    await window.acquire()
                    uploads.create_task(_upload(key, data, content_type))
        except BaseExceptionGroup as group:
            leaf = first_real_error(group)
            # ``from`` the leaf's own cause: hides the group, keeps any
            # chain the store built (see ``first_real_error``).
            raise leaf from leaf.__cause__

    async def publish(
        self,
        *,
        org_id: int,
        project_id: int,
        object_store_provider: ObjectStoreProvider,
        upload_concurrency: int,
        rendered_at: datetime | None = None,
    ) -> tuple[DashboardContext, DashboardUploadProgress]:
        """Build context, render, and upload in one call.

        Returns the rendered context (for callers that want to record
        ``rendered_at`` or counts in a queue job) along with the upload
        progress. ``upload_concurrency`` bounds the edition uploads, as
        for :meth:`render_and_upload`.

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
        resolved = await self.resolve_template(
            org_id=org_id, project_id=project_id
        )
        object_store = await object_store_provider()
        async with object_store:
            progress = await self.render_and_upload(
                context=context,
                object_store=object_store,
                resolved=resolved,
                upload_concurrency=upload_concurrency,
            )
        return context, progress
