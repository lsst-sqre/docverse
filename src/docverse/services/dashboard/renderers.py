"""Stateless renderers that turn a :class:`DashboardContext` into bytes.

Each renderer is independent so it can be unit-tested with a fixture
context. The pipeline ships four renderers — the dashboard HTML, the
pydata-sphinx-theme switcher JSON, the 404 error page, and the
per-edition metadata JSON.
"""

from __future__ import annotations

import json
from importlib.resources import as_file, files
from pathlib import Path

import jinja2

from docverse.client.models import EditionKind
from docverse.domain.dashboard_context import (
    MAIN_SLUG,
    DashboardContext,
    EditionContext,
    EditionsContext,
    version_sort_key,
)
from docverse.storage.dashboard_templates.template_source import (
    SwitcherConfig,
    TemplateSource,
)

__all__ = [
    "DashboardHtmlRenderer",
    "EditionJsonRenderer",
    "ErrorPageRenderer",
    "SwitcherJsonRenderer",
]

_DEFAULT_404_PACKAGE = "docverse.storage.dashboard_templates.builtin"
_DEFAULT_404_TEMPLATE = "default_404.html.jinja"

_PREFERRED_KINDS = frozenset({"main", "alternate"})


class DashboardHtmlRenderer:
    """Render the dashboard HTML page using the source's Jinja template."""

    def __init__(self, *, template_source: TemplateSource) -> None:
        self._template_source = template_source

    def render(self, context: DashboardContext) -> bytes:
        """Render and return UTF-8 bytes for the dashboard."""
        config = self._template_source.load_config()
        template_name = config.dashboard.template
        env = jinja2.Environment(
            loader=jinja2.FunctionLoader(self._template_source.read_template),
            autoescape=jinja2.select_autoescape(["html", "jinja"]),
            undefined=jinja2.StrictUndefined,
        )
        template = env.get_template(template_name)
        rendered = template.render(
            org=context.org,
            project=context.project,
            editions=context.editions,
            assets=context.assets,
            docverse=context.docverse,
            rendered_at=context.rendered_at,
        )
        return rendered.encode("utf-8")


class ErrorPageRenderer:
    """Render the project's ``__404.html`` page.

    When the template source's ``template.toml`` declares ``[error_404]``,
    the configured Jinja template is loaded through the source. Otherwise
    the renderer falls back to a packaged default 404 shipped with the
    server distribution so every project has a usable error page even
    without a custom template.
    """

    def __init__(self, *, template_source: TemplateSource) -> None:
        self._template_source = template_source

    def render(self, context: DashboardContext) -> bytes:
        """Render and return UTF-8 bytes for the 404 page."""
        config = self._template_source.load_config()
        env = jinja2.Environment(
            autoescape=jinja2.select_autoescape(["html", "jinja"]),
            undefined=jinja2.StrictUndefined,
        )
        if config.error_404 is not None:
            env.loader = jinja2.FunctionLoader(
                self._template_source.read_template
            )
            template = env.get_template(config.error_404.template)
        else:
            template = env.from_string(_load_default_404_template())
        rendered = template.render(
            org=context.org,
            project=context.project,
            editions=context.editions,
            assets=context.assets,
            docverse=context.docverse,
            rendered_at=context.rendered_at,
        )
        return rendered.encode("utf-8")


def _load_default_404_template() -> str:
    """Read the packaged fallback 404 Jinja template."""
    resource = files(_DEFAULT_404_PACKAGE).joinpath(_DEFAULT_404_TEMPLATE)
    with as_file(resource) as path:
        return Path(path).read_text(encoding="utf-8")


class SwitcherJsonRenderer:
    """Render the pydata-sphinx-theme version-switcher JSON.

    The switcher schema is a JSON array of objects:

    .. code-block:: json

       [
         {
           "name": "Latest (main)",
           "version": "main",
           "url": "https://example.com/v/main/",
           "preferred": true
         }
       ]

    Editions are filtered by ``[switcher].include_kinds`` (default
    ``["main", "release", "major", "alternate"]``). Ordering is:

    1. ``__main`` first when present.
    2. Alternates next, alphabetically by title.
    3. Remaining editions by version descending (with non-version slugs
       sorting last).

    ``preferred`` is true for the ``__main`` edition and for any
    ``alternate`` edition.
    """

    def render(
        self,
        context: DashboardContext,
        *,
        switcher_config: SwitcherConfig | None = None,
    ) -> bytes:
        """Render the switcher JSON and return UTF-8 bytes."""
        config = switcher_config or SwitcherConfig()
        entries = _switcher_entries(context.editions, config.include_kinds)
        return json.dumps(entries, indent=2).encode("utf-8")


def _switcher_entries(
    editions: EditionsContext,
    include_kinds: tuple[str, ...],
) -> list[dict[str, object]]:
    include_set = frozenset(include_kinds)

    main_entry: list[dict[str, object]] = []
    if editions.main is not None and "main" in include_set:
        main_entry.append(_entry(editions.main))

    alt_entries: list[dict[str, object]] = []
    if EditionKind.alternate.value in include_set:
        alt_entries = [_entry(e) for e in editions.alternates]

    rest: list[EditionContext] = []
    for kind, items in (
        (EditionKind.release, editions.releases),
        (EditionKind.major, editions.major),
        (EditionKind.minor, editions.minor),
        (EditionKind.draft, editions.drafts),
    ):
        if kind.value in include_set:
            rest.extend(items)

    # The grouped lists are already kind-sorted; preserve that for ties
    # but enforce the documented "version descending across the rest"
    # rule for non-main / non-alternate slugs.
    rest.sort(key=version_sort_key, reverse=True)

    return [*main_entry, *alt_entries, *[_entry(e) for e in rest]]


def _entry(edition: EditionContext) -> dict[str, object]:
    entry: dict[str, object] = {
        "name": edition.title,
        "version": edition.slug,
        "url": edition.published_url,
    }
    if edition.kind.value in _PREFERRED_KINDS:
        entry["preferred"] = True
    return entry


class EditionJsonRenderer:
    """Render one edition's ``__editions/{slug}.json`` metadata file.

    The rendered payload tells the client-side theme JS whether the
    reader is viewing the canonical edition and, when they are not,
    where the canonical edition lives. ``canonical_url`` always points
    at the ``__main`` edition's ``published_url``; ``is_canonical`` is
    true only for the ``__main`` edition. The payload therefore never
    depends on per-edition bookkeeping — every per-edition file is
    regenerated on every render, so a change to ``__main`` propagates
    to all editions on the next render.
    """

    def render(
        self,
        edition: EditionContext,
        context: DashboardContext,
    ) -> bytes:
        """Render one edition's metadata JSON and return UTF-8 bytes."""
        canonical_url = (
            context.editions.main.published_url
            if context.editions.main is not None
            else context.project.published_url
        )
        payload: dict[str, object] = {
            "project_slug": context.project.slug,
            "edition_slug": edition.slug,
            "edition_title": edition.title,
            "kind": edition.kind.value,
            "is_canonical": edition.slug == MAIN_SLUG,
            "canonical_url": canonical_url,
            "published_url": edition.published_url,
        }
        return json.dumps(payload, indent=2).encode("utf-8")
