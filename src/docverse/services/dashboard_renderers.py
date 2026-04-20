"""Stateless renderers that turn a :class:`DashboardContext` into bytes.

Each renderer is independent so it can be unit-tested with a fixture
context. The MVP slice ships two renderers — the dashboard HTML and the
pydata-sphinx-theme switcher JSON. The 404 and per-edition JSON
renderers are deferred to follow-up tickets per the parent PRD.
"""

from __future__ import annotations

import json

import jinja2

from docverse.client.models import EditionKind
from docverse.domain.dashboard_context import (
    DashboardContext,
    EditionContext,
    EditionsContext,
)

from .dashboard_template_source import SwitcherConfig, TemplateSource

__all__ = [
    "DashboardHtmlRenderer",
    "SwitcherJsonRenderer",
]

_DEFAULT_INCLUDE_KINDS: tuple[str, ...] = (
    "main",
    "release",
    "major",
    "alternate",
)
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
        include_kinds = (
            tuple(config.include_kinds)
            if config.include_kinds
            else _DEFAULT_INCLUDE_KINDS
        )
        entries = _switcher_entries(context.editions, include_kinds)
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
    rest.sort(key=_version_sort_key, reverse=True)

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


def _version_sort_key(edition: EditionContext) -> tuple[int, ...]:
    """Stable descending key: parse leading version triple from the slug.

    Slugs that don't start with a numeric version sort last by emitting
    a leading ``-1``.
    """
    parts: list[int] = []
    candidate = edition.slug.lstrip("v")
    for token in candidate.split("."):
        try:
            parts.append(int(token))
        except ValueError:
            break
    if not parts:
        return (-1,)
    return tuple(parts)
