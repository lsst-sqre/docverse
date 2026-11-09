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

from docverse.models import EditionKind
from docverse_server.domain.dashboard_context import (
    MAIN_SLUG,
    DashboardContext,
    EditionContext,
    EditionsContext,
    version_sort_key,
)
from docverse_server.exceptions import DocverseSlackException
from docverse_server.storage.dashboard_templates.template_source import (
    SwitcherConfig,
    TemplateSource,
)

__all__ = [
    "SWITCHER_BOOLEAN_KEYS",
    "SWITCHER_STRING_KEYS",
    "DashboardHtmlRenderer",
    "EditionJsonRenderer",
    "ErrorPageRenderer",
    "InvalidSwitcherJsonError",
    "SwitcherJsonRenderer",
]

_DEFAULT_404_PACKAGE = "docverse_server.storage.dashboard_templates.builtin"
_DEFAULT_404_TEMPLATE = "default_404.html.jinja"

_PREFERRED_KINDS = frozenset({"main", "alternate"})

SWITCHER_STRING_KEYS: tuple[str, ...] = ("name", "version", "url")
"""Keys every switcher entry must carry, each with a string value."""

SWITCHER_BOOLEAN_KEYS: tuple[str, ...] = ("preferred",)
"""Keys a switcher entry may carry, each with a boolean value."""

_SWITCHER_KEYS = frozenset((*SWITCHER_STRING_KEYS, *SWITCHER_BOOLEAN_KEYS))


class InvalidSwitcherJsonError(DocverseSlackException):
    """A template set's switcher template rendered an invalid switcher.

    Raised by :class:`SwitcherJsonRenderer` when the Jinja file named by
    ``[switcher].template`` renders anything other than a JSON array of
    pydata-sphinx-theme switcher entries: objects with string ``name``,
    ``version`` and ``url`` and an optional boolean ``preferred``, and no
    other keys. ``dashboard_build`` fails its job on it the way it does
    on a broken dashboard template, so the project keeps serving its
    previous ``__switcher.json`` rather than a file the theme cannot
    read.

    No ``to_sentry`` override: the rendered message names the template,
    the project and org, and what is wrong with which entry, which is
    the whole triage story. Mirrors
    :class:`~docverse_server.storage.objectstore.ObjectStoreError`.

    Parameters
    ----------
    template
        Name of the switcher template in the template set.
    org_slug
        Slug of the organization whose dashboard was rendering.
    project_slug
        Slug of the project whose dashboard was rendering.
    reason
        What is wrong with the output, e.g. ``"entry 2 has no 'url'"``.
    message
        Rendered message, overriding the default built from the fields
        above.
    """

    def __init__(
        self,
        *,
        template: str | None = None,
        org_slug: str | None = None,
        project_slug: str | None = None,
        reason: str | None = None,
        message: str | None = None,
    ) -> None:
        self.template = template
        self.org_slug = org_slug
        self.project_slug = project_slug
        self.reason = reason
        super().__init__(
            message
            if message is not None
            else self._format_message(
                template=template,
                org_slug=org_slug,
                project_slug=project_slug,
                reason=reason,
            )
        )

    @staticmethod
    def _format_message(
        *,
        template: str | None,
        org_slug: str | None,
        project_slug: str | None,
        reason: str | None,
    ) -> str:
        named = f" {template!r}" if template else ""
        project = f" for project {project_slug!r}" if project_slug else ""
        org = f" in org {org_slug!r}" if org_slug else ""
        because = f": {reason}" if reason else ""
        return (
            f"Switcher template{named}{project}{org} rendered invalid "
            f"switcher JSON{because}"
        )


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
        rendered = template.render(_template_variables(context))
        return rendered.encode("utf-8")


def _template_variables(context: DashboardContext) -> dict[str, object]:
    """Return the variables every template-set Jinja file renders with."""
    return {
        "org": context.org,
        "project": context.project,
        "editions": context.editions,
        "assets": context.assets,
        "docverse": context.docverse,
        "rendered_at": context.rendered_at,
    }


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
        rendered = template.render(_template_variables(context))
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

    When ``[switcher].template`` names a Jinja file, the template set
    renders the array itself instead and ``include_kinds`` is ignored:
    the file is rendered with the same variables as the dashboard page,
    the output must be a JSON array of objects with string ``name``,
    ``version`` and ``url``, an optional boolean ``preferred`` and no
    other keys, and it is re-dumped in the built-in output's format.
    Anything else raises :class:`InvalidSwitcherJsonError`.
    """

    def __init__(
        self, *, template_source: TemplateSource | None = None
    ) -> None:
        self._template_source = template_source

    def render(
        self,
        context: DashboardContext,
        *,
        switcher_config: SwitcherConfig | None = None,
    ) -> bytes:
        """Render the switcher JSON and return UTF-8 bytes."""
        config = switcher_config or SwitcherConfig()
        if config.template is not None:
            entries: object = self._render_template(context, config.template)
        else:
            entries = _switcher_entries(context.editions, config.include_kinds)
        return json.dumps(entries, indent=2).encode("utf-8")

    def _render_template(
        self, context: DashboardContext, template_name: str
    ) -> list[object]:
        """Render the template set's switcher template and validate it.

        Raises
        ------
        InvalidSwitcherJsonError
            If the output is not a JSON array of switcher entries.
        """
        if self._template_source is None:
            msg = (
                f"[switcher] template {template_name!r} is configured but "
                "SwitcherJsonRenderer has no template source to load it from"
            )
            raise RuntimeError(msg)
        # Built like the dashboard page's environment, except that HTML
        # autoescaping stays off: the output is JSON, never HTML, and
        # escaping would turn a URL's ``&`` into ``&amp;``. Templates emit
        # their strings with the ``tojson`` filter instead, and the
        # re-dump below serializes them exactly as the built-in path does.
        env = jinja2.Environment(
            loader=jinja2.FunctionLoader(self._template_source.read_template),
            autoescape=False,  # noqa: S701
            undefined=jinja2.StrictUndefined,
        )
        template = env.get_template(template_name)
        rendered = template.render(_template_variables(context))

        def invalid(reason: str) -> InvalidSwitcherJsonError:
            return InvalidSwitcherJsonError(
                template=template_name,
                org_slug=context.org.slug,
                project_slug=context.project.slug,
                reason=reason,
            )

        try:
            entries = json.loads(rendered)
        except json.JSONDecodeError as exc:
            raise invalid(f"output is not valid JSON ({exc})") from exc
        if not isinstance(entries, list):
            reason = f"top-level value is {_json_type(entries)}, not an array"
            raise invalid(reason)
        for index, entry in enumerate(entries):
            violation = _switcher_entry_violation(entry)
            if violation is not None:
                raise invalid(f"entry {index} {violation}")
        return entries


def _switcher_entry_violation(entry: object) -> str | None:
    """Say what keeps ``entry`` from being a switcher entry, if anything."""
    if not isinstance(entry, dict):
        return f"is {_json_type(entry)}, not an object"
    for key in SWITCHER_STRING_KEYS:
        if key not in entry:
            return f"has no {key!r}"
        if not isinstance(entry[key], str):
            return f"{key!r} is {_json_type(entry[key])}, not a string"
    for key in SWITCHER_BOOLEAN_KEYS:
        if key in entry and not isinstance(entry[key], bool):
            return f"{key!r} is {_json_type(entry[key])}, not a boolean"
    unexpected = sorted(set(entry) - _SWITCHER_KEYS)
    if unexpected:
        noun = "key" if len(unexpected) == 1 else "keys"
        return f"has unexpected {noun} {', '.join(map(repr, unexpected))}"
    return None


def _json_type(value: object) -> str:
    """Name the JSON type of a parsed JSON value, with its article."""
    # ``bool`` first: it is a subclass of ``int``.
    if isinstance(value, bool):
        return "a boolean"
    if isinstance(value, int | float):
        return "a number"
    if isinstance(value, str):
        return "a string"
    if isinstance(value, list):
        return "an array"
    if isinstance(value, dict):
        return "an object"
    return "null"


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
