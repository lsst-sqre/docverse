"""Tests for the dashboard HTML and switcher JSON renderers."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from docverse.models import EditionKind
from docverse_server.domain.dashboard_context import (
    AssetsContext,
    BuildContext,
    DashboardContext,
    DocverseContext,
    EditionContext,
    EditionsContext,
    OrgContext,
    ProjectContext,
)
from docverse_server.services.dashboard.renderers import (
    DashboardHtmlRenderer,
    EditionJsonRenderer,
    ErrorPageRenderer,
    InvalidSwitcherJsonError,
    SwitcherJsonRenderer,
)
from docverse_server.storage.dashboard_templates.builtin import (
    BuiltInTemplateSource,
)
from docverse_server.storage.dashboard_templates.template_source import (
    DashboardTemplateConfig,
    ParsedTemplateConfig,
    Switcher404Config,
    SwitcherConfig,
)
from tests.support.dashboard_templates import DEMO_SWITCHER_JINJA


class _FakeTemplateSource:
    """In-memory ``TemplateSource`` for renderer tests."""

    def __init__(
        self,
        *,
        config: ParsedTemplateConfig,
        templates: dict[str, str] | None = None,
    ) -> None:
        self._config = config
        self._templates = templates or {}

    def load_config(self) -> ParsedTemplateConfig:
        return self._config

    def read_template(self, name: str) -> str:
        return self._templates[name]

    def read_asset(self, path: str) -> bytes:  # pragma: no cover - unused
        raise NotImplementedError


def _edition(
    *,
    slug: str,
    title: str,
    kind: EditionKind,
    alternate_name: str | None = None,
    build: BuildContext | None = None,
    project_url: str = "https://proj.example.com/",
) -> EditionContext:
    if slug == "__main":
        published_url = project_url
    else:
        published_url = f"{project_url}v/{slug}/"
    return EditionContext(
        slug=slug,
        title=title,
        kind=kind,
        alternate_name=alternate_name,
        date_updated=datetime(2026, 4, 20, tzinfo=UTC),
        published_url=published_url,
        build=build,
    )


def _make_context(
    *,
    main: EditionContext | None = None,
    releases: list[EditionContext] | None = None,
    drafts: list[EditionContext] | None = None,
    major: list[EditionContext] | None = None,
    minor: list[EditionContext] | None = None,
    alternates: list[EditionContext] | None = None,
) -> DashboardContext:
    return DashboardContext(
        org=OrgContext(
            slug="proj", title="Proj Org", base_domain="example.com"
        ),
        project=ProjectContext(
            slug="proj",
            title="A Project",
            source_repo_url="https://example.com/example/proj",
            published_url="https://proj.example.com/",
        ),
        editions=EditionsContext(
            main=main,
            releases=releases or [],
            drafts=drafts or [],
            major=major or [],
            minor=minor or [],
            alternates=alternates or [],
        ),
        assets=AssetsContext(),
        docverse=DocverseContext(
            api_url="https://api.example.com/",
            version="0.0.0",
        ),
        rendered_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
    )


def test_switcher_default_kinds_includes_main_release_major_alternate() -> (
    None
):
    main = _edition(slug="__main", title="Latest", kind=EditionKind.main)
    release = _edition(slug="v1.0.0", title="v1.0.0", kind=EditionKind.release)
    major = _edition(slug="v2", title="v2.x", kind=EditionKind.major)
    minor = _edition(slug="v2.5", title="v2.5.x", kind=EditionKind.minor)
    draft = _edition(slug="latest", title="Latest", kind=EditionKind.draft)
    alt = _edition(
        slug="dev-east",
        title="East",
        kind=EditionKind.alternate,
        alternate_name="east",
    )
    ctx = _make_context(
        main=main,
        releases=[release],
        major=[major],
        minor=[minor],
        drafts=[draft],
        alternates=[alt],
    )

    payload = json.loads(SwitcherJsonRenderer().render(ctx).decode("utf-8"))
    versions = [entry["version"] for entry in payload]
    # __main first, alternate next, then release + major (minor and
    # draft are excluded by default).
    assert versions == ["__main", "dev-east", "v2", "v1.0.0"]


def test_switcher_main_and_alternates_are_preferred() -> None:
    main = _edition(slug="__main", title="Latest", kind=EditionKind.main)
    alt = _edition(slug="dev-east", title="East", kind=EditionKind.alternate)
    release = _edition(slug="v1.0.0", title="v1.0.0", kind=EditionKind.release)
    ctx = _make_context(main=main, releases=[release], alternates=[alt])

    payload = json.loads(SwitcherJsonRenderer().render(ctx).decode("utf-8"))
    by_version = {e["version"]: e for e in payload}
    assert by_version["__main"]["preferred"] is True
    assert by_version["dev-east"]["preferred"] is True
    assert "preferred" not in by_version["v1.0.0"]


def test_switcher_respects_custom_include_kinds() -> None:
    main = _edition(slug="__main", title="Latest", kind=EditionKind.main)
    draft = _edition(
        slug="latest", title="Latest draft", kind=EditionKind.draft
    )
    release = _edition(slug="v1.0.0", title="v1.0.0", kind=EditionKind.release)
    ctx = _make_context(main=main, drafts=[draft], releases=[release])

    payload = json.loads(
        SwitcherJsonRenderer()
        .render(
            ctx,
            switcher_config=SwitcherConfig(include_kinds=("draft",)),
        )
        .decode("utf-8")
    )
    assert [e["version"] for e in payload] == ["latest"]


def test_switcher_versions_sorted_descending_for_rest() -> None:
    main = _edition(slug="__main", title="Latest", kind=EditionKind.main)
    releases = [
        _edition(slug=s, title=s, kind=EditionKind.release)
        for s in ("v1.0.0", "v10.0.0", "v2.5.10", "v2.5.3")
    ]
    ctx = _make_context(main=main, releases=releases)

    payload = json.loads(SwitcherJsonRenderer().render(ctx).decode("utf-8"))
    versions = [e["version"] for e in payload]
    assert versions == ["__main", "v10.0.0", "v2.5.10", "v2.5.3", "v1.0.0"]


def test_switcher_orders_eups_release_tags_by_full_version() -> None:
    """EUPS release tags key by their full version, not the leading run.

    The leading dotted run reads ``v30_0_12``, ``v30_0_11`` and
    ``v30_0_12_rc3`` all as ``30``, so given the releases out of order
    the switcher must still list a final above its candidates,
    ``v17_0`` below ``v17_0_1`` and the legacy ``13-0`` last, rather
    than lean on the grouped list's pre-sort.
    """
    main = _edition(slug="__main", title="Latest", kind=EditionKind.main)
    releases = [
        _edition(slug=s, title=s, kind=EditionKind.release)
        for s in (
            "v17_0",
            "v30_0_12_rc3",
            "13-0",
            "v30_0_11",
            "v17_0_1",
            "v30_0_12",
        )
    ]
    ctx = _make_context(main=main, releases=releases)

    payload = json.loads(SwitcherJsonRenderer().render(ctx).decode("utf-8"))
    versions = [e["version"] for e in payload]
    assert versions == [
        "__main",
        "v30_0_12",
        "v30_0_12_rc3",
        "v30_0_11",
        "v17_0_1",
        "v17_0",
        "13-0",
    ]


def test_switcher_keeps_semver_prerelease_with_its_final() -> None:
    """A semver prerelease keeps its final's key in the switcher.

    ``v1.2.3-rc1`` must not be read as an EUPS candidate, so it ties
    with ``v1.2.3`` and keeps the grouped list's order.
    """
    releases = [
        _edition(slug=s, title=s, kind=EditionKind.release)
        for s in ("v1.2.3-rc1", "v1.2.3", "v1.10.0")
    ]
    ctx = _make_context(releases=releases)

    payload = json.loads(SwitcherJsonRenderer().render(ctx).decode("utf-8"))
    versions = [e["version"] for e in payload]
    assert versions == ["v1.10.0", "v1.2.3-rc1", "v1.2.3"]


def test_switcher_empty_project_yields_empty_array() -> None:
    ctx = _make_context()
    payload = json.loads(SwitcherJsonRenderer().render(ctx).decode("utf-8"))
    assert payload == []


def test_dashboard_html_contains_section_markers_for_present_groups() -> None:
    main = _edition(slug="__main", title="Latest", kind=EditionKind.main)
    release = _edition(slug="v1.0.0", title="v1.0.0", kind=EditionKind.release)
    ctx = _make_context(main=main, releases=[release])

    html = (
        DashboardHtmlRenderer(template_source=BuiltInTemplateSource())
        .render(ctx)
        .decode("utf-8")
    )

    assert 'id="main-edition"' in html
    assert 'id="releases"' in html
    assert "v1.0.0" in html
    assert 'id="drafts"' not in html
    assert 'id="alternates"' not in html


def test_dashboard_html_for_empty_project_omits_all_sections() -> None:
    ctx = _make_context()
    html = (
        DashboardHtmlRenderer(template_source=BuiltInTemplateSource())
        .render(ctx)
        .decode("utf-8")
    )

    for marker in (
        'id="main-edition"',
        'id="releases"',
        'id="drafts"',
        'id="major-versions"',
        'id="minor-versions"',
        'id="alternates"',
    ):
        assert marker not in html


def test_dashboard_html_includes_rendered_at_meta_tag() -> None:
    ctx = _make_context()
    html = (
        DashboardHtmlRenderer(template_source=BuiltInTemplateSource())
        .render(ctx)
        .decode("utf-8")
    )
    assert "2026-04-20T12:00:00+00:00" in html


def test_error_page_falls_back_to_packaged_default_when_unconfigured() -> None:
    """No ``[error_404]`` in template.toml → packaged default fires."""
    ctx = _make_context()
    renderer = ErrorPageRenderer(template_source=BuiltInTemplateSource())

    html = renderer.render(ctx).decode("utf-8")

    # Branded with project title and links back to the dashboard.
    assert "<!DOCTYPE html>" in html
    assert "A Project" in html
    assert "https://proj.example.com/v/" in html
    assert "404" in html


def test_error_page_uses_configured_template_when_present() -> None:
    """``[error_404]`` declared → renderer loads it from the source."""
    config = ParsedTemplateConfig(
        dashboard=DashboardTemplateConfig(),
        error_404=Switcher404Config(template="custom_404.html.jinja"),
        switcher=SwitcherConfig(),
    )
    template_source = _FakeTemplateSource(
        config=config,
        templates={
            "custom_404.html.jinja": (
                "<!doctype html><title>Custom 404</title>"
                "<p>Project: {{ project.title }}</p>"
                "<p>Org: {{ org.title }}</p>"
            ),
        },
    )
    ctx = _make_context()
    renderer = ErrorPageRenderer(template_source=template_source)

    html = renderer.render(ctx).decode("utf-8")

    assert "Custom 404" in html
    assert "Project: A Project" in html
    assert "Org: Proj Org" in html


def test_error_page_default_renders_for_empty_project() -> None:
    """Empty-editions context still produces a 404 page."""
    ctx = _make_context()
    renderer = ErrorPageRenderer(template_source=BuiltInTemplateSource())

    html = renderer.render(ctx).decode("utf-8")

    assert "404" in html
    assert "A Project" in html


@pytest.mark.parametrize(
    ("include_kinds", "expected"),
    [
        (("main",), ["__main"]),
        (("alternate",), ["dev-east"]),
        ((), []),
    ],
)
def test_switcher_include_kinds_filters(
    include_kinds: tuple[str, ...],
    expected: list[str],
) -> None:
    main = _edition(slug="__main", title="Latest", kind=EditionKind.main)
    alt = _edition(slug="dev-east", title="East", kind=EditionKind.alternate)
    release = _edition(slug="v1.0.0", title="v1.0.0", kind=EditionKind.release)
    ctx = _make_context(main=main, releases=[release], alternates=[alt])

    payload = json.loads(
        SwitcherJsonRenderer()
        .render(
            ctx, switcher_config=SwitcherConfig(include_kinds=include_kinds)
        )
        .decode("utf-8")
    )
    assert [e["version"] for e in payload] == expected


def test_edition_json_main_is_canonical() -> None:
    main = _edition(slug="__main", title="Latest", kind=EditionKind.main)
    release = _edition(slug="v1.0.0", title="v1.0.0", kind=EditionKind.release)
    ctx = _make_context(main=main, releases=[release])

    payload = json.loads(
        EditionJsonRenderer().render(main, ctx).decode("utf-8")
    )

    assert payload["is_canonical"] is True
    assert payload["canonical_url"] == main.published_url
    assert payload["edition_slug"] == "__main"
    assert payload["published_url"] == main.published_url


def test_edition_json_non_main_points_canonical_at_main() -> None:
    main = _edition(slug="__main", title="Latest", kind=EditionKind.main)
    release = _edition(slug="v1.0.0", title="v1.0.0", kind=EditionKind.release)
    ctx = _make_context(main=main, releases=[release])

    payload = json.loads(
        EditionJsonRenderer().render(release, ctx).decode("utf-8")
    )

    assert payload["is_canonical"] is False
    assert payload["canonical_url"] == main.published_url
    assert payload["canonical_url"] != release.published_url
    assert payload["edition_slug"] == "v1.0.0"
    assert payload["published_url"] == release.published_url


def test_edition_json_includes_project_and_kind_metadata() -> None:
    main = _edition(slug="__main", title="Latest", kind=EditionKind.main)
    release = _edition(slug="v1.0.0", title="v1.0.0", kind=EditionKind.release)
    ctx = _make_context(main=main, releases=[release])

    payload = json.loads(
        EditionJsonRenderer().render(release, ctx).decode("utf-8")
    )

    assert payload["project_slug"] == "proj"
    assert payload["edition_title"] == "v1.0.0"
    assert payload["kind"] == "release"


def test_edition_json_falls_back_to_project_url_when_main_missing() -> None:
    """Defensive: a project without a ``__main`` edition still renders."""
    release = _edition(slug="v1.0.0", title="v1.0.0", kind=EditionKind.release)
    ctx = _make_context(releases=[release])

    payload = json.loads(
        EditionJsonRenderer().render(release, ctx).decode("utf-8")
    )

    assert payload["is_canonical"] is False
    assert payload["canonical_url"] == ctx.project.published_url


def _switcher_template_source(
    template_text: str, *, template_name: str = "switcher.json.jinja"
) -> _FakeTemplateSource:
    return _FakeTemplateSource(
        config=ParsedTemplateConfig(
            switcher=SwitcherConfig(template=template_name),
        ),
        templates={template_name: template_text},
    )


def test_switcher_template_renders_with_dashboard_context() -> None:
    """A ``[switcher] template`` renders with ``editions``/``project``/``org``.

    The demo template emits ``__main`` and the three newest releases
    only; the rendered bytes are the parsed array re-dumped exactly as
    the built-in renderer formats its own output.
    """
    main = _edition(slug="__main", title="Latest", kind=EditionKind.main)
    releases = [
        _edition(slug=s, title=s, kind=EditionKind.release)
        for s in ("v4.0.0", "v3.0.0", "v2.0.0", "v1.0.0")
    ]
    draft = _edition(slug="tickets-1", title="t1", kind=EditionKind.draft)
    ctx = _make_context(main=main, releases=releases, drafts=[draft])
    source = _switcher_template_source(DEMO_SWITCHER_JINJA)

    rendered = SwitcherJsonRenderer(template_source=source).render(
        ctx, switcher_config=source.load_config().switcher
    )

    expected = [
        {
            "name": "Latest",
            "version": "__main",
            "url": "https://proj.example.com/",
            "preferred": True,
        },
        *(
            {
                "name": f"{slug} of A Project (Proj Org)",
                "version": slug,
                "url": f"https://proj.example.com/v/{slug}/",
            }
            for slug in ("v4.0.0", "v3.0.0", "v2.0.0")
        ),
    ]
    assert rendered == json.dumps(expected, indent=2).encode("utf-8")


_VALID_ENTRY = '"name": "Latest", "version": "__main", "url": "https://p/"'


@pytest.mark.parametrize(
    ("template_text", "reason"),
    [
        pytest.param("not json", "not valid JSON", id="not-json"),
        pytest.param(
            "{" + _VALID_ENTRY + "}",
            "top-level value is an object, not an array",
            id="non-array",
        ),
        pytest.param(
            '["__main"]', "entry 0 is a string, not an object", id="non-object"
        ),
        pytest.param(
            '[{"name": "Latest", "version": "__main"}]',
            "entry 0 has no 'url'",
            id="missing-url",
        ),
        pytest.param(
            '[{"name": 1, "version": "__main", "url": "https://p/"}]',
            "entry 0 'name' is a number, not a string",
            id="non-string-name",
        ),
        pytest.param(
            "[{" + _VALID_ENTRY + '}, {"name": "v1", "version": "v1", '
            '"url": "https://p/v/v1/", "preferred": "yes"}]',
            "entry 1 'preferred' is a string, not a boolean",
            id="non-boolean-preferred",
        ),
        pytest.param(
            "[{" + _VALID_ENTRY + ', "prefered": true}]',
            "entry 0 has unexpected key 'prefered'",
            id="unexpected-key",
        ),
    ],
)
def test_switcher_template_rejects_malformed_output(
    template_text: str, reason: str
) -> None:
    """Output that is not a pydata switcher array raises a typed error."""
    main = _edition(slug="__main", title="Latest", kind=EditionKind.main)
    ctx = _make_context(main=main)
    source = _switcher_template_source(template_text)

    with pytest.raises(InvalidSwitcherJsonError) as exc_info:
        SwitcherJsonRenderer(template_source=source).render(
            ctx, switcher_config=source.load_config().switcher
        )

    exc = exc_info.value
    assert exc.template == "switcher.json.jinja"
    assert exc.org_slug == "proj"
    assert exc.project_slug == "proj"
    assert exc.reason is not None
    assert reason in exc.reason
    assert reason in str(exc)
    assert "'switcher.json.jinja'" in str(exc)


_DEFAULT_SWITCHER_BYTES = b"""\
[
  {
    "name": "Latest",
    "version": "__main",
    "url": "https://proj.example.com/",
    "preferred": true
  },
  {
    "name": "East",
    "version": "dev-east",
    "url": "https://proj.example.com/v/dev-east/",
    "preferred": true
  },
  {
    "name": "v2.x",
    "version": "v2",
    "url": "https://proj.example.com/v/v2/"
  },
  {
    "name": "v1.0.0",
    "version": "v1.0.0",
    "url": "https://proj.example.com/v/v1.0.0/"
  }
]"""


def test_switcher_without_template_output_is_byte_identical() -> None:
    """No ``[switcher] template`` keeps the built-in renderer's bytes.

    A template source alongside a template-less config changes nothing:
    the built-in ``include_kinds`` path renders exactly as before.
    """
    main = _edition(slug="__main", title="Latest", kind=EditionKind.main)
    release = _edition(slug="v1.0.0", title="v1.0.0", kind=EditionKind.release)
    major = _edition(slug="v2", title="v2.x", kind=EditionKind.major)
    draft = _edition(slug="latest", title="Latest", kind=EditionKind.draft)
    alt = _edition(slug="dev-east", title="East", kind=EditionKind.alternate)
    ctx = _make_context(
        main=main,
        releases=[release],
        major=[major],
        drafts=[draft],
        alternates=[alt],
    )
    source = BuiltInTemplateSource()

    assert SwitcherJsonRenderer().render(ctx) == _DEFAULT_SWITCHER_BYTES
    assert (
        SwitcherJsonRenderer(template_source=source).render(
            ctx, switcher_config=source.load_config().switcher
        )
        == _DEFAULT_SWITCHER_BYTES
    )


def test_switcher_template_without_template_source_raises() -> None:
    """A configured template with nothing to load it from is a wiring bug."""
    ctx = _make_context()

    with pytest.raises(RuntimeError, match=r"switcher\.json\.jinja"):
        SwitcherJsonRenderer().render(
            ctx,
            switcher_config=SwitcherConfig(template="switcher.json.jinja"),
        )
