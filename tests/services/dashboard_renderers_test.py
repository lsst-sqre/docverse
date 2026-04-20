"""Tests for the dashboard HTML and switcher JSON renderers."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from docverse.client.models import EditionKind
from docverse.domain.dashboard_context import (
    AssetsContext,
    BuildContext,
    DashboardContext,
    DocverseContext,
    EditionContext,
    EditionsContext,
    OrgContext,
    ProjectContext,
)
from docverse.services.dashboard_renderers import (
    DashboardHtmlRenderer,
    ErrorPageRenderer,
    SwitcherJsonRenderer,
)
from docverse.services.dashboard_template_source import (
    BuiltInTemplateSource,
    DashboardTemplateConfig,
    ParsedTemplateConfig,
    Switcher404Config,
    SwitcherConfig,
)


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
            source_repo_url="https://github.com/example/proj",
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
        ((), ["__main", "dev-east", "v1.0.0"]),
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
