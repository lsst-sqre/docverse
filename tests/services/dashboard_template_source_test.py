"""Tests for the dashboard template source."""

from __future__ import annotations

from docverse.services.dashboard_template_source import (
    BuiltInTemplateSource,
    parse_template_toml,
)


def test_builtin_template_source_loads_packaged_config() -> None:
    src = BuiltInTemplateSource()
    config = src.load_config()
    assert config.dashboard.template == "dashboard.html.jinja"
    assert config.switcher.include_kinds == (
        "main",
        "release",
        "major",
        "alternate",
    )
    assert config.error_404 is None


def test_builtin_template_source_caches_parsed_config() -> None:
    src = BuiltInTemplateSource()
    first = src.load_config()
    second = src.load_config()
    assert first is second


def test_builtin_template_source_reads_html_jinja_template() -> None:
    src = BuiltInTemplateSource()
    template_text = src.read_template("dashboard.html.jinja")
    assert "{% if editions.main %}" in template_text
    assert "<!DOCTYPE html>" in template_text


def test_parse_template_toml_supports_optional_error_404() -> None:
    payload = b"""
[dashboard]
template = "dash.html.jinja"

[dashboard.assets]
css = ["dash.css"]

[error_404]
template = "404.html.jinja"

[error_404.assets]
js = ["err.js"]
"""
    config = parse_template_toml(payload)
    assert config.dashboard.template == "dash.html.jinja"
    assert config.dashboard.css == ("dash.css",)
    assert config.error_404 is not None
    assert config.error_404.template == "404.html.jinja"
    assert config.error_404.js == ("err.js",)


def test_parse_template_toml_default_switcher_kinds() -> None:
    config = parse_template_toml(b"[dashboard]\n")
    assert config.switcher.include_kinds == (
        "main",
        "release",
        "major",
        "alternate",
    )


def test_parse_template_toml_custom_switcher_kinds() -> None:
    payload = b"""
[dashboard]

[switcher]
include_kinds = ["draft"]
"""
    config = parse_template_toml(payload)
    assert config.switcher.include_kinds == ("draft",)
