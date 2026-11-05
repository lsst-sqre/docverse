"""Tests for the dashboard template source."""

from __future__ import annotations

import pytest

from docverse_server.storage.dashboard_templates.builtin import (
    BuiltInTemplateSource,
)
from docverse_server.storage.dashboard_templates.template_source import (
    InvalidTemplateConfigError,
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


def test_parse_template_toml_switcher_template_alongside_include_kinds() -> (
    None
):
    payload = b"""
[dashboard]

[switcher]
include_kinds = ["main", "release"]
template = "switcher.json.jinja"
"""
    config = parse_template_toml(payload)
    assert config.switcher.template == "switcher.json.jinja"
    assert config.switcher.include_kinds == ("main", "release")


def test_parse_template_toml_switcher_template_defaults_to_none() -> None:
    config = parse_template_toml(b"[switcher]\ninclude_kinds = []\n")
    assert config.switcher.template is None


@pytest.mark.parametrize(
    ("value", "got"),
    [
        pytest.param('""', "''", id="empty"),
        pytest.param('["a.jinja"]', "['a.jinja']", id="array"),
        pytest.param("1", "1", id="integer"),
        pytest.param("true", "True", id="boolean"),
    ],
)
def test_parse_template_toml_rejects_unusable_switcher_template(
    value: str, got: str
) -> None:
    """A ``[switcher] template`` that names no file fails the parse.

    It would otherwise sync cleanly and then fail every
    ``dashboard_build`` from the Jinja loader; raising here lets the
    sync record the problem in the binding's ``last_sync_error``.
    """
    payload = f"[switcher]\ntemplate = {value}\n".encode()

    with pytest.raises(InvalidTemplateConfigError) as exc_info:
        parse_template_toml(payload)

    exc = exc_info.value
    assert exc.key == "[switcher] template"
    assert exc.reason == f"must be a non-empty string, got {got}"
    assert str(exc) == f"[switcher] template {exc.reason}"
