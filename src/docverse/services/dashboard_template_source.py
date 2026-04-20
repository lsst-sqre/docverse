"""Dashboard template-source abstraction.

Defines the :class:`TemplateSource` protocol used by the dashboard
rendering pipeline and the built-in implementation that loads template
files packaged inside the ``docverse`` distribution.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from importlib.resources import as_file, files
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "BuiltInTemplateSource",
    "DashboardTemplateConfig",
    "ParsedTemplateConfig",
    "Switcher404Config",
    "SwitcherConfig",
    "TemplateSource",
]


_DEFAULT_INCLUDE_KINDS: tuple[str, ...] = (
    "main",
    "release",
    "major",
    "alternate",
)


@dataclass(frozen=True)
class DashboardTemplateConfig:
    """``[dashboard]`` and ``[dashboard.assets]`` sections."""

    template: str = "dashboard.html.jinja"
    css: tuple[str, ...] = ()
    js: tuple[str, ...] = ()
    images: tuple[str, ...] = ()


@dataclass(frozen=True)
class Switcher404Config:
    """``[error_404]`` and ``[error_404.assets]`` sections."""

    template: str = "404.html.jinja"
    css: tuple[str, ...] = ()
    js: tuple[str, ...] = ()
    images: tuple[str, ...] = ()


@dataclass(frozen=True)
class SwitcherConfig:
    """``[switcher]`` section."""

    include_kinds: tuple[str, ...] = _DEFAULT_INCLUDE_KINDS


@dataclass(frozen=True)
class ParsedTemplateConfig:
    """Structured view of a ``template.toml`` file."""

    dashboard: DashboardTemplateConfig = field(
        default_factory=DashboardTemplateConfig
    )
    error_404: Switcher404Config | None = None
    switcher: SwitcherConfig = field(default_factory=SwitcherConfig)


def _parse_assets(section: dict[str, Any]) -> dict[str, tuple[str, ...]]:
    return {
        "css": tuple(section.get("css", [])),
        "js": tuple(section.get("js", [])),
        "images": tuple(section.get("images", [])),
    }


def parse_template_toml(data: bytes) -> ParsedTemplateConfig:
    """Parse a ``template.toml`` payload into a structured config."""
    parsed = tomllib.loads(data.decode("utf-8"))

    dashboard_section = parsed.get("dashboard", {})
    dashboard_assets = parsed.get("dashboard", {}).get("assets", {})
    dashboard = DashboardTemplateConfig(
        template=dashboard_section.get("template", "dashboard.html.jinja"),
        **_parse_assets(dashboard_assets),
    )

    error_404: Switcher404Config | None = None
    if "error_404" in parsed:
        e_section = parsed["error_404"]
        e_assets = e_section.get("assets", {})
        error_404 = Switcher404Config(
            template=e_section.get("template", "404.html.jinja"),
            **_parse_assets(e_assets),
        )

    switcher_section = parsed.get("switcher", {})
    switcher = SwitcherConfig(
        include_kinds=tuple(
            switcher_section.get("include_kinds", _DEFAULT_INCLUDE_KINDS)
        ),
    )

    return ParsedTemplateConfig(
        dashboard=dashboard,
        error_404=error_404,
        switcher=switcher,
    )


@runtime_checkable
class TemplateSource(Protocol):
    """Backend-agnostic interface for loading dashboard templates."""

    def load_config(self) -> ParsedTemplateConfig:
        """Return the parsed ``template.toml`` for this source."""
        ...

    def read_template(self, name: str) -> str:
        """Return the source text of a Jinja template by name."""
        ...

    def read_asset(self, path: str) -> bytes:
        """Return the bytes of an asset by relative path."""
        ...


class BuiltInTemplateSource:
    """Template source backed by built-in packaged templates.

    Templates ship as packaged files inside the server distribution and
    are loaded via :mod:`importlib.resources` so the lookup works in
    development checkouts and in zipped distributions alike.
    """

    _PACKAGE = "docverse.dashboard_templates.builtin"

    def __init__(self) -> None:
        self._root = files(self._PACKAGE)
        self._config: ParsedTemplateConfig | None = None

    def load_config(self) -> ParsedTemplateConfig:
        """Parse and cache the packaged ``template.toml``."""
        if self._config is None:
            data = self._read_bytes("template.toml")
            self._config = parse_template_toml(data)
        return self._config

    def read_template(self, name: str) -> str:
        """Read a Jinja template from the packaged template directory."""
        return self._read_bytes(name).decode("utf-8")

    def read_asset(self, path: str) -> bytes:
        """Read an asset file from the packaged template directory."""
        return self._read_bytes(path)

    def _read_bytes(self, relative: str) -> bytes:
        resource = self._root.joinpath(relative)
        with as_file(resource) as path:
            return Path(path).read_bytes()
