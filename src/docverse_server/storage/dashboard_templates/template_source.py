"""Dashboard template-source abstraction.

Defines the :class:`TemplateSource` protocol used by the dashboard
rendering pipeline along with the parsed-config dataclasses and the
shared ``template.toml`` parser.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from docverse_server.exceptions import DocverseSlackException

__all__ = [
    "DashboardTemplateConfig",
    "InvalidTemplateConfigError",
    "ParsedTemplateConfig",
    "Switcher404Config",
    "SwitcherConfig",
    "TemplateSource",
    "parse_template_toml",
]


class InvalidTemplateConfigError(DocverseSlackException):
    """A ``template.toml`` parsed as TOML but a value is unusable.

    Raised by :func:`parse_template_toml` so a template-set sync records
    the problem in the binding's ``last_sync_error`` instead of storing a
    set whose every ``dashboard_build`` would fail later.

    No ``to_sentry`` override: the message names the key and its value,
    and the sync records it rather than reporting it. Mirrors
    :class:`~docverse_server.domain.slug.InvalidSlugError`.

    Parameters
    ----------
    key
        The offending key as the docs name it, e.g.
        ``"[switcher] template"``.
    reason
        What is wrong with its value, e.g.
        ``"must be a non-empty string, got ''"``.
    message
        Rendered message, overriding the default built from the fields
        above.
    """

    def __init__(
        self,
        *,
        key: str | None = None,
        reason: str | None = None,
        message: str | None = None,
    ) -> None:
        self.key = key
        self.reason = reason
        super().__init__(
            message
            if message is not None
            else self._format_message(key=key, reason=reason)
        )

    @staticmethod
    def _format_message(*, key: str | None, reason: str | None) -> str:
        named = key or "A template.toml key"
        return f"{named} {reason or 'is invalid'}"


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
    """``[switcher]`` section.

    ``template`` names a Jinja file in the template set that renders the
    whole switcher JSON array itself. When it is set, ``include_kinds``
    is ignored; when it is ``None``, the built-in renderer lists the
    editions of ``include_kinds``.
    """

    include_kinds: tuple[str, ...] = _DEFAULT_INCLUDE_KINDS
    template: str | None = None


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
    """Parse a ``template.toml`` payload into a structured config.

    Raises
    ------
    tomllib.TOMLDecodeError
        The payload is not TOML.
    UnicodeDecodeError
        The payload is not UTF-8.
    InvalidTemplateConfigError
        ``[switcher] template`` is set to anything but a non-empty
        string.
    """
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
    switcher_template = switcher_section.get("template")
    # A set value that names no file would sync cleanly only to fail
    # every dashboard_build from the Jinja loader.
    if switcher_template is not None and (
        not isinstance(switcher_template, str) or not switcher_template
    ):
        raise InvalidTemplateConfigError(
            key="[switcher] template",
            reason=f"must be a non-empty string, got {switcher_template!r}",
        )
    switcher = SwitcherConfig(
        include_kinds=tuple(
            switcher_section.get("include_kinds", _DEFAULT_INCLUDE_KINDS)
        ),
        template=switcher_template,
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
