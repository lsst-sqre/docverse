"""Built-in template source backed by packaged template assets."""

from __future__ import annotations

from importlib.resources import as_file, files
from pathlib import Path

from ..template_source import ParsedTemplateConfig, parse_template_toml

__all__ = ["BuiltInTemplateSource"]


class BuiltInTemplateSource:
    """Template source backed by built-in packaged templates.

    Templates ship as packaged files inside the server distribution and
    are loaded via :mod:`importlib.resources` so the lookup works in
    development checkouts and in zipped distributions alike.
    """

    _PACKAGE = "docverse.storage.dashboard_templates.builtin"

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
