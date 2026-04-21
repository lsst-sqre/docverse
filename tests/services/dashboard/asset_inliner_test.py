"""Tests for the dashboard asset inliner."""

from __future__ import annotations

import base64
from dataclasses import dataclass, field

import pytest

from docverse.services.dashboard.asset_inliner import AssetInliner
from docverse.storage.dashboard_templates.template_source import (
    BuiltInTemplateSource,
    ParsedTemplateConfig,
)


@dataclass
class FakeTemplateSource:
    """In-memory template source for asset inliner tests."""

    assets: dict[str, bytes] = field(default_factory=dict)

    def load_config(self) -> ParsedTemplateConfig:  # pragma: no cover
        return ParsedTemplateConfig()

    def read_template(self, name: str) -> str:  # pragma: no cover
        raise NotImplementedError

    def read_asset(self, path: str) -> bytes:
        return self.assets[path]


def test_inliner_concatenates_css_in_declared_order() -> None:
    src = FakeTemplateSource(
        assets={
            "first.css": b"a { color: red; }",
            "second.css": b"b { color: blue; }",
        }
    )
    inliner = AssetInliner(template_source=src)

    assets = inliner.inline(
        css=("first.css", "second.css"),
        js=(),
        images=(),
    )

    assert assets.css == "a { color: red; }\nb { color: blue; }"
    assert assets.js == ""
    assert assets.images == {}


def test_inliner_concatenates_js_in_declared_order() -> None:
    src = FakeTemplateSource(
        assets={
            "a.js": b"const a = 1;",
            "b.js": b"const b = 2;",
        }
    )
    inliner = AssetInliner(template_source=src)

    assets = inliner.inline(
        css=(),
        js=("a.js", "b.js"),
        images=(),
    )

    assert assets.js == "const a = 1;\nconst b = 2;"


def test_inliner_inlines_svg_as_raw_markup() -> None:
    svg = b'<svg xmlns="http://www.w3.org/2000/svg"><rect/></svg>'
    src = FakeTemplateSource(assets={"logo.svg": svg})
    inliner = AssetInliner(template_source=src)

    assets = inliner.inline(css=(), js=(), images=("logo.svg",))

    assert assets.images == {"logo_svg": svg.decode("utf-8")}


def test_inliner_encodes_png_as_base64_data_uri() -> None:
    raw = b"\x89PNG\r\n\x1a\nfake-png-bytes"
    src = FakeTemplateSource(assets={"icon.png": raw})
    inliner = AssetInliner(template_source=src)

    assets = inliner.inline(css=(), js=(), images=("icon.png",))

    encoded = base64.b64encode(raw).decode("ascii")
    assert assets.images == {"icon_png": f"data:image/png;base64,{encoded}"}


@pytest.mark.parametrize(
    ("filename", "mime"),
    [
        ("photo.jpg", "image/jpeg"),
        ("photo.jpeg", "image/jpeg"),
        ("anim.gif", "image/gif"),
        ("hero.webp", "image/webp"),
    ],
)
def test_inliner_encodes_known_raster_types_as_data_uri(
    filename: str, mime: str
) -> None:
    raw = b"\x00\x01\x02\x03"
    src = FakeTemplateSource(assets={filename: raw})
    inliner = AssetInliner(template_source=src)

    assets = inliner.inline(css=(), js=(), images=(filename,))

    encoded = base64.b64encode(raw).decode("ascii")
    key = filename.replace(".", "_").replace("-", "_")
    assert assets.images == {key: f"data:{mime};base64,{encoded}"}


def test_inliner_filename_key_replaces_dots_and_hyphens() -> None:
    raw = b"\x89PNG\r\n\x1a\n"
    src = FakeTemplateSource(
        assets={
            "my-cool-logo.svg": b"<svg/>",
            "favicon-32x32.png": raw,
        }
    )
    inliner = AssetInliner(template_source=src)

    assets = inliner.inline(
        css=(),
        js=(),
        images=("my-cool-logo.svg", "favicon-32x32.png"),
    )

    assert "my_cool_logo_svg" in assets.images
    assert "favicon_32x32_png" in assets.images
    assert assets.images["my_cool_logo_svg"] == "<svg/>"


def test_inliner_uses_basename_for_key_when_path_has_directory() -> None:
    src = FakeTemplateSource(assets={"images/logo.svg": b"<svg/>"})
    inliner = AssetInliner(template_source=src)

    assets = inliner.inline(css=(), js=(), images=("images/logo.svg",))

    assert assets.images == {"logo_svg": "<svg/>"}


def test_inliner_rejects_unknown_image_extension() -> None:
    src = FakeTemplateSource(assets={"thing.bmp": b"\x00"})
    inliner = AssetInliner(template_source=src)

    with pytest.raises(ValueError, match="Unsupported image extension"):
        inliner.inline(css=(), js=(), images=("thing.bmp",))


def test_inliner_returns_empty_assets_when_nothing_declared() -> None:
    src = FakeTemplateSource()
    inliner = AssetInliner(template_source=src)

    assets = inliner.inline(css=(), js=(), images=())

    assert assets.css == ""
    assert assets.js == ""
    assert assets.images == {}


def test_inliner_round_trips_packaged_built_in_template_assets() -> None:
    """Integration check: packaged assets load via importlib.resources."""
    src = BuiltInTemplateSource()
    config = src.load_config()
    inliner = AssetInliner(template_source=src)

    assets = inliner.inline(
        css=config.dashboard.css,
        js=config.dashboard.js,
        images=config.dashboard.images,
    )

    # CSS / JS were declared in template.toml and should round-trip
    # through the packaged data into non-empty source text.
    assert "body" in assets.css
    assert "docverse-js" in assets.js

    # Both image branches (raw SVG + base64 data URI) round-trip and use
    # the documented filename-to-key conversion.
    assert assets.images["logo_svg"].startswith("<svg")
    assert assets.images["favicon_png"].startswith("data:image/png;base64,")
