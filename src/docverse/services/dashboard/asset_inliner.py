"""Inline packaged template assets into an :class:`AssetsContext`.

Reads the CSS, JS, and image files declared in ``template.toml`` through
a :class:`TemplateSource` and produces the ``assets.*`` values that the
dashboard and 404 templates consume — concatenated CSS/JS strings, raw
SVG markup for SVG images, and base64 ``data:`` URIs for raster images.
"""

from __future__ import annotations

import base64
from collections.abc import Sequence
from pathlib import PurePosixPath

from docverse.domain.dashboard_context import AssetsContext
from docverse.storage.dashboard_templates.template_source import TemplateSource

__all__ = ["AssetInliner"]

_RASTER_MIME_TYPES: dict[str, str] = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
}
_SVG_EXT = ".svg"


class AssetInliner:
    """Concatenate and encode declared template assets.

    The inliner is stateless aside from its bound :class:`TemplateSource`;
    one instance can serve both the dashboard and the 404 page by
    invoking :meth:`inline` with each section's asset lists.
    """

    def __init__(self, *, template_source: TemplateSource) -> None:
        self._template_source = template_source

    def inline(
        self,
        *,
        css: Sequence[str],
        js: Sequence[str],
        images: Sequence[str],
    ) -> AssetsContext:
        """Read each declared asset and return an :class:`AssetsContext`.

        Parameters
        ----------
        css
            Relative paths of CSS files to concatenate, in declared order.
        js
            Relative paths of JS files to concatenate, in declared order.
        images
            Relative paths of image files. SVGs inline as raw markup;
            raster images (`.png`, `.jpg`, `.jpeg`, `.gif`, `.webp`) are
            encoded as base64 ``data:`` URIs.

        Raises
        ------
        ValueError
            If an image path does not have a supported extension.
        """
        return AssetsContext(
            css=self._concat_text(css),
            js=self._concat_text(js),
            images={
                _image_key(path): self._encode_image(path) for path in images
            },
        )

    def _concat_text(self, paths: Sequence[str]) -> str:
        chunks = [
            self._template_source.read_asset(path).decode("utf-8")
            for path in paths
        ]
        return "\n".join(chunks)

    def _encode_image(self, path: str) -> str:
        ext = PurePosixPath(path).suffix.lower()
        data = self._template_source.read_asset(path)
        if ext == _SVG_EXT:
            return data.decode("utf-8")
        mime = _RASTER_MIME_TYPES.get(ext)
        if mime is None:
            msg = (
                f"Unsupported image extension {ext!r} for asset {path!r}; "
                f"expected one of .svg, "
                f"{', '.join(sorted(_RASTER_MIME_TYPES))}"
            )
            raise ValueError(msg)
        encoded = base64.b64encode(data).decode("ascii")
        return f"data:{mime};base64,{encoded}"


def _image_key(path: str) -> str:
    """Convert ``path`` to the python-friendly ``assets.images`` key.

    Uses the basename only, then replaces ``.`` and ``-`` with ``_`` so
    templates can reference values via attribute-style lookups (e.g.
    ``assets.images.logo_svg``).
    """
    name = PurePosixPath(path).name
    return name.replace(".", "_").replace("-", "_")
