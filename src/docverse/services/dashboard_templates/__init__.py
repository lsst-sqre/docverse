"""Dashboard-template services (resolution, sync, fan-out)."""

from __future__ import annotations

from .resolver import (
    ResolvedTemplate,
    ResolvedTemplateOrigin,
    TemplateResolver,
)

__all__ = [
    "ResolvedTemplate",
    "ResolvedTemplateOrigin",
    "TemplateResolver",
]
