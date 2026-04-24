"""Dashboard-template services (resolution, sync, fan-out)."""

from __future__ import annotations

from .binding import (
    DashboardTemplateBindingResult,
    DashboardTemplateBindingService,
)
from .resolver import (
    ResolvedTemplate,
    ResolvedTemplateOrigin,
    TemplateResolver,
)

__all__ = [
    "DashboardTemplateBindingResult",
    "DashboardTemplateBindingService",
    "ResolvedTemplate",
    "ResolvedTemplateOrigin",
    "TemplateResolver",
]
