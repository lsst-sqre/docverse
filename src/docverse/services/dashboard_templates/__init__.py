"""Dashboard-template services (resolution, sync, fan-out)."""

from __future__ import annotations

from .binding import (
    DashboardTemplateBindingResult,
    DashboardTemplateBindingService,
)
from .enqueue import DashboardSyncEnqueuer
from .fanout import DashboardRebuildFanout
from .installation_processor import InstallationEventProcessor
from .push_processor import PushEventProcessor
from .rename_processor import RenameEventProcessor
from .resolver import (
    ResolvedTemplate,
    ResolvedTemplateOrigin,
    TemplateResolver,
)
from .sync import DashboardTemplateSyncer, DashboardTemplateSyncError

__all__ = [
    "DashboardRebuildFanout",
    "DashboardSyncEnqueuer",
    "DashboardTemplateBindingResult",
    "DashboardTemplateBindingService",
    "DashboardTemplateSyncError",
    "DashboardTemplateSyncer",
    "InstallationEventProcessor",
    "PushEventProcessor",
    "RenameEventProcessor",
    "ResolvedTemplate",
    "ResolvedTemplateOrigin",
    "TemplateResolver",
]
