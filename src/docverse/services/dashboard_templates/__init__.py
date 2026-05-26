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
from .ref_deleted_processor import RefDeletedResult, RefDeletedWebhookProcessor
from .rename_processor import RenameEventProcessor
from .resolver import (
    ResolvedTemplate,
    ResolvedTemplateOrigin,
    TemplateResolver,
)
from .sync import DashboardTemplateSyncer

__all__ = [
    "DashboardRebuildFanout",
    "DashboardSyncEnqueuer",
    "DashboardTemplateBindingResult",
    "DashboardTemplateBindingService",
    "DashboardTemplateSyncer",
    "InstallationEventProcessor",
    "PushEventProcessor",
    "RefDeletedResult",
    "RefDeletedWebhookProcessor",
    "RenameEventProcessor",
    "ResolvedTemplate",
    "ResolvedTemplateOrigin",
    "TemplateResolver",
]
