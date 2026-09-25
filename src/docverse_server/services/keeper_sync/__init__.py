"""LTD Keeper sync engine.

Pulls projects, editions, and builds from the legacy LTD Keeper API
into a Docverse organization, copying build content from the public
``lsst-the-docs`` S3 bucket into Docverse R2 storage.

See SQR-112 and the parent PRD (#275) for the full design.
"""

from __future__ import annotations

from .copier import (
    DEFAULT_COPY_CONCURRENCY,
    BuildContentCopier,
    CopyResult,
    CopyTally,
)
from .mappers import (
    EditionKindDerivation,
    KindDerivationSource,
    TrackingDerivationSource,
    derive_edition_kind,
    derive_edition_slug,
    derive_tracking_source,
    map_edition_tracking,
)
from .service import (
    DEFAULT_COPY_RETRY_DELAY_SECONDS,
    BuildCopiedCallback,
    BuildCopyReport,
    BuildSyncOutcome,
    CopyCallable,
    EditionSyncOutcome,
    KeeperSyncContext,
    KeeperSyncService,
    ProjectSyncResult,
)

__all__ = [
    "DEFAULT_COPY_CONCURRENCY",
    "DEFAULT_COPY_RETRY_DELAY_SECONDS",
    "BuildContentCopier",
    "BuildCopiedCallback",
    "BuildCopyReport",
    "BuildSyncOutcome",
    "CopyCallable",
    "CopyResult",
    "CopyTally",
    "EditionKindDerivation",
    "EditionSyncOutcome",
    "KeeperSyncContext",
    "KeeperSyncService",
    "KindDerivationSource",
    "ProjectSyncResult",
    "TrackingDerivationSource",
    "derive_edition_kind",
    "derive_edition_slug",
    "derive_tracking_source",
    "map_edition_tracking",
]
