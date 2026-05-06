"""LTD Keeper sync engine.

Pulls projects, editions, and builds from the legacy LTD Keeper API
into a Docverse organization, copying build content from the public
``lsst-the-docs`` S3 bucket into Docverse R2 storage.

See SQR-112 and the parent PRD (#275) for the full design.
"""

from __future__ import annotations

from .copier import BuildContentCopier, CopyResult
from .mappers import (
    derive_edition_kind,
    derive_edition_slug,
    map_edition_tracking,
)
from .service import (
    BuildSyncOutcome,
    CopyCallable,
    EditionSyncOutcome,
    KeeperSyncContext,
    KeeperSyncService,
    ProjectSyncResult,
)

__all__ = [
    "BuildContentCopier",
    "BuildSyncOutcome",
    "CopyCallable",
    "CopyResult",
    "EditionSyncOutcome",
    "KeeperSyncContext",
    "KeeperSyncService",
    "ProjectSyncResult",
    "derive_edition_kind",
    "derive_edition_slug",
    "map_edition_tracking",
]
