"""LTD Keeper sync engine.

Pulls projects, editions, and builds from the legacy LTD Keeper API
into a Docverse organization, copying build content from the public
``lsst-the-docs`` S3 bucket into Docverse R2 storage.

See SQR-112 and the parent PRD (#275) for the full design.
"""

from .client import LtdClient, LtdClientError, LtdNotFoundError
from .copier import BuildContentCopier, CopyResult
from .mappers import (
    derive_edition_kind,
    derive_edition_slug,
    map_edition_tracking,
)
from .models import (
    LtdBuild,
    LtdEdition,
    LtdEditionMode,
    LtdProduct,
    LtdProductsListing,
)
from .s3_source import LtdS3Source, LtdSourceProtocol
from .service import (
    BuildSyncOutcome,
    CopyCallable,
    EditionSyncOutcome,
    KeeperSyncService,
    ProjectSyncResult,
)
from .state_store import KeeperSyncState, KeeperSyncStateStore, ResourceType

__all__ = [
    "BuildContentCopier",
    "BuildSyncOutcome",
    "CopyCallable",
    "CopyResult",
    "EditionSyncOutcome",
    "KeeperSyncService",
    "KeeperSyncState",
    "KeeperSyncStateStore",
    "LtdBuild",
    "LtdClient",
    "LtdClientError",
    "LtdEdition",
    "LtdEditionMode",
    "LtdNotFoundError",
    "LtdProduct",
    "LtdProductsListing",
    "LtdS3Source",
    "LtdSourceProtocol",
    "ProjectSyncResult",
    "ResourceType",
    "derive_edition_kind",
    "derive_edition_slug",
    "map_edition_tracking",
]
