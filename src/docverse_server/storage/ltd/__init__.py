"""Clients and typed views for the legacy LTD Keeper API and S3 bucket."""

from __future__ import annotations

from .client import (
    LtdClient,
    LtdClientError,
    LtdNotFoundError,
    LtdProductsError,
)
from .models import (
    LtdBuild,
    LtdEdition,
    LtdEditionMode,
    LtdProduct,
    LtdProductsListing,
)
from .products_client import LtdProductsClient
from .s3_source import (
    RETRYABLE_SOURCE_TRANSPORT_ERRORS,
    LtdS3Source,
    LtdSourceAccessDeniedError,
    LtdSourceProtocol,
)

__all__ = [
    "RETRYABLE_SOURCE_TRANSPORT_ERRORS",
    "LtdBuild",
    "LtdClient",
    "LtdClientError",
    "LtdEdition",
    "LtdEditionMode",
    "LtdNotFoundError",
    "LtdProduct",
    "LtdProductsClient",
    "LtdProductsError",
    "LtdProductsListing",
    "LtdS3Source",
    "LtdSourceAccessDeniedError",
    "LtdSourceProtocol",
]
