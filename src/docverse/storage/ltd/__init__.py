"""Clients and typed views for the legacy LTD Keeper API and S3 bucket."""

from __future__ import annotations

from .client import LtdClient, LtdClientError, LtdNotFoundError
from .models import (
    LtdBuild,
    LtdEdition,
    LtdEditionMode,
    LtdProduct,
    LtdProductsListing,
)
from .products_client import LtdProductsClient
from .s3_source import LtdS3Source, LtdSourceProtocol

__all__ = [
    "LtdBuild",
    "LtdClient",
    "LtdClientError",
    "LtdEdition",
    "LtdEditionMode",
    "LtdNotFoundError",
    "LtdProduct",
    "LtdProductsClient",
    "LtdProductsListing",
    "LtdS3Source",
    "LtdSourceProtocol",
]
