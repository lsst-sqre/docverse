"""Pydantic models for LTD Keeper v1 API responses.

Captures the JSON shape returned by ``keeper.lsst.codes`` for products,
editions, and builds. Schema drift is tolerated: unknown fields are
ignored so a future LTD-side addition cannot break the sync.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

__all__ = [
    "LtdBuild",
    "LtdEdition",
    "LtdEditionMode",
    "LtdProduct",
    "LtdProductsListing",
]


class LtdEditionMode(StrEnum):
    """LTD Keeper edition tracking modes.

    Mirrors the strings the v1 API returns in the edition resource's
    ``mode`` field. Only ``git_refs`` has full mapper coverage in the
    sync engine foundation; the others are placeholders for follow-up
    slices that map them onto Docverse tracking modes.
    """

    git_refs = "git_refs"
    lsst_doc = "lsst_doc"
    eups_major_release = "eups_major_release"
    eups_weekly_release = "eups_weekly_release"
    eups_daily_release = "eups_daily_release"
    manual = "manual"


class LtdProductsListing(BaseModel):
    """Response model for ``GET /products/`` (the flat URL listing)."""

    model_config = ConfigDict(extra="ignore")

    products: list[HttpUrl] = Field(
        default_factory=list,
        description="Product resource URLs.",
    )


class LtdProduct(BaseModel):
    """Response model for ``GET /products/{slug}``."""

    model_config = ConfigDict(extra="ignore")

    self_url: HttpUrl
    slug: str
    doc_repo: HttpUrl
    title: str
    domain: str
    published_url: HttpUrl
    bucket_name: str | None = None


class LtdEdition(BaseModel):
    """Response model for ``GET /editions/{id}``.

    The numeric LTD edition id is parsed from the trailing path segment
    of ``self_url`` via :meth:`ltd_id`; the v1 API does not surface the
    integer id as its own field.
    """

    model_config = ConfigDict(extra="ignore")

    self_url: HttpUrl
    product_url: HttpUrl
    build_url: HttpUrl | None = None
    published_url: HttpUrl
    slug: str
    title: str
    date_created: datetime
    date_rebuilt: datetime | None = None
    date_ended: datetime | None = None
    pending_rebuild: bool = False
    tracked_refs: list[str] | None = None
    mode: str

    @property
    def ltd_id(self) -> int:
        """Parse the integer LTD id from the trailing ``self_url`` segment."""
        return _parse_trailing_id(str(self.self_url))

    @property
    def build_id(self) -> int | None:
        """Parse the trailing integer id from ``build_url``, if set."""
        if self.build_url is None:
            return None
        return _parse_trailing_id(str(self.build_url))


class LtdBuild(BaseModel):
    """Response model for ``GET /builds/{id}``."""

    model_config = ConfigDict(extra="ignore")

    self_url: HttpUrl
    product_url: HttpUrl
    slug: str
    date_created: datetime
    date_ended: datetime | None = None
    uploaded: bool = False
    bucket_name: str
    bucket_root_dir: str
    git_refs: list[str] | None = None
    github_requester: str | None = None
    published_url: HttpUrl
    surrogate_key: str | None = None

    @property
    def ltd_id(self) -> int:
        """Parse the integer LTD id from the trailing ``self_url`` segment."""
        return _parse_trailing_id(str(self.self_url))


def _parse_trailing_id(url: str) -> int:
    """Pull the trailing integer path segment off an LTD resource URL."""
    trimmed = url.rstrip("/")
    last = trimmed.rsplit("/", 1)[-1]
    return int(last)
