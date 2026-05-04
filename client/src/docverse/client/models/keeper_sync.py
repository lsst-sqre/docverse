"""Pydantic model for the LTD Keeper sync configuration on an org."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

__all__ = ["KeeperSyncConfig"]


_DEFAULT_LTD_BASE_URL = "https://keeper.lsst.codes"


class KeeperSyncConfig(BaseModel):
    """LTD Keeper sync configuration for an organization.

    Stored as a JSONB blob on the ``organizations`` row and validated
    through this model on read and write. ``GET /orgs/{org}/keeper-sync``
    returns a default-disabled instance when no config has been
    persisted; ``PUT`` replaces the stored config wholesale.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=False,
        description="Whether LTD Keeper sync is enabled on the organization.",
    )

    ltd_base_url: HttpUrl = Field(
        default=HttpUrl(_DEFAULT_LTD_BASE_URL),
        description="Base URL of the LTD Keeper API (v1 shape).",
    )

    project_slugs: list[str] | Literal["*"] = Field(
        default_factory=list,
        description=(
            'LTD project slugs to sync, or ``"*"`` for every project'
            " visible on the LTD instance."
        ),
    )
