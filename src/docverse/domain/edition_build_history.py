"""Domain model for edition build history."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class EditionBuildHistory(BaseModel):
    """Domain representation of an edition build history entry.

    Each entry records a build that an edition pointed to. Position 1
    is the most recent.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the history entry.")

    edition_id: int = Field(
        description="ID of the edition this history entry belongs to."
    )

    build_id: int = Field(
        description="ID of the build the edition pointed to."
    )

    position: int = Field(
        description="Ordering position; 1 is the most recent."
    )

    date_created: datetime = Field(
        description="Timestamp when this history entry was recorded."
    )
