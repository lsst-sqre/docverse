"""Domain model for individual template content files."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class DashboardTemplateContentFile(BaseModel):
    """One file in a synced template tree."""

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the file row.")

    content_id: int = Field(description="ID of the parent content row.")

    relative_path: str = Field(
        description="Path of the file relative to the template ``root_path``."
    )

    is_text: bool = Field(
        description=(
            "True for text-mode sources (Jinja, TOML, CSS, JS); False for "
            "binary assets (images, fonts, etc.)."
        )
    )

    data: bytes = Field(description="Raw bytes of the file.")

    size_bytes: int = Field(description="Size of ``data`` in bytes.")
