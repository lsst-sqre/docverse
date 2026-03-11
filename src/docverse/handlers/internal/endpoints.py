"""Internal endpoint handlers for Docverse."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from safir.metadata import get_metadata

from docverse.config import config

router = APIRouter(include_in_schema=False)
"""FastAPI router for internal endpoints."""


@router.get(
    "/",
    response_model=dict[str, Any],
    summary="Application metadata",
)
async def get_index() -> dict[str, Any]:
    """Return application metadata."""
    _metadata = get_metadata(
        package_name="docverse",
        application_name=config.name,
    )
    return _metadata.model_dump()


@router.get(
    "/health",
    summary="Health check",
)
async def get_health() -> dict[str, str]:
    """Return health status."""
    return {"status": "ok"}
