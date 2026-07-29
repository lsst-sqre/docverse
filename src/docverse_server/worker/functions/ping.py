"""Proof-of-concept ping task for the arq worker."""

from __future__ import annotations

from typing import Any


async def ping(ctx: dict[str, Any]) -> str:
    """Return 'pong' — used to verify the worker is running."""
    return "pong"
