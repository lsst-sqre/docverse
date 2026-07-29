"""HTTP handlers for GitHub webhooks."""

from __future__ import annotations

from .github import router as webhook_router

__all__ = ["webhook_router"]
