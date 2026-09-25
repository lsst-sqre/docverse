"""ASGI middleware for the Docverse API service."""

from __future__ import annotations

from .api_request import ApiRequestMiddleware

__all__ = ["ApiRequestMiddleware"]
