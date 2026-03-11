"""Tests for the docverse.handlers.internal module and routes."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_get_health(client: AsyncClient) -> None:
    """Test ``GET /health``."""
    response = await client.get("health")
    assert response.status_code == 200
