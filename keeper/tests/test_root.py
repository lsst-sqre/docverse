"""Tests for the root route."""

from __future__ import annotations

from typing import TYPE_CHECKING

from keeper.version import get_version

if TYPE_CHECKING:
    from keeper.testutils import TestClient


def test_root(client: TestClient) -> None:
    r = client.get("/")
    assert r.status == 200

    data = r.json
    assert "data" in data
    assert "server_version" in data["data"]
    assert data["data"]["server_version"] == get_version()
    assert "documentation" in data["data"]
    assert "message" in data["data"]
    assert "links" in data
    assert "self_url" in data["links"]
    assert "products" in data["links"]
    assert "token" in data["links"]
