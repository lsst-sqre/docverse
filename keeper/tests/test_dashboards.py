"""Tests for the dashboard API."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from keeper.testutils import TestClient


def test_rebuild_dashboards(client: TestClient) -> None:
    """Test dashboard rebuilds with full client."""
    r = client.post("/dashboards", {})
    assert r.status == 202


def test_rebuild_dashboards_anon(anon_client: TestClient) -> None:
    """Test dashaboard rebuild with anonymous client."""
    r = anon_client.post("/dashboards", {})
    assert r.status == 401


def test_rebuild_dashboards_edition(edition_client: TestClient) -> None:
    """Test dashaboard rebuild with edition client."""
    r = edition_client.post("/dashboards", {})
    assert r.status == 403


def test_rebuild_dashboards_upload_build(
    upload_build_client: TestClient,
) -> None:
    """Test dashaboard rebuild with upload_build client."""
    r = upload_build_client.post("/dashboards", {})
    assert r.status == 403
