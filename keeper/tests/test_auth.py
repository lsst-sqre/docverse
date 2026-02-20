"""Test /token route to get auth token from a client providing a
username+password.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from keeper.testutils import TestClient


def test_get_token(basic_client: TestClient) -> None:
    r = basic_client.get("/token")
    assert r.status == 200
    assert "token" in r.json
