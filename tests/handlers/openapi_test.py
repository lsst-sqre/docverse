"""Spec-lint tests for the generated OpenAPI schema."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

__all__ = []


@pytest.mark.asyncio
async def test_no_module_mangled_schema_names(client: AsyncClient) -> None:
    """No component schema name is module-path mangled.

    When two handler/client models share a name, FastAPI disambiguates
    them by prefixing the full module path (e.g.
    ``docverse__handlers__orgs__models__Edition``). Those ``__``-laden
    names leak the server's internal package layout into the public
    contract and produce ugly client codegen. Convergent models must be
    collapsed onto a single schema so no such name survives.
    """
    spec = (await client.get("/docverse/openapi.json")).json()
    schemas = spec["components"]["schemas"]
    mangled = sorted(name for name in schemas if "__" in name)
    assert mangled == [], (
        f"module-mangled schema names must not appear in openapi.json: "
        f"{mangled}"
    )


@pytest.mark.asyncio
async def test_admin_organization_schema_named(client: AsyncClient) -> None:
    """Admin serves ``AdminOrganization``; orgs keep ``Organization``."""
    spec = (await client.get("/docverse/openapi.json")).json()
    schemas = spec["components"]["schemas"]
    assert "AdminOrganization" in schemas
    assert "Organization" in schemas
