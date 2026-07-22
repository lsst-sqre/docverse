"""Spec-lint tests for the generated OpenAPI schema."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from httpx import AsyncClient

__all__ = []

_HTTP_METHODS = frozenset({"get", "post", "put", "patch", "delete"})


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


@pytest.mark.asyncio
async def test_operation_ids_match_route_names(
    app: FastAPI, client: AsyncClient
) -> None:
    """Every operationId equals its route's declared ``name``.

    FastAPI's default ``operationId`` is derived from the route name *and*
    its path plus HTTP method (e.g.
    ``get_build_docverse_orgs__org__projects__project__builds__build__get``),
    which leaks the URL structure into the public contract and produces
    unwieldy client codegen. Passing a ``generate_unique_id_function`` that
    returns ``route.name`` makes each operationId the clean route name.
    """
    expected: dict[tuple[str, str], str] = {}
    for route in app.routes:
        if not isinstance(route, APIRoute) or not route.include_in_schema:
            continue
        for method in route.methods:
            if method.lower() in _HTTP_METHODS:
                expected[(route.path, method.lower())] = route.name

    spec = (await client.get("/docverse/openapi.json")).json()
    mismatches: list[str] = []
    for path, operations in spec["paths"].items():
        for method, operation in operations.items():
            if method.lower() not in _HTTP_METHODS:
                continue
            op_id = operation.get("operationId")
            want = expected.get((path, method.lower()))
            if op_id != want:
                mismatches.append(
                    f"{method.upper()} {path}: operationId={op_id!r} "
                    f"expected route name={want!r}"
                )
    assert mismatches == [], (
        "operationIds must equal their route names:\n" + "\n".join(mismatches)
    )


@pytest.mark.asyncio
async def test_no_path_derived_operation_ids(client: AsyncClient) -> None:
    """No operationId carries a path-derived suffix.

    Route names use single-underscore words (``post_build``); FastAPI's
    default path-mangled operationIds contain double underscores from the
    substituted path separators. Asserting no operationId contains ``__``
    guards against any default-generated id slipping back into the spec.
    """
    spec = (await client.get("/docverse/openapi.json")).json()
    op_ids = [
        operation["operationId"]
        for operations in spec["paths"].values()
        for method, operation in operations.items()
        if method.lower() in _HTTP_METHODS and "operationId" in operation
    ]
    assert op_ids, "spec exposed no operations"
    path_derived = sorted(op_id for op_id in op_ids if "__" in op_id)
    assert path_derived == [], (
        f"default path-derived operationIds must not appear in openapi.json: "
        f"{path_derived}"
    )
