"""Spec-lint tests for the generated OpenAPI schema."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from httpx import AsyncClient

__all__ = []

_HTTP_METHODS = frozenset({"get", "post", "put", "patch", "delete"})

# Tags identifying the orgs sub-routers and admin router — the operations
# whose 403/404/409 error contract this spec-lint covers. Internal and
# webhook routes are deliberately excluded.
_IN_SCOPE_TAGS = frozenset({"orgs", "projects", "jobs", "admin"})

_ERROR_MODEL_REF = "#/components/schemas/ErrorModel"


def _in_scope_operations(
    spec: dict[str, Any],
) -> list[tuple[str, str, dict[str, Any]]]:
    """Return ``(path, method, operation)`` for in-scope operations."""
    operations: list[tuple[str, str, dict[str, Any]]] = []
    for path, methods in spec["paths"].items():
        for method, operation in methods.items():
            if method.lower() not in _HTTP_METHODS:
                continue
            tags = set(operation.get("tags", []))
            if tags & _IN_SCOPE_TAGS:
                operations.append((path, method, operation))
    return operations


def _documents_error_model(
    operation: dict[str, Any], status_code: str
) -> bool:
    """Whether an operation documents ``status_code`` with ``ErrorModel``."""
    response = operation.get("responses", {}).get(status_code)
    if response is None:
        return False
    schema = (
        response.get("content", {})
        .get("application/json", {})
        .get("schema", {})
    )
    return bool(schema.get("$ref") == _ERROR_MODEL_REF)


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


@pytest.mark.asyncio
async def test_org_dashboard_rebuild_tagged_orgs(client: AsyncClient) -> None:
    """The org-wide dashboard rebuild is an org-level op tagged ``orgs``.

    ``POST /orgs/{org}/dashboard/rebuild`` addresses the whole
    organization (not a single project), so it must carry the ``orgs``
    tag and must not be mis-filed under ``projects``.
    """
    spec = (await client.get("/docverse/openapi.json")).json()
    operation = spec["paths"]["/docverse/orgs/{org}/dashboard/rebuild"]["post"]
    tags = set(operation.get("tags", []))
    assert tags == {"orgs"}


@pytest.mark.asyncio
async def test_operations_document_forbidden_response(
    client: AsyncClient,
) -> None:
    """Every in-scope operation documents a 403 with the error body.

    Orgs and admin operations all sit behind a role dependency, so each
    can return 403 for an insufficiently-privileged caller. That contract
    must be documented from the shared error-responses helper with safir's
    ``ErrorModel`` body schema.
    """
    spec = (await client.get("/docverse/openapi.json")).json()
    missing = [
        f"{method.upper()} {path}"
        for path, method, operation in _in_scope_operations(spec)
        if not _documents_error_model(operation, "403")
    ]
    assert missing == [], (
        "operations must document a 403 ErrorModel response:\n"
        + "\n".join(missing)
    )


@pytest.mark.asyncio
async def test_resource_operations_document_not_found_response(
    client: AsyncClient,
) -> None:
    """Every in-scope operation with a path parameter documents 404.

    An operation addressing a resource by a path parameter (e.g.
    ``/orgs/{org}/...``) can always miss, so it must document a 404 with
    the shared ``ErrorModel`` body schema. Collection endpoints without a
    path parameter (e.g. ``POST /admin/orgs``) are exempt.
    """
    spec = (await client.get("/docverse/openapi.json")).json()
    missing = [
        f"{method.upper()} {path}"
        for path, method, operation in _in_scope_operations(spec)
        if "{" in path and not _documents_error_model(operation, "404")
    ]
    assert missing == [], (
        "resource operations must document a 404 ErrorModel response:\n"
        + "\n".join(missing)
    )


@pytest.mark.asyncio
async def test_conflict_operations_document_conflict_response(
    client: AsyncClient,
) -> None:
    """Operations that can conflict document a 409 with the error body.

    The create/enqueue operations that raise ``ConflictError`` must
    surface that in the contract via the shared helper. Keyed by
    operationId (which equals the route name).
    """
    spec = (await client.get("/docverse/openapi.json")).json()
    by_op_id = {
        operation["operationId"]: operation
        for _, _, operation in _in_scope_operations(spec)
    }
    conflict_op_ids = [
        "admin_post_organization",
        "post_member",
        "post_dashboard_rebuild",
    ]
    missing = [
        op_id
        for op_id in conflict_op_ids
        if not _documents_error_model(by_op_id[op_id], "409")
    ]
    assert missing == [], (
        "conflict operations must document a 409 ErrorModel response: "
        f"{missing}"
    )
