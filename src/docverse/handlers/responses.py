"""Reusable OpenAPI ``responses=`` declarations for client-error codes.

Handlers and routers attach these via the FastAPI ``responses=`` argument so
the generated OpenAPI spec documents each operation's 403/404/409 error
contract with safir's :class:`~safir.models.ErrorModel` body shape, instead
of leaving those responses undocumented or repeating inline literals in every
handler.
"""

from __future__ import annotations

from typing import Any

from fastapi import status
from safir.models import ErrorModel

__all__ = ["error_responses"]

_ERROR_RESPONSES: dict[int, dict[str, Any]] = {
    status.HTTP_403_FORBIDDEN: {
        "model": ErrorModel,
        "description": (
            "The caller lacks the role required for this operation."
        ),
    },
    status.HTTP_404_NOT_FOUND: {
        "model": ErrorModel,
        "description": (
            "A resource addressed by the request path does not exist."
        ),
    },
    status.HTTP_409_CONFLICT: {
        "model": ErrorModel,
        "description": (
            "The request conflicts with the current state of the resource."
        ),
    },
}


def error_responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    """Build ``responses=`` entries for the given client-error status codes.

    Parameters
    ----------
    *status_codes
        One or more of 403, 404, and 409. Each maps to an OpenAPI response
        documented with safir's ``ErrorModel`` body schema.

    Returns
    -------
    dict
        A mapping suitable for FastAPI's ``responses=`` argument. A fresh
        copy of each entry is returned so callers may merge or mutate the
        result without disturbing the shared templates.
    """
    return {code: dict(_ERROR_RESPONSES[code]) for code in status_codes}
