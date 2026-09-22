"""Reusable OpenAPI ``responses=`` declarations for error status codes.

Handlers and routers attach these via the FastAPI ``responses=`` argument so
the generated OpenAPI spec documents each operation's 403/404/409/502 error
contract with safir's :class:`~safir.models.ErrorModel` body shape, instead
of leaving those responses undocumented or repeating inline literals in every
handler.

502 is here alongside the client-error codes because Docverse renders it
through the same ``ErrorModel`` path: it is raised as an
:class:`~docverse_server.exceptions.UpstreamServiceError`, which safir's
``client_request_error_handler`` serializes exactly like a 4xx.
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
    status.HTTP_502_BAD_GATEWAY: {
        "model": ErrorModel,
        "description": (
            "An upstream service the operation had to call synchronously"
            " was unreachable or returned an error. The message carries"
            " that service's own status when it returned one."
        ),
    },
}


def error_responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    """Build ``responses=`` entries for the given client-error status codes.

    Parameters
    ----------
    *status_codes
        One or more of 403, 404, 409, and 502. Each maps to an OpenAPI
        response documented with safir's ``ErrorModel`` body schema.

    Returns
    -------
    dict
        A mapping suitable for FastAPI's ``responses=`` argument. A fresh
        copy of each entry is returned so callers may merge or mutate the
        result without disturbing the shared templates.
    """
    return {code: dict(_ERROR_RESPONSES[code]) for code in status_codes}
