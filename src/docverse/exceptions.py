"""Exception classes for Docverse."""

from __future__ import annotations

from fastapi import status
from safir.fastapi import ClientRequestError

__all__ = [
    "ConflictError",
    "InvalidJobStateError",
    "JobNotFoundError",
    "NotFoundError",
]


class NotFoundError(ClientRequestError):
    """The requested resource was not found."""

    error = "not_found"
    status_code = status.HTTP_404_NOT_FOUND


class ConflictError(ClientRequestError):
    """The request conflicts with an existing resource."""

    error = "conflict"
    status_code = status.HTTP_409_CONFLICT


class InvalidJobStateError(Exception):
    """A queue job state transition is invalid.

    This is a non-HTTP exception because it may be raised from worker
    code outside of a request context.
    """


class JobNotFoundError(Exception):
    """A queue job was not found in the database.

    This is a non-HTTP exception because it may be raised from worker
    code outside of a request context.
    """
