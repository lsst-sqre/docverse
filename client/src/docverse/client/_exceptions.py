"""Exceptions for the Docverse client."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import QueueJob

__all__ = [
    "BuildProcessingError",
    "DocverseClientError",
]


class DocverseClientError(Exception):
    """Base exception for Docverse client errors.

    Parameters
    ----------
    message
        Human-readable error description.
    status_code
        HTTP status code, if the error originated from an HTTP response.
    """

    def __init__(
        self, message: str, *, status_code: int | None = None
    ) -> None:
        self.status_code = status_code
        super().__init__(message)


class BuildProcessingError(DocverseClientError):
    """Raised when a build processing job fails.

    Parameters
    ----------
    message
        Human-readable error description.
    job
        The queue job that failed.
    """

    def __init__(self, message: str, *, job: QueueJob) -> None:
        self.job = job
        super().__init__(message)
