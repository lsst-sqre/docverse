"""Exception classes for Docverse."""

from __future__ import annotations

from fastapi import status
from safir.fastapi import ClientRequestError
from safir.slack.blockkit import SlackException

__all__ = [
    "BadRequestError",
    "ConflictError",
    "DocverseSlackException",
    "InvalidBase32IdError",
    "InvalidBuildStateError",
    "InvalidJobStateError",
    "JobNotFoundError",
    "MissingConfigurationError",
    "NotFoundError",
    "PermissionDeniedError",
]


class DocverseSlackException(SlackException):
    """Shared base for non-``ClientRequestError`` server-side exceptions.

    Every Docverse exception that should be routed to Slack and Sentry
    (i.e. anything that is not a 4xx user error) derives from this class.
    Future slices will override :meth:`to_sentry` on individual
    subclasses to surface API-facing identifiers as tags and contexts;
    this base exists so those overrides have one place to layer on.
    """


class BadRequestError(ClientRequestError):
    """The request is malformed or fails domain-level validation."""

    error = "bad_request"
    status_code = status.HTTP_400_BAD_REQUEST


class NotFoundError(ClientRequestError):
    """The requested resource was not found."""

    error = "not_found"
    status_code = status.HTTP_404_NOT_FOUND


class ConflictError(ClientRequestError):
    """The request conflicts with an existing resource."""

    error = "conflict"
    status_code = status.HTTP_409_CONFLICT


class MissingConfigurationError(ClientRequestError):
    """The organization is missing required configuration."""

    error = "missing_configuration"
    status_code = status.HTTP_422_UNPROCESSABLE_CONTENT


class InvalidBase32IdError(ClientRequestError):
    """A base32-encoded ID in the request is malformed."""

    error = "invalid_id"
    status_code = status.HTTP_422_UNPROCESSABLE_CONTENT


class PermissionDeniedError(ClientRequestError):
    """The user does not have permission to perform this action."""

    error = "permission_denied"
    status_code = status.HTTP_403_FORBIDDEN


class InvalidJobStateError(DocverseSlackException):
    """A queue job state transition is invalid.

    This is a non-HTTP exception because it may be raised from worker
    code outside of a request context.
    """


class InvalidBuildStateError(DocverseSlackException):
    """A build status transition is invalid.

    This is a non-HTTP exception because it may be raised from worker
    code outside of a request context.
    """


class JobNotFoundError(DocverseSlackException):
    """A queue job was not found in the database.

    This is a non-HTTP exception because it may be raised from worker
    code outside of a request context.
    """
