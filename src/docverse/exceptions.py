"""Exception classes for Docverse."""

from __future__ import annotations

from typing import Any, override

from fastapi import status
from safir.fastapi import ClientRequestError
from safir.slack.blockkit import SlackException
from safir.slack.sentry import SentryEventInfo

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
    """A build status transition is invalid or the target build is missing.

    Carries the build's API-facing identifiers (base32 ``public_id`` and
    the org / project / edition slugs) so a Sentry triager can paste them
    straight into a ``GET /v1/orgs/{org}/projects/{project}/builds/{id}``
    URL without translating an internal row id. Construct with the
    structured kwargs; ``message`` defaults to a useful summary of the
    transition when omitted.

    This is a non-HTTP exception because it may be raised from worker
    code outside of a request context.
    """

    def __init__(  # noqa: PLR0913
        self,
        *,
        current_state: str | None = None,
        target_state: str | None = None,
        build_public_id: str | None = None,
        project_slug: str | None = None,
        org_slug: str | None = None,
        edition_slug: str | None = None,
        message: str | None = None,
    ) -> None:
        if message is None:
            message = _format_build_transition_message(
                current_state=current_state,
                target_state=target_state,
                build_public_id=build_public_id,
            )
        super().__init__(message)
        self.current_state = current_state
        self.target_state = target_state
        self.build_public_id = build_public_id
        self.project_slug = project_slug
        self.org_slug = org_slug
        self.edition_slug = edition_slug

    @override
    def to_sentry(self) -> SentryEventInfo:
        info = super().to_sentry()
        if self.org_slug is not None:
            info.tags["org_slug"] = self.org_slug
        if self.project_slug is not None:
            info.tags["project_slug"] = self.project_slug
        if self.current_state is not None:
            info.tags["build_current_state"] = self.current_state
        if self.target_state is not None:
            info.tags["build_target_state"] = self.target_state
        transition: dict[str, Any] = {
            "build_public_id": self.build_public_id,
            "project_slug": self.project_slug,
            "org_slug": self.org_slug,
            "edition_slug": self.edition_slug,
            "current_state": self.current_state,
            "target_state": self.target_state,
        }
        info.contexts["build_transition"] = transition
        return info


def _format_build_transition_message(
    *,
    current_state: str | None,
    target_state: str | None,
    build_public_id: str | None,
) -> str:
    """Render a default message for :class:`InvalidBuildStateError`."""
    build_part = (
        f"build {build_public_id}" if build_public_id is not None else "build"
    )
    if current_state is not None and target_state is not None:
        return (
            f"Cannot transition {build_part} from "
            f"{current_state!r} to {target_state!r}"
        )
    if target_state is not None:
        return f"Cannot transition {build_part} to {target_state!r}"
    if current_state is not None:
        return (
            f"Invalid state transition for {build_part} from {current_state!r}"
        )
    return f"Invalid state for {build_part}"


class JobNotFoundError(DocverseSlackException):
    """A queue job was not found in the database.

    This is a non-HTTP exception because it may be raised from worker
    code outside of a request context.
    """
