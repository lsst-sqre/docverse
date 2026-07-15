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
    "KeeperSyncInvariantError",
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

    Carries the queue job's API-facing identifiers (base32 ``public_id``,
    queue name, and worker function name) so a Sentry triager can paste
    the ``public_id`` straight into a ``GET /queue/{job_id}`` URL without
    translating an internal row id. Construct with the structured
    kwargs; ``message`` defaults to a useful summary of the transition
    when omitted.

    This is a non-HTTP exception because it may be raised from worker
    code outside of a request context. The same class is reused by the
    ``keeper_sync_runs`` and ``lifecycle_eval_runs`` stores for
    run-state transition errors: those raises supply ``current_state``
    / ``target_state`` / ``queue_name`` but leave ``job_public_id``
    unset because a run is not itself a queue job.
    """

    def __init__(
        self,
        *,
        current_state: str | None = None,
        target_state: str | None = None,
        job_public_id: str | None = None,
        queue_name: str | None = None,
        job_function: str | None = None,
        message: str | None = None,
    ) -> None:
        if message is None:
            message = self._format_message(
                current_state=current_state,
                target_state=target_state,
                job_public_id=job_public_id,
            )
        super().__init__(message)
        self.current_state = current_state
        self.target_state = target_state
        self.job_public_id = job_public_id
        self.queue_name = queue_name
        self.job_function = job_function

    @override
    def to_sentry(self) -> SentryEventInfo:
        info = super().to_sentry()
        if self.queue_name is not None:
            info.tags["queue_name"] = self.queue_name
        if self.job_function is not None:
            info.tags["job_function"] = self.job_function
        if self.current_state is not None:
            info.tags["job_current_state"] = self.current_state
        if self.target_state is not None:
            info.tags["job_target_state"] = self.target_state
        transition: dict[str, Any] = {
            "job_public_id": self.job_public_id,
            "queue_name": self.queue_name,
            "job_function": self.job_function,
            "current_state": self.current_state,
            "target_state": self.target_state,
        }
        info.contexts["queue_job_transition"] = transition
        return info

    @staticmethod
    def _format_message(
        *,
        current_state: str | None,
        target_state: str | None,
        job_public_id: str | None,
    ) -> str:
        job_part = (
            f"job {job_public_id}" if job_public_id is not None else "job"
        )
        if current_state is not None and target_state is not None:
            return (
                f"Cannot transition {job_part} from "
                f"{current_state!r} to {target_state!r}"
            )
        if target_state is not None:
            return f"Cannot transition {job_part} to {target_state!r}"
        if current_state is not None:
            return (
                f"Invalid state transition for {job_part} "
                f"from {current_state!r}"
            )
        return f"Invalid state for {job_part}"


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

    def __init__(
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
            message = self._format_message(
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

    @staticmethod
    def _format_message(
        *,
        current_state: str | None,
        target_state: str | None,
        build_public_id: str | None,
    ) -> str:
        build_part = (
            f"build {build_public_id}"
            if build_public_id is not None
            else "build"
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
                f"Invalid state transition for {build_part} "
                f"from {current_state!r}"
            )
        return f"Invalid state for {build_part}"


class JobNotFoundError(DocverseSlackException):
    """A queue job was not found in the database.

    Carries the missing queue job's API-facing identifiers (base32
    ``public_id``, queue name, and the worker function or lookup site
    that triggered the miss) so a Sentry triager has the same
    ``/queue/{job_id}`` link as for a successful lookup. Construct with
    the structured kwargs; when ``job_public_id`` is unknown (e.g. the
    lookup was by internal row id) pass ``message=`` to render the
    internal identifier in logs without leaking it into Sentry tags.

    This is a non-HTTP exception because it may be raised from worker
    code outside of a request context.
    """

    def __init__(
        self,
        *,
        job_public_id: str | None = None,
        queue_name: str | None = None,
        job_function: str | None = None,
        message: str | None = None,
    ) -> None:
        if message is None:
            message = self._format_message(job_public_id=job_public_id)
        super().__init__(message)
        self.job_public_id = job_public_id
        self.queue_name = queue_name
        self.job_function = job_function

    @override
    def to_sentry(self) -> SentryEventInfo:
        info = super().to_sentry()
        if self.queue_name is not None:
            info.tags["queue_name"] = self.queue_name
        lookup: dict[str, Any] = {
            "job_public_id": self.job_public_id,
            "queue_name": self.queue_name,
            "job_function": self.job_function,
        }
        info.contexts["queue_job_lookup"] = lookup
        return info

    @staticmethod
    def _format_message(*, job_public_id: str | None) -> str:
        if job_public_id is not None:
            return f"Queue job {job_public_id} not found"
        return "Queue job not found"


class KeeperSyncInvariantError(DocverseSlackException):
    """An internal keeper-sync invariant was violated.

    Raised by "should never happen" guards on the keeper-sync sync and
    tombstone paths (composing a tombstone response from a row that is
    not tombstoned, a state row vanishing mid-transaction, or
    ``_fetch_live_refs`` invoked without its GitHub collaborators
    configured). Replaces bare ``assert`` statements so the invariant
    still fails loudly under ``python -O``.

    No ``to_sentry`` override: the stack trace plus the free-form
    ``message`` is enough to triage, and the only identifiers in scope
    (``state_id``, ``org_id``) are internal row ids that must not be
    surfaced as Sentry tags. Mirrors :class:`InvalidSlugError`.
    """
