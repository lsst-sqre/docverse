"""Async HTTP client for the Docverse API."""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import click
import httpx

from ._exceptions import BuildProcessingError, DocverseClientError
from .models import (
    Build,
    BuildStatus,
    BuildUpdate,
    KeeperSyncConfig,
    KeeperSyncConfigUpdate,
    OrganizationSummary,
    OrgMembership,
    OrgMembershipUpdate,
    OrgRole,
    Project,
    QueueJob,
)
from .models.builds import BuildAnnotations
from .models.queue_enums import JobStatus

__all__ = [
    "DEFAULT_UPDATED_SINCE_OVERLAP",
    "DocverseClient",
    "ProjectList",
]

_BACKOFF_INITIAL = 1.0
_BACKOFF_MAX = 15.0
_BACKOFF_FACTOR = 2.0
_VERBOSE_BODY_MAX = 2000
_TOKEN_SUFFIX_LEN = 4

DEFAULT_UPDATED_SINCE_OVERLAP = timedelta(seconds=60)
"""How far `DocverseClient.list_projects` backdates ``updated_since``.

A project's ``date_updated`` is stamped with PostgreSQL's transaction
*start* clock, and commit order is not start order, so a write that
began before a poll can become visible after it while wearing a
timestamp that poll has already passed. Asking from a minute earlier
than the caller believes it needs covers a write that took up to that
long to commit; the price is re-receiving the rows in the window, which
a caller keyed on a project's ``id`` simply overwrites.
"""


@dataclass(frozen=True, slots=True)
class ProjectList:
    """One complete pass over an organization's project listing.

    Returned by `DocverseClient.list_projects`, which walks every page
    before handing this back, so ``projects`` is the whole listing and
    not one page of it.

    A pass filtered by ``updated_since`` re-sends rows the previous
    pass already delivered: the request is backdated by
    `DEFAULT_UPDATED_SINCE_OVERLAP` so a slow commit cannot slip
    between two polls, and the inclusive bound re-sends the boundary
    row besides. Treat the listing as a set of upserts keyed on each
    project's ``id`` rather than as a stream of distinct changes.
    """

    projects: list[Project] = field(default_factory=list)
    """Every project the listing returned, in server order across pages.

    Empty when ``not_modified`` is `True`.
    """

    etag: str | None = None
    """The first page's ``ETag``, or `None` if the server sent none.

    An opaque validator: pass it back verbatim as ``if_none_match`` on
    the next poll rather than interpreting it.
    """

    last_modified: str | None = None
    """The first page's ``Last-Modified``, or `None` if none was sent.

    The raw HTTP-date string, to be passed back verbatim as
    ``if_modified_since``; it is left unparsed so a round trip cannot
    lose the second-granularity truncation the server applied.
    """

    not_modified: bool = False
    """Whether the server answered the first page with a 304.

    `True` means the caller's precondition matched and nothing in the
    listing has changed, so ``projects`` is empty because there was
    nothing to fetch — not because the organization has no projects.
    """


class DocverseClient:
    """Async HTTP client for the Docverse API.

    Use as an async context manager::

        async with DocverseClient(base_url, token) as client:
            build = await client.create_build(...)

    Parameters
    ----------
    base_url
        Root URL of the Docverse API (e.g. ``https://roundtable.lsst.cloud/docverse/api``).
    token
        Bearer token for authentication.
    timeout
        HTTP request timeout in seconds.
    verbose
        If `True`, log detailed HTTP request/response information.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 30.0,
        verbose: bool = False,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout
        self._verbose = verbose
        self._http: httpx.AsyncClient | None = None

    async def __aenter__(self) -> DocverseClient:  # noqa: PYI034
        self._http = httpx.AsyncClient(
            base_url=self._base_url,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=self._timeout,
            event_hooks=self._build_event_hooks(),
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    def _build_event_hooks(
        self,
    ) -> dict[str, list[Any]]:
        """Build httpx event hooks for verbose logging."""
        if not self._verbose:
            return {"request": [], "response": []}
        return {
            "request": [self._log_request],
            "response": [self._log_response],
        }

    async def _log_request(self, request: httpx.Request) -> None:
        """Log request details when verbose mode is enabled."""
        click.echo(f">> {request.method} {request.url}", err=True)
        for name, value in request.headers.items():
            display_value = (
                _mask_token(value)
                if name.lower() == "authorization"
                else value
            )
            click.echo(f">> {name}: {display_value}", err=True)
        click.echo(">>", err=True)

    async def _log_response(self, response: httpx.Response) -> None:
        """Log response details when verbose mode is enabled."""
        await response.aread()
        click.echo(
            f"<< {response.status_code} {response.reason_phrase}",
            err=True,
        )
        for name, value in response.headers.items():
            click.echo(f"<< {name}: {value}", err=True)
        body = response.text
        if len(body) > _VERBOSE_BODY_MAX:
            body = body[:_VERBOSE_BODY_MAX] + "... (truncated)"
        if body:
            click.echo(f"<< {body}", err=True)
        click.echo("<<", err=True)

    @property
    def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            msg = "DocverseClient must be used as an async context manager"
            raise RuntimeError(msg)
        return self._http

    async def list_organizations(self) -> list[OrganizationSummary]:
        """List organizations the caller can access.

        Returns one summary per organization in which the caller holds an
        effective role (via direct or group membership); a superadmin
        receives every organization. An empty list is a valid response.

        Returns
        -------
        list of OrganizationSummary
            The accessible organizations, each with the caller's effective
            role.
        """
        response = await self._client.get("/orgs")
        _raise_for_status(response)
        return [
            OrganizationSummary.model_validate(item)
            for item in response.json()
        ]

    async def list_projects(
        self,
        org: str,
        *,
        updated_since: datetime | None = None,
        updated_since_overlap: timedelta = DEFAULT_UPDATED_SINCE_OVERLAP,
        include_deleted: bool = False,
        order: str = "slug",
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
    ) -> ProjectList:
        """List every project in an organization, following pagination.

        Walks the listing's ``Link rel="next"`` chain to the last page,
        so the result holds the whole listing rather than one page.

        A pass filtered by ``updated_since`` **re-sends rows**: the
        bound is backdated by ``updated_since_overlap`` and is
        inclusive besides, so a caller must be prepared to see a
        project it has already processed. Deduplicate on the project's
        ``id``, which is stable across a slug rename.

        Parameters
        ----------
        org
            Organization slug.
        updated_since
            Only return projects whose ``date_updated`` is at or after
            this instant, less ``updated_since_overlap``. Must be
            timezone-aware; the server rejects a naive timestamp with a
            422.
        updated_since_overlap
            How far to backdate ``updated_since`` before sending it.
            A project's clock is PostgreSQL's transaction *start* time
            and commit order is not start order, so a write that began
            before the caller's last pass can become visible after it
            while wearing a timestamp that pass has already gone by;
            the overlap is the window of slow commits that covers.
            Defaults to `DEFAULT_UPDATED_SINCE_OVERLAP`. Pass
            ``timedelta(0)`` to send the caller's instant unmodified,
            accepting that a concurrent write can be missed.
        include_deleted
            Also return soft-deleted projects, each with its
            ``date_deleted`` set.
        order
            Sort order: ``slug``, ``date_created``, or ``date_updated``.
        if_none_match
            An ``ETag`` from a previous call, sent as ``If-None-Match``.
        if_modified_since
            A ``Last-Modified`` value from a previous call, sent as
            ``If-Modified-Since``. The server consults it only when
            ``if_none_match`` is absent.

        Returns
        -------
        ProjectList
            The projects, plus the first page's validators. When the
            server answers the first page 304, ``not_modified`` is
            `True` and ``projects`` is empty.
        """
        params: dict[str, Any] = {
            "order": order,
            "include_deleted": include_deleted,
        }
        if updated_since is not None:
            params["updated_since"] = (
                updated_since - updated_since_overlap
            ).isoformat()
        headers: dict[str, str] = {}
        if if_none_match is not None:
            headers["If-None-Match"] = if_none_match
        if if_modified_since is not None:
            headers["If-Modified-Since"] = if_modified_since

        # The preconditions belong to the first request alone: the
        # server validates the page the caller already holds, and the
        # ``Link`` URLs that follow are pages it has never seen.
        response = await self._client.get(
            f"/orgs/{org}/projects", params=params, headers=headers
        )
        etag = response.headers.get("ETag")
        last_modified = response.headers.get("Last-Modified")
        if response.status_code == httpx.codes.NOT_MODIFIED:
            return ProjectList(
                etag=etag,
                last_modified=last_modified,
                not_modified=True,
            )
        _raise_for_status(response)
        projects = [Project.model_validate(item) for item in response.json()]
        next_url = _next_page_url(response)
        while next_url is not None:
            response = await self._client.get(next_url)
            _raise_for_status(response)
            projects.extend(
                Project.model_validate(item) for item in response.json()
            )
            next_url = _next_page_url(response)
        return ProjectList(
            projects=projects, etag=etag, last_modified=last_modified
        )

    async def update_member(
        self, org: str, member: str, *, role: OrgRole
    ) -> OrgMembership:
        """Update an organization member's role.

        Only the ``role`` is mutable; a member's ``principal`` and
        ``principal_type`` are immutable (changing identity is a delete plus
        re-add).

        Parameters
        ----------
        org
            Organization slug.
        member
            Membership identifier in the ``{principal_type}:{principal}``
            format (e.g. ``user:jdoe``).
        role
            The new role to assign.

        Returns
        -------
        OrgMembership
            The updated membership.
        """
        update = OrgMembershipUpdate(role=role)
        url = f"/orgs/{org}/members/{member}"
        response = await self._client.patch(
            url, json=update.model_dump(exclude_unset=True)
        )
        _raise_for_status(response)
        return OrgMembership.model_validate(response.json())

    async def update_keeper_sync_config(
        self, org: str, update: KeeperSyncConfigUpdate
    ) -> KeeperSyncConfig:
        """Update an organization's LTD Keeper sync config in part.

        Applies JSON-Merge-Patch semantics: only the fields set on ``update``
        are changed; omitted fields are left untouched. ``project_slugs``,
        when provided, replaces the stored list wholesale (no append). Send a
        full ``KeeperSyncConfig`` via ``PUT`` for a complete replacement.

        Parameters
        ----------
        org
            Organization slug.
        update
            The partial update; construct it with only the fields to change.

        Returns
        -------
        KeeperSyncConfig
            The updated configuration.
        """
        url = f"/orgs/{org}/keeper-sync"
        response = await self._client.patch(
            url, json=update.model_dump(exclude_unset=True, mode="json")
        )
        _raise_for_status(response)
        return KeeperSyncConfig.model_validate(response.json())

    async def create_build(
        self,
        org: str,
        project: str,
        *,
        git_ref: str,
        content_hash: str | None = None,
        alternate_name: str | None = None,
        annotations: BuildAnnotations | None = None,
    ) -> Build:
        """Create a new build.

        Parameters
        ----------
        org
            Organization slug.
        project
            Project slug.
        git_ref
            Git ref for the build.
        content_hash
            Deprecated transport digest of the tarball
            (``sha256:<hex>``), recorded as provenance only. The server
            computes the build's content identity itself and overwrites
            this value at completion, so leaving it unset is preferred;
            when omitted the key is left out of the payload entirely
            rather than sent as ``null``.
        alternate_name
            Optional alternate deployment name.
        annotations
            Optional metadata annotations.

        Returns
        -------
        Build
            The created build, including ``upload_url``.
        """
        payload: dict[str, Any] = {"git_ref": git_ref}
        if content_hash is not None:
            payload["content_hash"] = content_hash
        if alternate_name is not None:
            payload["alternate_name"] = alternate_name
        if annotations is not None:
            payload["annotations"] = annotations.model_dump(exclude_none=True)

        url = f"/orgs/{org}/projects/{project}/builds"
        response = await self._client.post(url, json=payload)
        _raise_for_status(response)
        return Build.model_validate(response.json())

    async def restore_build(self, org: str, project: str, build: str) -> Build:
        """Restore a soft-deleted build.

        Undoes a ``DELETE`` while the build's object-store content is
        still there. The build's status is not changed: one deleted
        before it finished was cancelled on the way out and comes back
        ``cancelled``, because a restore returns the row and its
        content, not a re-run.

        Parameters
        ----------
        org
            Organization slug.
        project
            Project slug.
        build
            Base32 build ID, as carried by ``Build.id``.

        Returns
        -------
        Build
            The restored build.

        Raises
        ------
        DocverseClientError
            With ``status_code`` 409 if the build has been purged — its
            content permanently reclaimed, which no restore can undo —
            or 404 if there is no soft-deleted build with that ID.
        """
        url = f"/orgs/{org}/projects/{project}/builds/{build}/restore"
        response = await self._client.post(url)
        _raise_for_status(response)
        return Build.model_validate(response.json())

    async def upload_tarball(
        self, upload_url: str, tarball_path: Path
    ) -> None:
        """Upload a tarball to the presigned URL.

        Uses a separate HTTP client without auth headers to avoid leaking
        the Bearer token to the cloud storage provider.

        Parameters
        ----------
        upload_url
            Presigned upload URL from the build response.
        tarball_path
            Path to the tarball file.
        """
        async with httpx.AsyncClient(
            timeout=self._timeout,
            event_hooks=self._build_event_hooks(),
        ) as upload_client:
            with tarball_path.open("rb") as f:
                response = await upload_client.put(
                    upload_url,
                    content=f.read(),
                    headers={"Content-Type": "application/gzip"},
                )
            _raise_for_status(response)

    async def complete_upload(self, build_self_url: str) -> Build:
        """Signal that the upload is complete.

        Parameters
        ----------
        build_self_url
            The ``self_url`` from the build resource.

        Returns
        -------
        Build
            Updated build with ``job_url`` populated.
        """
        update = BuildUpdate(status=BuildStatus.uploaded)
        response = await self._client.patch(
            build_self_url,
            json=update.model_dump(exclude_none=True),
        )
        _raise_for_status(response)
        return Build.model_validate(response.json())

    async def get_job(self, job_url: str) -> QueueJob:
        """Fetch the current state of a job.

        Parameters
        ----------
        job_url
            URL to the job resource.

        Returns
        -------
        QueueJob
            Current job state.
        """
        response = await self._client.get(job_url)
        _raise_for_status(response)
        return QueueJob.model_validate(response.json())

    async def wait_for_job(self, job_url: str) -> QueueJob:
        """Poll a job until it reaches a terminal state.

        Uses exponential backoff with jitter (1 s initial, 15 s max).

        Parameters
        ----------
        job_url
            URL to the job resource.

        Returns
        -------
        QueueJob
            Completed job.

        Raises
        ------
        BuildProcessingError
            If the job reaches ``failed`` status.
        """
        delay = _BACKOFF_INITIAL
        while True:
            job = await self.get_job(job_url)
            if job.status == JobStatus.failed:
                msg = f"Build processing failed (phase={job.phase})"
                raise BuildProcessingError(msg, job=job)
            if job.status in (
                JobStatus.completed,
                JobStatus.completed_with_errors,
                JobStatus.cancelled,
            ):
                return job
            jitter = random.uniform(0, delay * 0.5)  # noqa: S311
            await asyncio.sleep(delay + jitter)
            delay = min(delay * _BACKOFF_FACTOR, _BACKOFF_MAX)


def _next_page_url(response: httpx.Response) -> str | None:
    """Return the ``Link rel="next"`` URL, or `None` on the last page."""
    return response.links.get("next", {}).get("url")


def _mask_token(value: str) -> str:
    """Mask a bearer token for safe display.

    Shows only the last 4 characters of the token value.
    """
    scheme, _, token = value.partition(" ")
    if not token:
        return "****"
    if len(token) > _TOKEN_SUFFIX_LEN:
        return f"{scheme} ****...{token[-_TOKEN_SUFFIX_LEN:]}"
    return f"{scheme} ****"


def _raise_for_status(response: httpx.Response) -> None:
    """Raise ``DocverseClientError`` for non-2xx responses."""
    if response.is_success:
        return
    try:
        detail = response.text
    except Exception:
        detail = "<no body>"
    msg = f"HTTP {response.status_code}: {detail}"
    raise DocverseClientError(msg, status_code=response.status_code)
