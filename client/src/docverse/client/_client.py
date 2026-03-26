"""Async HTTP client for the Docverse API."""

from __future__ import annotations

import asyncio
import random
from pathlib import Path
from typing import Any

import click
import httpx

from ._exceptions import BuildProcessingError, DocverseClientError
from .models import Build, BuildStatus, BuildUpdate, QueueJob
from .models.queue_enums import JobStatus

__all__ = ["DocverseClient"]

_BACKOFF_INITIAL = 1.0
_BACKOFF_MAX = 15.0
_BACKOFF_FACTOR = 2.0
_VERBOSE_BODY_MAX = 2000
_TOKEN_SUFFIX_LEN = 4


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

    async def create_build(  # noqa: PLR0913
        self,
        org: str,
        project: str,
        *,
        git_ref: str,
        content_hash: str,
        alternate_name: str | None = None,
        annotations: dict[str, Any] | None = None,
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
            SHA-256 hash of the tarball (``sha256:<hex>``).
        alternate_name
            Optional alternate deployment name.
        annotations
            Optional metadata annotations.

        Returns
        -------
        Build
            The created build, including ``upload_url``.
        """
        payload: dict[str, Any] = {
            "git_ref": git_ref,
            "content_hash": content_hash,
        }
        if alternate_name is not None:
            payload["alternate_name"] = alternate_name
        if annotations is not None:
            payload["annotations"] = annotations

        url = f"/orgs/{org}/projects/{project}/builds"
        response = await self._client.post(url, json=payload)
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
            Updated build with ``queue_url`` populated.
        """
        update = BuildUpdate(status=BuildStatus.uploaded)
        response = await self._client.patch(
            build_self_url,
            json=update.model_dump(exclude_none=True),
        )
        _raise_for_status(response)
        return Build.model_validate(response.json())

    async def get_queue_job(self, queue_url: str) -> QueueJob:
        """Fetch the current state of a queue job.

        Parameters
        ----------
        queue_url
            URL to the queue job resource.

        Returns
        -------
        QueueJob
            Current job state.
        """
        response = await self._client.get(queue_url)
        _raise_for_status(response)
        return QueueJob.model_validate(response.json())

    async def wait_for_job(self, queue_url: str) -> QueueJob:
        """Poll a queue job until it reaches a terminal state.

        Uses exponential backoff with jitter (1 s initial, 15 s max).

        Parameters
        ----------
        queue_url
            URL to the queue job resource.

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
            job = await self.get_queue_job(queue_url)
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
    except Exception:  # noqa: BLE001
        detail = "<no body>"
    msg = f"HTTP {response.status_code}: {detail}"
    raise DocverseClientError(msg, status_code=response.status_code)
