"""Cloudflare Workers KV edition publisher."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Any, Self

import httpx
import structlog

from docverse_server.domain.cache_profile import (
    CACHE_PROFILE_LONG,
    CACHE_PROFILE_SHORT,
    CacheProfile,
)
from docverse_server.domain.edition_pointer import (
    EditionPointer,
    edition_pointer_key,
)

from .._http_retry import (
    DEFAULT_BASE_BACKOFF_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    retry_request,
)

__all__ = ["CloudflareKvEditionPublisher"]

_HTTP_NOT_FOUND = 404

_BULK_GET_CHUNK_SIZE = 100
"""Keys per ``bulk/get`` request.

Cloudflare's own limit for the endpoint. An organization with more
editions than this is ordinary, so the read-back chunks rather than
failing — and chunking at exactly the limit keeps the number of
round-trips (and of shared API rate-limit slots consumed) minimal.
"""


def _decode_pointer(value: Any) -> EditionPointer | None:
    """Turn one decoded KV value into a pointer, or ``None``.

    ``None`` means "nothing usable is published at this key": either
    Cloudflare omitted the key (it does not exist) or its value is
    ``null``. Everything else is read leniently — a value carrying an
    unexpected shape still counts as a *present* pointer, with the
    missing fields read as empty strings.

    That leniency is deliberate in both directions the reconciler cares
    about. A tombstoned edition whose key holds a garbled value still
    needs its key deleted, which a ``None`` here would silently skip;
    and a live edition whose pointer reads as an empty build id can
    never match its real build, so it is republished. Rejecting the
    value outright would get the first case wrong to gain nothing in
    the second.
    """
    if not isinstance(value, dict):
        return None
    raw_profile = value.get("cache_profile")
    profile: CacheProfile | None = None
    if raw_profile == CACHE_PROFILE_LONG:
        profile = CACHE_PROFILE_LONG
    elif raw_profile == CACHE_PROFILE_SHORT:
        profile = CACHE_PROFILE_SHORT
    build_id = value.get("build_id")
    r2_prefix = value.get("r2_prefix")
    return EditionPointer(
        build_public_id=build_id if isinstance(build_id, str) else "",
        r2_prefix=r2_prefix if isinstance(r2_prefix, str) else "",
        cache_profile=profile,
    )


class CloudflareKvEditionPublisher:
    """Edition publisher backed by a Cloudflare Workers KV namespace.

    Publishes the edition pointer by issuing a ``PUT`` against
    ``/client/v4/accounts/{account_id}/storage/kv/namespaces/``
    ``{namespace_id}/values/{project_slug}/{edition_slug}``, removes it
    with a ``DELETE`` against the same URL, and reads pointers back in
    bulk through ``POST .../{namespace_id}/bulk/get``.

    Parameters
    ----------
    account_id
        Cloudflare account that owns the KV namespace.
    namespace_id
        KV namespace holding the edition pointers.
    api_token
        Cloudflare API token authorized to write the namespace.
    http_client
        Shared ``httpx.AsyncClient`` used to issue requests.
    logger
        Bound logger for contextual logging.
    max_attempts
        Attempts allowed for a pointer write, including the first.
        Clamped to at least 1 so a misconfigured budget degrades to
        "publish once, no retries" rather than to a silent no-op.
    base_backoff_seconds
        Delay after a pointer write's first failure; doubles each
        subsequent attempt.
    """

    def __init__(
        self,
        *,
        account_id: str,
        namespace_id: str,
        api_token: str,
        http_client: httpx.AsyncClient,
        logger: structlog.stdlib.BoundLogger,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        base_backoff_seconds: float = DEFAULT_BASE_BACKOFF_SECONDS,
    ) -> None:
        self._account_id = account_id
        self._namespace_id = namespace_id
        self._api_token = api_token
        self._http_client = http_client
        self._logger = logger
        self._max_attempts = max_attempts
        self._base_backoff_seconds = base_backoff_seconds

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    @property
    def _namespace_url(self) -> str:
        """Base URL of the configured KV namespace."""
        return (
            "https://api.cloudflare.com/client/v4"
            f"/accounts/{self._account_id}"
            f"/storage/kv/namespaces/{self._namespace_id}"
        )

    def _value_url(self, *, project_slug: str, edition_slug: str) -> str:
        """URL of one edition's KV value.

        Built from the shared key builder rather than by interpolating
        the two slugs here, so the key this publisher writes, deletes,
        and reads back can never drift from the one the reconciler asks
        about.
        """
        key = edition_pointer_key(project_slug, edition_slug)
        return f"{self._namespace_url}/values/{key}"

    async def publish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
        build_public_id: str,
        object_key_prefix: str,
        cache_profile: CacheProfile,
    ) -> None:
        """Write the edition pointer to the configured KV namespace.

        A ``429``, a transient 5xx, or a transient transport failure is
        retried with exponential backoff, honouring ``Retry-After`` when
        Cloudflare sends one (up to
        `~docverse_server.storage._http_retry.MAX_BACKOFF_SECONDS`). The
        pointer write shares Cloudflare's API rate limits with the zone
        purge and sits on the critical path of every publish, so a
        single 429 used to abandon an otherwise healthy publish and
        leave the edition pointing at its previous build.

        Raises
        ------
        httpx.HTTPStatusError
            If Cloudflare answers with a non-retryable non-2xx status —
            including a 3xx redirect, which this client does not follow,
            so the pointer was not written — or keeps answering with a
            retryable one until the attempt budget is exhausted. The
            failure is logged at ``ERROR`` with the status code and
            response body first.
        httpx.TransportError
            If the transport keeps failing until the attempt budget is
            exhausted, or fails in a way a retry cannot fix.
        """
        url = self._value_url(
            project_slug=project_slug, edition_slug=edition_slug
        )
        logger = self._logger.bind(
            project_slug=project_slug, edition_slug=edition_slug
        )

        async def send() -> httpx.Response:
            # The Cloudflare Worker resolver reads the object-store
            # prefix from the ``r2_prefix`` KV field and the edge cache
            # policy from ``cache_profile``; see
            # cloudflare-worker/src/resolver.ts. The ``cache_profile``
            # field is additive — a Worker that predates it falls back to
            # the short profile.
            return await self._http_client.put(
                url,
                json={
                    "build_id": build_public_id,
                    "r2_prefix": object_key_prefix,
                    "cache_profile": cache_profile,
                },
                headers={"Authorization": f"Bearer {self._api_token}"},
            )

        outcome = await retry_request(
            send,
            operation="Cloudflare KV publish",
            logger=logger,
            max_attempts=self._max_attempts,
            base_backoff_seconds=self._base_backoff_seconds,
        )
        # Only a 2xx means Cloudflare stored the pointer. A 3xx is not
        # followed on this client, so gating the diagnostic on
        # ``is_error`` (4xx/5xx only) let a redirect raise below with no
        # status or body for the triager to read.
        if not outcome.response.is_success:
            logger.error(
                "Cloudflare KV publish failed",
                status_code=outcome.response.status_code,
                response_body=outcome.response.text,
                attempts=outcome.attempts,
                retryable=outcome.retryable,
            )
        outcome.response.raise_for_status()

    async def unpublish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
    ) -> None:
        """Remove the edition pointer from the configured KV namespace.

        A 404 from Cloudflare is treated as a successful no-op so the
        operation is idempotent — soft-deleting an edition whose pointer
        was never published, or running cleanup twice, must not surface
        as a failure to the caller.

        Raises
        ------
        httpx.HTTPStatusError
            If Cloudflare answers with any other non-2xx status,
            including a 3xx redirect (this client does not follow
            redirects, so the pointer is still in place). The failure is
            logged at ``ERROR`` with the status code and response body
            first.
        """
        url = self._value_url(
            project_slug=project_slug, edition_slug=edition_slug
        )
        response = await self._http_client.delete(
            url,
            headers={"Authorization": f"Bearer {self._api_token}"},
        )
        if response.status_code == _HTTP_NOT_FOUND:
            self._logger.info(
                "Cloudflare KV unpublish: key not found (idempotent)",
                project_slug=project_slug,
                edition_slug=edition_slug,
            )
            return
        # The 404 above is the one non-2xx that means success. Every
        # other non-2xx — including an unfollowed 3xx — left the pointer
        # in place, so it is logged with its context before raising.
        if not response.is_success:
            self._logger.error(
                "Cloudflare KV unpublish failed",
                status_code=response.status_code,
                response_body=response.text,
                project_slug=project_slug,
                edition_slug=edition_slug,
            )
        response.raise_for_status()

    async def get_pointers(
        self, keys: Sequence[str]
    ) -> Mapping[str, EditionPointer | None]:
        """Read the pointers Cloudflare currently serves for ``keys``.

        Issues ``POST .../bulk/get`` with ``{"keys": [...], "type":
        "json"}`` in chunks of `_BULK_GET_CHUNK_SIZE`, so reconciling an
        organization costs one round-trip per hundred editions rather
        than one per edition. Every requested key appears in the result:
        a key Cloudflare omits (it does not exist) and one whose value
        is ``null`` both map to ``None``, because the reconciler's
        question is "is a usable pointer published here?" and both
        answers are no.

        A ``429``, a transient 5xx, or a transient transport failure is
        retried with the same backoff policy the pointer write uses.
        Unlike the write, this call is off the publish critical path, so
        a chunk that fails takes the whole read with it rather than
        returning a half-read view — a missing pointer is exactly the
        signal that triggers a republish, and inventing one from a
        failed HTTP call would re-drive publishes the edge never lost.

        Parameters
        ----------
        keys
            CDN keys to read, as built by
            `~docverse_server.domain.edition_pointer.edition_pointer_key`.

        Returns
        -------
        Mapping
            One entry per requested key, in request order, mapping to
            the decoded pointer or to ``None``.

        Raises
        ------
        httpx.HTTPStatusError
            If Cloudflare answers a chunk with a non-2xx status that a
            retry cannot fix, or keeps answering with a retryable one
            until the attempt budget is exhausted. The failure is logged
            at ``ERROR`` with the status code and response body first.
        httpx.TransportError
            If the transport keeps failing until the attempt budget is
            exhausted, or fails in a way a retry cannot fix.
        """
        pointers: dict[str, EditionPointer | None] = {}
        for start in range(0, len(keys), _BULK_GET_CHUNK_SIZE):
            chunk = list(keys[start : start + _BULK_GET_CHUNK_SIZE])
            values = await self._read_chunk(chunk)
            for key in chunk:
                pointers[key] = _decode_pointer(values.get(key))
        return pointers

    async def _read_chunk(self, chunk: Sequence[str]) -> Mapping[str, Any]:
        """Read one chunk of keys, returning Cloudflare's value map."""
        url = f"{self._namespace_url}/bulk/get"
        logger = self._logger.bind(key_count=len(chunk))

        async def send() -> httpx.Response:
            return await self._http_client.post(
                url,
                json={"keys": list(chunk), "type": "json"},
                headers={"Authorization": f"Bearer {self._api_token}"},
            )

        outcome = await retry_request(
            send,
            operation="Cloudflare KV bulk read",
            logger=logger,
            max_attempts=self._max_attempts,
            base_backoff_seconds=self._base_backoff_seconds,
        )
        if not outcome.response.is_success:
            logger.error(
                "Cloudflare KV bulk read failed",
                status_code=outcome.response.status_code,
                response_body=outcome.response.text,
                attempts=outcome.attempts,
                retryable=outcome.retryable,
            )
        outcome.response.raise_for_status()

        payload = outcome.response.json()
        result = payload.get("result") if isinstance(payload, dict) else None
        values = result.get("values") if isinstance(result, dict) else None
        return values if isinstance(values, dict) else {}
