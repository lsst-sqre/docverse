"""Cloudflare Workers KV edition publisher."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Any, Literal, Self, override

import httpx
import structlog
from safir.slack.sentry import SentryEventInfo

from docverse_server.domain.cache_profile import (
    CACHE_PROFILE_LONG,
    CACHE_PROFILE_SHORT,
    CacheProfile,
)
from docverse_server.domain.edition_pointer import (
    EditionPointer,
    edition_pointer_key,
)
from docverse_server.exceptions import DocverseSlackException

from .._http_retry import (
    DEFAULT_BASE_BACKOFF_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    retry_request,
)

__all__ = [
    "CloudflareKvEditionPublisher",
    "CloudflareKvReadError",
    "MalformedKvReadField",
]

_HTTP_NOT_FOUND = 404

_MAX_BODY_BYTES = 4 * 1024
"""Cap on the response-body bytes a malformed-read event carries.

A 2xx that is not the documented shape can be anything — a proxy's HTML
maintenance page, a full JSON payload — so the snippet Sentry keeps is
truncated at the constructor rather than left for Sentry to cut. Four
KiB is the same bar the LTD client sets, and is enough for the leading
"what did Cloudflare actually say" fragment.
"""

MalformedKvReadField = Literal["body", "result", "result.values"]
"""Which part of a ``bulk/get`` response failed to match the contract.

Low cardinality by construction — there are exactly three places the
documented ``{"result": {"values": {...}}}`` shape can break — so it is
worth a Sentry tag: "Cloudflare stopped sending ``values``" and "a proxy
replaced the body with HTML" are different outages with different
owners, and the tag is what tells them apart at a glance.
"""

_BULK_GET_CHUNK_SIZE = 100
"""Keys per ``bulk/get`` request.

Cloudflare's own limit for the endpoint. An organization with more
editions than this is ordinary, so the read-back chunks rather than
failing — and chunking at exactly the limit keeps the number of
round-trips (and of shared API rate-limit slots consumed) minimal.
"""


def _truncate_body(body: str) -> str:
    """Cap ``body`` at :data:`_MAX_BODY_BYTES` UTF-8 bytes.

    Truncation is measured in bytes rather than characters so the cap
    holds for a multi-byte body; ``errors="ignore"`` drops any trailing
    partial code point so the result is always well-formed text.
    """
    encoded = body.encode("utf-8")
    if len(encoded) <= _MAX_BODY_BYTES:
        return body
    return encoded[:_MAX_BODY_BYTES].decode("utf-8", errors="ignore")


class CloudflareKvReadError(DocverseSlackException):
    """Raised when a 2xx ``bulk/get`` body is not the documented shape.

    The read-back is the reconciler's only window onto the edge, and the
    reconciler acts on absence: a key with no pointer is exactly what
    makes it re-drive a publish. So a body this client cannot read is not
    "no keys are published" — it is "the question was not answered", and
    the two must never collapse. Reading a malformed body as an empty
    map reported every ``published`` edition as ``pointer_missing`` and
    re-drove up to a whole org's publishes every tick, silently, for as
    long as Cloudflare kept answering in that shape.

    The override earns its keep the same way
    :class:`~docverse_server.storage.cdncachepurger.CloudflareCachePurgeError`'s
    does: every malformed read raises this one type from this one call
    site, so the stack trace tells a triager nothing they do not already
    know. What they need is which namespace answered, how much of the
    read it took down, and *how* the body broke — a missing ``values``
    map is a Cloudflare API change, a non-JSON body is something in
    front of Cloudflare — which are the three tags. The body itself is
    unbounded, so it rides in the context (truncated here, not by
    Sentry) and stays out of ``str(exc)``, which becomes the failing
    job's ``errors['message']``, the Sentry issue title, and the Slack
    message.

    Parameters
    ----------
    namespace_id
        KV namespace the malformed answer came from.
    key_count
        Keys in the chunk whose read failed, so the event says how much
        of the org's read-back this took down.
    missing_field
        Which part of the documented shape was absent or the wrong type.
    response_body
        Cloudflare's body, truncated to :data:`_MAX_BODY_BYTES`.
    message
        Overrides the rendered default when the caller wants its own.
    """

    def __init__(
        self,
        *,
        namespace_id: str | None = None,
        key_count: int = 0,
        missing_field: MalformedKvReadField = "body",
        response_body: str | None = None,
        message: str | None = None,
    ) -> None:
        if message is None:
            message = self._format_message(
                key_count=key_count, missing_field=missing_field
            )
        super().__init__(message)
        self.namespace_id = namespace_id
        self.key_count = key_count
        self.missing_field = missing_field
        self.response_body = (
            _truncate_body(response_body)
            if response_body is not None
            else None
        )

    @override
    def to_sentry(self) -> SentryEventInfo:
        info = super().to_sentry()
        if self.namespace_id is not None:
            info.tags["kv_namespace_id"] = self.namespace_id
        info.tags["kv_key_count"] = str(self.key_count)
        info.tags["kv_missing_field"] = self.missing_field
        context: dict[str, Any] = {
            "namespace_id": self.namespace_id,
            "key_count": self.key_count,
            "missing_field": self.missing_field,
            "response_body": self.response_body,
        }
        info.contexts["cloudflare_kv_read"] = context
        return info

    @staticmethod
    def _format_message(
        *, key_count: int, missing_field: MalformedKvReadField
    ) -> str:
        plural = "key" if key_count == 1 else "keys"
        what = (
            "the response body" if missing_field == "body" else missing_field
        )
        return (
            f"Cloudflare KV bulk read of {key_count} {plural} returned a "
            f"malformed body: {what} is missing or not a JSON object"
        )


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

    So a value that is not an object at all — a string, a number, a
    list, a boolean, from a hand edit, a legacy format, or a partial
    write — reads as a present pointer with no fields, not as an absent
    one. It is the same claim a ``{}`` makes, and it is the truthful
    one: the key exists, and the reconciler's unpublish leg is what
    removes it.
    """
    if value is None:
        return None
    # Every non-object value falls through the field reads below and
    # lands on the empty-string defaults, which is the whole point: the
    # key's presence is what the caller asked about, and its unreadable
    # contents can only ever fail the comparison against the database.
    fields: Mapping[str, Any] = value if isinstance(value, dict) else {}
    raw_profile = fields.get("cache_profile")
    profile: CacheProfile | None = None
    if raw_profile == CACHE_PROFILE_LONG:
        profile = CACHE_PROFILE_LONG
    elif raw_profile == CACHE_PROFILE_SHORT:
        profile = CACHE_PROFILE_SHORT
    build_id = fields.get("build_id")
    r2_prefix = fields.get("r2_prefix")
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
        CloudflareKvReadError
            If a chunk's 2xx body is not a JSON object, carries no
            ``result`` object, or carries no ``result.values`` object.
            Such a body is a failed read, not an edge with no pointers.
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
        """Read one chunk of keys, returning Cloudflare's value map.

        Raises
        ------
        CloudflareKvReadError
            If the 2xx body is not the documented
            ``{"result": {"values": {...}}}`` shape.
        """
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

        # Every departure from ``{"result": {"values": {...}}}`` is a
        # failure, never an empty map: see CloudflareKvReadError for why
        # "the edge has no pointers" is the one answer this must not
        # invent.
        try:
            payload = outcome.response.json()
        except ValueError:
            raise self._malformed(chunk, "body", outcome.response) from None
        if not isinstance(payload, dict):
            raise self._malformed(chunk, "body", outcome.response)
        result = payload.get("result")
        if not isinstance(result, dict):
            raise self._malformed(chunk, "result", outcome.response)
        values = result.get("values")
        if not isinstance(values, dict):
            raise self._malformed(chunk, "result.values", outcome.response)
        return values

    def _malformed(
        self,
        chunk: Sequence[str],
        missing_field: MalformedKvReadField,
        response: httpx.Response,
    ) -> CloudflareKvReadError:
        """Log a malformed ``bulk/get`` answer and build its exception.

        Logged here rather than at the raise sites so the body reaches
        the pod logs exactly once however the shape broke, and beside
        the ``ERROR`` a non-2xx already writes — a 200 that cannot be
        read is the same operational event as a 500.
        """
        self._logger.error(
            "Cloudflare KV bulk read returned a malformed body",
            key_count=len(chunk),
            missing_field=missing_field,
            status_code=response.status_code,
            response_body=response.text,
        )
        return CloudflareKvReadError(
            namespace_id=self._namespace_id,
            key_count=len(chunk),
            missing_field=missing_field,
            response_body=response.text,
        )
