"""HTTP client for the legacy LTD Keeper v1 API.

A thin async wrapper over the worker's shared
:class:`httpx.AsyncClient` that translates the API's resource URLs into
typed Pydantic models, retries 429/5xx with bounded exponential
backoff, and surfaces 404 as :class:`LtdNotFoundError` so callers can
distinguish "LTD doesn't have this any more" (soft-deletion path) from
"LTD is having a bad day" (transient retry path).
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import structlog

from .models import LtdBuild, LtdEdition, LtdProduct, LtdProductsListing

__all__ = [
    "LtdClient",
    "LtdClientError",
    "LtdNotFoundError",
]

#: Maximum retry attempts for transient (429/5xx) responses, including
#: the original request (``_MAX_ATTEMPTS - 1`` retries).
_MAX_ATTEMPTS = 4

#: Initial backoff in seconds; doubles each subsequent attempt.
_BASE_BACKOFF_SECONDS = 0.5

#: Status codes that the client retries.
_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


class LtdClientError(Exception):
    """Raised when an LTD Keeper API call cannot be satisfied."""


class LtdNotFoundError(LtdClientError):
    """Raised when an LTD resource returns 404.

    Distinct from :class:`LtdClientError` so the sync engine can map an
    LTD soft-deletion onto a Docverse soft-deletion without tripping
    the generic retry/error path.
    """


class LtdClient:
    """LTD Keeper v1 API client over a shared ``httpx.AsyncClient``."""

    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient,
        base_url: str,
        logger: structlog.stdlib.BoundLogger,
        max_attempts: int = _MAX_ATTEMPTS,
        base_backoff_seconds: float = _BASE_BACKOFF_SECONDS,
    ) -> None:
        self._http_client = http_client
        self._base_url = base_url.rstrip("/")
        self._logger = logger
        self._max_attempts = max_attempts
        self._base_backoff_seconds = base_backoff_seconds

    async def list_products(self) -> LtdProductsListing:
        """Fetch ``GET /products/`` — the flat listing of product URLs."""
        payload = await self._get_json(self._url("/products/"))
        return LtdProductsListing.model_validate(payload)

    async def get_product(self, slug: str) -> LtdProduct:
        """Fetch ``GET /products/{slug}``."""
        payload = await self._get_json(self._url(f"/products/{slug}"))
        return LtdProduct.model_validate(payload)

    async def list_editions_for_product(
        self, product_slug: str
    ) -> list[LtdEdition]:
        """Fetch and follow ``GET /products/{slug}/editions/``.

        The legacy API returns ``{"editions": [<edition_url>, ...]}``;
        this helper follows each URL and validates the result so the
        caller gets a flat list of :class:`LtdEdition` models.
        """
        edition_urls = await self.list_edition_urls_for_product(product_slug)
        results: list[LtdEdition] = []
        for url in edition_urls:
            edition_payload = await self._get_json(url)
            results.append(LtdEdition.model_validate(edition_payload))
        return results

    async def list_edition_urls_for_product(
        self, product_slug: str
    ) -> list[str]:
        """Fetch only the edition URL list for a product.

        The cheap variant of :meth:`list_editions_for_product`: one HTTP
        call returns every edition's resource URL, but this helper does
        not follow them. Used by ``keeper_sync_tier_main`` where we
        want to walk the URL list looking for the ``main`` edition
        without paying a round-trip per non-``main`` edition along the
        way.
        """
        payload = await self._get_json(
            self._url(f"/products/{product_slug}/editions/")
        )
        urls = payload.get("editions", [])
        return [url for url in urls if isinstance(url, str)]

    async def get_edition_by_url(self, url: str) -> LtdEdition:
        """Fetch an edition by its full ``self_url``."""
        payload = await self._get_json(url)
        return LtdEdition.model_validate(payload)

    async def get_build_by_url(self, url: str) -> LtdBuild:
        """Fetch a build by its full ``self_url``."""
        payload = await self._get_json(url)
        return LtdBuild.model_validate(payload)

    def _url(self, path: str) -> str:
        return f"{self._base_url}{path}"

    async def _get_json(self, url: str) -> dict[str, Any]:
        """Issue a GET with retries and return the parsed JSON body.

        Raises
        ------
        LtdNotFoundError
            On HTTP 404. Not retried.
        LtdClientError
            On any other non-2xx after the retry budget is exhausted, or
            on a network error after the retry budget is exhausted.
        """
        last_exc: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = await self._http_client.get(url)
            except httpx.HTTPError as exc:
                last_exc = exc
                if attempt >= self._max_attempts:
                    msg = f"LTD GET {url} failed: {exc}"
                    raise LtdClientError(msg) from exc
                await asyncio.sleep(self._backoff_for_attempt(attempt))
                continue

            if response.status_code == httpx.codes.NOT_FOUND:
                msg = f"LTD resource {url} not found (404)"
                raise LtdNotFoundError(msg)

            if response.status_code in _RETRYABLE_STATUS_CODES:
                if attempt >= self._max_attempts:
                    msg = (
                        f"LTD GET {url} returned {response.status_code} "
                        f"after {attempt} attempts"
                    )
                    raise LtdClientError(msg)
                await asyncio.sleep(
                    self._backoff_for_response(response, attempt)
                )
                continue

            if response.is_error:
                msg = (
                    f"LTD GET {url} returned non-retryable status "
                    f"{response.status_code}"
                )
                raise LtdClientError(msg)

            payload: dict[str, Any] = response.json()
            return payload

        msg = f"LTD GET {url} exhausted retries"
        raise LtdClientError(msg) from last_exc

    def _backoff_for_attempt(self, attempt: int) -> float:
        multiplier: int = 2 ** (attempt - 1)
        return self._base_backoff_seconds * multiplier

    def _backoff_for_response(
        self, response: httpx.Response, attempt: int
    ) -> float:
        """Honour ``Retry-After`` on 429, else fall back to exp backoff."""
        retry_after = response.headers.get("Retry-After")
        if retry_after is not None:
            try:
                value = float(retry_after)
            except ValueError:
                self._logger.warning(
                    "Ignoring non-numeric Retry-After",
                    retry_after=retry_after,
                )
            else:
                return max(0.0, value)
        return self._backoff_for_attempt(attempt)
