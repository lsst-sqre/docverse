"""Minimal client for the LTD Keeper v1 ``/products/`` endpoint.

The legacy LTD Keeper API has no search, ``since`` filter, ETag, or
event stream, so backfill discovery just lists every product slug in
one HTTP call (~73 KB at the time of writing). Only the slug is
needed at this layer — the per-product/edition/build sync calls go
through dedicated endpoints which the deeper sync slice will own.

Every way this call can fail — transport, status, or a 200 whose body
is not a usable listing — surfaces as one
:class:`~docverse_server.storage.ltd.client.LtdProductsError`, so the
three call sites (run discovery, the tier crons, and the synchronous
scope-preview endpoint) share one fetch path and differ only in the
error policy they apply at the call site.
"""

from __future__ import annotations

from urllib.parse import urlparse

import httpx
import structlog

from .client import LtdProductsError

__all__ = ["LtdProductsClient"]

#: Cap on the characters of a malformed ``products`` entry quoted back
#: in an error message. The message lands in a Sentry issue title and
#: in a failing job's ``queue_jobs.errors``, neither of which should
#: carry an unbounded blob; the full body is on the exception's
#: ``body`` attribute for anyone who needs it.
_MAX_REPORTED_ENTRY_CHARS = 120


class LtdProductsClient:
    """Fetch the flat product slug list from an LTD Keeper instance."""

    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient,
        base_url: str,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._http_client = http_client
        # Strip a trailing slash so url joining is deterministic.
        self._base_url = base_url.rstrip("/")
        self._logger = logger

    async def list_product_slugs(self) -> list[str]:
        """Return every product slug visible on the LTD instance.

        The endpoint returns ``{"products": ["<base>/products/<slug>/",
        ...]}`` — slugs are extracted by parsing the path. Slugs are
        de-duplicated and returned in input order so callers can
        intersect them with an allowlist deterministically.

        Raises
        ------
        LtdProductsError
            On a transport failure, a non-2xx status, or a response
            whose body is not a usable product listing. A malformed
            body is an error rather than an empty list because every
            caller uses this listing to decide what is *in scope*:
            silently dropping products would present an LTD outage as
            "nothing to sync" — a wrong answer that looks like a right
            one.
        """
        url = f"{self._base_url}/products/"
        try:
            response = await self._http_client.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise LtdProductsError(
                url=url,
                method="GET",
                status_code=exc.response.status_code,
                body=exc.response.text,
            ) from exc
        except httpx.HTTPError as exc:
            raise LtdProductsError(
                url=url,
                method="GET",
                message=f"LTD GET {url} failed: {exc}",
            ) from exc

        slugs = _parse_product_slugs(url=url, response=response)
        self._logger.debug(
            "Fetched LTD product slugs",
            base_url=self._base_url,
            count=len(slugs),
        )
        return slugs


def _parse_product_slugs(*, url: str, response: httpx.Response) -> list[str]:
    """Extract the slug list from a 2xx ``/products/`` response.

    Validates the payload shape before trusting it. LTD answering 200
    is not a promise that the body is the listing: a proxy or
    maintenance page in front of it answers 200 with HTML, and the
    unguarded ``response.json()`` / ``payload.get(...)`` pair used to
    raise ``json.JSONDecodeError`` / ``AttributeError`` for those —
    exceptions no caller was catching (issue #675).
    """
    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise LtdProductsError(
            url=url,
            method="GET",
            status_code=status_code,
            body=response.text,
            message=(
                f"LTD GET {url} returned HTTP {status_code} with a body"
                " that is not JSON"
            ),
        ) from exc

    if not isinstance(payload, dict):
        raise _malformed(
            url=url,
            response=response,
            detail=(
                f"a JSON {type(payload).__name__} where a product"
                " listing object was expected"
            ),
        )

    products = payload.get("products", [])
    if not isinstance(products, list):
        raise _malformed(
            url=url,
            response=response,
            detail=(
                f"a 'products' field of type {type(products).__name__}"
                " where a list of product URLs was expected"
            ),
        )

    slugs: list[str] = []
    seen: set[str] = set()
    for product_url in products:
        slug = _slug_from_url(product_url)
        if slug is None:
            raise _malformed(
                url=url,
                response=response,
                detail=(
                    "a 'products' entry that is not a product URL:"
                    f" {_abbreviate(product_url)}"
                ),
            )
        if slug in seen:
            continue
        seen.add(slug)
        slugs.append(slug)
    return slugs


def _malformed(
    *, url: str, response: httpx.Response, detail: str
) -> LtdProductsError:
    """Build the malformed-listing error for a 2xx with a bad body."""
    return LtdProductsError(
        url=url,
        method="GET",
        status_code=response.status_code,
        body=response.text,
        message=(
            f"LTD GET {url} returned HTTP {response.status_code} with {detail}"
        ),
    )


def _abbreviate(value: object) -> str:
    """Render ``value`` for an error message, bounded in length."""
    text = repr(value)
    if len(text) <= _MAX_REPORTED_ENTRY_CHARS:
        return text
    return f"{text[:_MAX_REPORTED_ENTRY_CHARS]}…"


def _slug_from_url(url: object) -> str | None:
    """Extract the product slug from a ``.../products/<slug>/`` URL."""
    if not isinstance(url, str):
        return None
    path = urlparse(url).path
    parts = [p for p in path.split("/") if p]
    # The LTD API returns ``/products/<slug>/`` — the last segment is
    # the slug, the one before it is the literal ``products``. Tolerate
    # trailing slash variations.
    min_segments = 2
    if len(parts) < min_segments or parts[-2] != "products":
        return None
    return parts[-1]
