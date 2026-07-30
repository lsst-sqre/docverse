"""Minimal client for the LTD Keeper v1 ``/products/`` endpoint.

The legacy LTD Keeper API has no search, ``since`` filter, ETag, or
event stream, so backfill discovery just lists every product slug in
one HTTP call (~73 KB at the time of writing). Only the slug is
needed at this layer — the per-product/edition/build sync calls go
through dedicated endpoints which the deeper sync slice will own.
"""

from __future__ import annotations

from urllib.parse import urlparse

import httpx
import structlog

__all__ = ["LtdProductsClient"]


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
        """
        url = f"{self._base_url}/products/"
        response = await self._http_client.get(url)
        response.raise_for_status()
        payload = response.json()
        products = payload.get("products", [])
        slugs: list[str] = []
        seen: set[str] = set()
        for product_url in products:
            slug = _slug_from_url(product_url)
            if slug is None or slug in seen:
                continue
            seen.add(slug)
            slugs.append(slug)
        self._logger.debug(
            "Fetched LTD product slugs",
            base_url=self._base_url,
            count=len(slugs),
        )
        return slugs


def _slug_from_url(url: str) -> str | None:
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
