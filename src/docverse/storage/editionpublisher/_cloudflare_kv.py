"""Cloudflare Workers KV edition publisher."""

from __future__ import annotations

from types import TracebackType
from typing import Self

import httpx
import structlog

__all__ = ["CloudflareKvEditionPublisher"]

_HTTP_NOT_FOUND = 404


class CloudflareKvEditionPublisher:
    """Edition publisher that writes to a Cloudflare Workers KV namespace.

    Publishes the edition pointer by issuing a ``PUT`` against
    ``/client/v4/accounts/{account_id}/storage/kv/namespaces/``
    ``{namespace_id}/values/{project_slug}/{edition_slug}``.
    """

    def __init__(
        self,
        *,
        account_id: str,
        namespace_id: str,
        api_token: str,
        http_client: httpx.AsyncClient,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._account_id = account_id
        self._namespace_id = namespace_id
        self._api_token = api_token
        self._http_client = http_client
        self._logger = logger

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    async def publish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
        build_public_id: str,
        object_key_prefix: str,
    ) -> None:
        """Write the edition pointer to the configured KV namespace."""
        url = (
            "https://api.cloudflare.com/client/v4"
            f"/accounts/{self._account_id}"
            f"/storage/kv/namespaces/{self._namespace_id}"
            f"/values/{project_slug}/{edition_slug}"
        )
        # The Cloudflare Worker resolver reads the object-store prefix
        # from the ``r2_prefix`` KV field; see cloudflare-worker/src/
        # resolver.ts.
        response = await self._http_client.put(
            url,
            json={
                "build_id": build_public_id,
                "r2_prefix": object_key_prefix,
            },
            headers={"Authorization": f"Bearer {self._api_token}"},
        )
        if response.is_error:
            self._logger.error(
                "Cloudflare KV publish failed",
                status_code=response.status_code,
                response_body=response.text,
                project_slug=project_slug,
                edition_slug=edition_slug,
            )
        response.raise_for_status()

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
        """
        url = (
            "https://api.cloudflare.com/client/v4"
            f"/accounts/{self._account_id}"
            f"/storage/kv/namespaces/{self._namespace_id}"
            f"/values/{project_slug}/{edition_slug}"
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
        if response.is_error:
            self._logger.error(
                "Cloudflare KV unpublish failed",
                status_code=response.status_code,
                response_body=response.text,
                project_slug=project_slug,
                edition_slug=edition_slug,
            )
        response.raise_for_status()
