"""Service for publishing editions to the CDN."""

from __future__ import annotations

from typing import Protocol

import structlog

from docverse.client.models.queue_enums import PublishStatus
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.edition_build_history import EditionBuildHistory
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.editionpublisher import EditionPublisher
from docverse.storage.organization_store import OrganizationStore

__all__ = ["EditionPublisherProvider", "EditionPublishingService"]


class EditionPublisherProvider(Protocol):
    """Callable that resolves an ``EditionPublisher`` for an org."""

    async def __call__(
        self, *, org_id: int, service_label: str
    ) -> EditionPublisher:
        """Return an unopened ``EditionPublisher`` for the org."""
        ...


class EditionPublishingService:
    """Orchestrate CDN publishing for an edition.

    Bridges the ``EditionPublisher`` storage layer and the edition /
    history stores. Handlers and workers own the transaction — this
    service never calls ``commit()`` or ``flush()`` directly on the
    session (its store collaborators may ``flush()`` to obtain
    database-generated values but will not ``commit()``).
    """

    def __init__(
        self,
        *,
        org_store: OrganizationStore,
        edition_store: EditionStore,
        history_store: EditionBuildHistoryStore,
        publisher_provider: EditionPublisherProvider,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._org_store = org_store
        self._edition_store = edition_store
        self._history_store = history_store
        self._publisher_provider = publisher_provider
        self._logger = logger

    async def publish(
        self,
        *,
        org_id: int,
        project_slug: str,
        edition: Edition,
        build: Build,
        history_entry: EditionBuildHistory,
    ) -> None:
        """Publish an edition pointer via the org's configured CDN.

        If the organization has no ``cdn_service_label`` configured,
        the edition and history entry are marked as ``published``
        without invoking any publisher. Otherwise an
        ``EditionPublisher`` is resolved via ``publisher_provider`` and
        used as an async context manager to publish the pointer.

        On a successful publish both the edition row and the supplied
        history entry are updated to ``PublishStatus.published``. When
        the publisher raises, the exception propagates — the caller is
        responsible for marking the rows ``failed``.

        Raises
        ------
        RuntimeError
            If the organization cannot be found.
        """
        org = await self._org_store.get_by_id(org_id)
        if org is None:
            msg = f"Organization id={org_id} not found"
            raise RuntimeError(msg)

        if org.cdn_service_label is None:
            self._logger.info(
                "Edition published without CDN (no cdn_service_label)",
                org_id=org_id,
                project_slug=project_slug,
                edition_slug=edition.slug,
                build_id=build.id,
            )
            await self._mark_published(
                edition_id=edition.id, history_id=history_entry.id
            )
            return

        publisher = await self._publisher_provider(
            org_id=org_id, service_label=org.cdn_service_label
        )
        async with publisher:
            await publisher.publish(
                project_slug=project_slug,
                edition_slug=edition.slug,
                build_public_id=serialize_base32_id(build.public_id),
                object_key_prefix=build.storage_prefix,
            )
        await self._mark_published(
            edition_id=edition.id, history_id=history_entry.id
        )
        self._logger.info(
            "Published edition",
            org_id=org_id,
            project_slug=project_slug,
            edition_slug=edition.slug,
            build_id=build.id,
            cdn_service_label=org.cdn_service_label,
        )

    async def unpublish(
        self,
        *,
        org_id: int,
        project_slug: str,
        edition_slug: str,
    ) -> None:
        """Remove an edition pointer via the org's configured CDN.

        Mirrors :meth:`publish`'s resolver: loads the org, checks for a
        configured ``cdn_service_label``, resolves the publisher via
        ``publisher_provider``, and calls ``unpublish`` inside its async
        context. If the org has no CDN configured the call is a no-op so
        callers can invoke ``unpublish`` unconditionally without first
        inspecting the org row.

        The underlying publisher's ``unpublish`` is required to be
        idempotent (e.g. Cloudflare KV treats a 404 as success), so this
        method is safe to call against editions that were never
        published.

        Raises
        ------
        RuntimeError
            If the organization cannot be found.
        """
        org = await self._org_store.get_by_id(org_id)
        if org is None:
            msg = f"Organization id={org_id} not found"
            raise RuntimeError(msg)

        if org.cdn_service_label is None:
            self._logger.info(
                "Edition unpublish skipped (no cdn_service_label)",
                org_id=org_id,
                project_slug=project_slug,
                edition_slug=edition_slug,
            )
            return

        publisher = await self._publisher_provider(
            org_id=org_id, service_label=org.cdn_service_label
        )
        async with publisher:
            await publisher.unpublish(
                project_slug=project_slug,
                edition_slug=edition_slug,
            )
        self._logger.info(
            "Unpublished edition",
            org_id=org_id,
            project_slug=project_slug,
            edition_slug=edition_slug,
            cdn_service_label=org.cdn_service_label,
        )

    async def _mark_published(
        self, *, edition_id: int, history_id: int
    ) -> None:
        await self._edition_store.set_publish_status(
            edition_id=edition_id, status=PublishStatus.published
        )
        await self._history_store.set_publish_status(
            history_id=history_id, status=PublishStatus.published
        )
