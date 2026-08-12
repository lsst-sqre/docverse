"""Service for publishing editions to the CDN."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import sentry_sdk
import structlog

from docverse.models.queue_enums import PublishStatus
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.build import Build
from docverse_server.domain.cache_profile import (
    CACHE_PROFILE_LONG,
    compute_cache_profile,
)
from docverse_server.domain.edition import Edition
from docverse_server.domain.edition_build_history import EditionBuildHistory
from docverse_server.domain.organization import Organization
from docverse_server.domain.published_url import compute_project_hostname
from docverse_server.services.cdn_purge_coalescer import CdnPurgeCoalescer
from docverse_server.storage.cdncachepurger import CdnCachePurger
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.editionpublisher import EditionPublisher
from docverse_server.storage.organization_store import OrganizationStore

__all__ = [
    "CdnCachePurgerProvider",
    "EditionPublisherProvider",
    "EditionPublishingService",
    "PendingCdnPurge",
]


_PURGE_FAILURE_EVENT = "CDN cache purge failed"
"""Log event for a swallowed CDN purge failure, at either purge stage.

Purging is best-effort, but swallowed is not the same as dropped: a
purge that Cloudflare keeps rejecting leaves the edition serving a stale
copy at the edge until some later publish happens to purge the same
hostname, which is exactly the kind of failure that must not live only
in worker logs. So both places that swallow one — resolving the purger
during the publish transaction, and running the purge afterwards —
capture the exception to Sentry alongside this ERROR, carrying whatever
the purger knew (Cloudflare's ``errors[].code``, the status, the attempt
count) via its ``to_sentry`` override.

The capture is explicit rather than relying on the SDK's logging
integration to turn the ERROR into an event: the capture is the contract
here, not a side effect of how logging happens to be wired.
``DedupeIntegration`` (a default ``sentry_sdk.init`` integration) drops
the logging-integration capture of the same instance, so each failure
reports once — which is also why the ``capture_exception`` /
``logger.exception`` pair is spelled out inside each ``except`` block
rather than factored into a helper: ``logger.exception`` reads the
ambient exception context, and only that form feeds the dedupe.
"""


@dataclass(frozen=True, slots=True)
class PendingCdnPurge:
    """A CDN cache purge deferred until after the publish transaction.

    :meth:`EditionPublishingService.publish` returns one of these
    instead of purging inline, because the purge is the one step of a
    publish that can block for a long time: it queues behind the
    process-wide per-hostname
    `~docverse_server.services.cdn_purge_coalescer.CdnPurgeCoalescer`
    and then, inside the purger, sleeps out Cloudflare's rate-limit
    backoff. Holding the caller's transaction open across that wait
    pins an idle-in-transaction connection per waiter, which is enough
    to exhaust the async engine pool during a same-hostname publish
    burst (a keeper-sync backfill) and fail unrelated worker jobs.

    Everything the purge needs — including the resolved
    `~docverse_server.storage.cdncachepurger.CdnCachePurger`, whose
    construction reads the org's CDN credentials from the database — is
    captured while the publish transaction is still open, so
    :meth:`EditionPublishingService.purge_cdn_cache` touches no database
    at all and can safely run with no transaction in flight.
    """

    hostname: str
    """Project hostname the purge invalidates, and its coalescing key."""

    purger: CdnCachePurger
    """Unopened purger, resolved during the publish transaction."""

    org_id: int
    """Organization that owns the published project."""

    project_slug: str
    """Slug of the project whose hostname is purged."""

    edition_slug: str
    """Slug of the edition whose publish triggered the purge."""

    build_id: int
    """Build that the edition pointer was moved to."""

    cdn_service_label: str
    """Service label the purger's credentials were resolved from."""


class EditionPublisherProvider(Protocol):
    """Callable that resolves an ``EditionPublisher`` for an org."""

    async def __call__(
        self, *, org_id: int, service_label: str
    ) -> EditionPublisher:
        """Return an unopened ``EditionPublisher`` for the org."""
        ...


class CdnCachePurgerProvider(Protocol):
    """Callable that resolves a ``CdnCachePurger`` for an org."""

    async def __call__(
        self, *, org_id: int, service_label: str
    ) -> CdnCachePurger:
        """Return an unopened ``CdnCachePurger`` for the org."""
        ...


class EditionPublishingService:
    """Orchestrate CDN publishing for an edition.

    Bridges the ``EditionPublisher`` storage layer and the edition /
    history stores. Handlers and workers own the transaction — this
    service never calls ``commit()`` or ``flush()`` directly on the
    session (its store collaborators may ``flush()`` to obtain
    database-generated values but will not ``commit()``).

    Publishing is deliberately split in two so that ownership stays
    workable: :meth:`publish` does every database-touching step and runs
    inside the caller's transaction, then hands back a
    :class:`PendingCdnPurge` for :meth:`purge_cdn_cache` to run once
    that transaction has committed. The purge is the one step that can
    block for tens of seconds, and it must not do so on a connection.
    """

    def __init__(
        self,
        *,
        org_store: OrganizationStore,
        edition_store: EditionStore,
        history_store: EditionBuildHistoryStore,
        publisher_provider: EditionPublisherProvider,
        purger_provider: CdnCachePurgerProvider,
        purge_coalescer: CdnPurgeCoalescer,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._org_store = org_store
        self._edition_store = edition_store
        self._history_store = history_store
        self._publisher_provider = publisher_provider
        self._purger_provider = purger_provider
        self._purge_coalescer = purge_coalescer
        self._logger = logger

    async def publish(
        self,
        *,
        org_id: int,
        project_slug: str,
        edition: Edition,
        build: Build,
        history_entry: EditionBuildHistory,
    ) -> PendingCdnPurge | None:
        """Publish an edition pointer via the org's configured CDN.

        If the organization has no ``cdn_service_label`` configured,
        the edition and history entry are marked as ``published``
        without invoking any publisher. Otherwise an
        ``EditionPublisher`` is resolved via ``publisher_provider`` and
        used as an async context manager to publish the pointer.

        The edge cache profile written with the pointer is derived from
        the edition's ``kind`` (see
        `docverse_server.domain.cache_profile.compute_cache_profile`).
        Long-profile editions are cached at the CDN edge for far longer
        than the publish cadence, so a long-profile publish also needs
        the project's hostname purged from the CDN cache. Short-profile
        editions expire quickly on their own and are never purged.

        This method does **not** run that purge. It returns a
        :class:`PendingCdnPurge` describing it, and the caller is
        responsible for awaiting
        :meth:`purge_cdn_cache` **after** committing — see that method
        and :class:`PendingCdnPurge` for why the purge must not run
        inside the publish transaction. `None` is returned when no
        purge is warranted (no CDN configured, a short cache profile,
        or the purger could not be resolved).

        On a successful publish both the edition row and the supplied
        history entry are updated to ``PublishStatus.published``. When
        the publisher raises, the exception propagates — the caller is
        responsible for marking the rows ``failed``.

        Returns
        -------
        PendingCdnPurge or None
            The purge to run once the caller's transaction has
            committed, or `None` when nothing needs purging.

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
            return None

        cache_profile = compute_cache_profile(edition.kind)
        publisher = await self._publisher_provider(
            org_id=org_id, service_label=org.cdn_service_label
        )
        async with publisher:
            await publisher.publish(
                project_slug=project_slug,
                edition_slug=edition.slug,
                build_public_id=serialize_base32_id(build.public_id),
                object_key_prefix=build.storage_prefix,
                cache_profile=cache_profile,
            )
        pending_purge: PendingCdnPurge | None = None
        if cache_profile == CACHE_PROFILE_LONG:
            pending_purge = await self._prepare_cdn_purge(
                org=org,
                service_label=org.cdn_service_label,
                project_slug=project_slug,
                edition=edition,
                build=build,
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
            cache_profile=cache_profile,
        )
        return pending_purge

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

    async def purge_cdn_cache(self, pending: PendingCdnPurge) -> None:
        """Purge a published project's hostname from the CDN edge cache.

        **Call this with no database transaction open**, after the
        publish that produced ``pending`` has committed. Nothing here
        touches the database — the purger was resolved during the
        publish transaction — precisely so the two waits below happen on
        no connection at all:

        * the coalescer's per-hostname lock, which serializes every
          concurrent publish of the same project and then sleeps out the
          remainder of the throttle interval, and
        * the purger's own retry backoff, which can sleep tens of
          seconds when Cloudflare answers a purge with 429.

        The purge goes through the process-wide
        `~docverse_server.services.cdn_purge_coalescer.CdnPurgeCoalescer`
        rather than straight to the purger. Purging is hostname-scoped,
        so every edition of a project emits a byte-identical call and a
        publish burst (a release plus its semver aggregates, or a
        keeper-sync backfill) is almost entirely redundant. The
        coalescer folds the burst into a throttled sequence while
        preserving the happens-after invariant: the purge that marks
        this request served is guaranteed to have started after the
        pointer write that produced ``pending``.

        The purge fires as soon as the caller commits. Workers KV is
        eventually consistent, so for a short window afterwards an edge
        colo can still read the previous pointer and re-populate its
        cache with the old build under the long profile, where it lives
        until the next publish purges it. That window is still accepted:
        closing it needs a *delayed second* purge, i.e. the job-queue
        scheduling machinery PRD #183 put out of scope. Retrying a purge
        that Cloudflare rejected is a different problem and is handled
        inside `CloudflareCachePurger`.

        Best-effort: any failure invoking the purger is reported and
        swallowed, so a CDN outage degrades to a stale edge copy instead
        of a failed publish — and because the publish already committed,
        a purge failure cannot roll it back. Every outcome is logged —
        purged, coalesced, or failed.
        """
        logger = self._bind_purge_logger(
            org_id=pending.org_id,
            project_slug=pending.project_slug,
            edition_slug=pending.edition_slug,
            build_id=pending.build_id,
            service_label=pending.cdn_service_label,
            hostname=pending.hostname,
        )

        async def purge_hostname() -> None:
            async with pending.purger:
                await pending.purger.purge_hostname(pending.hostname)

        try:
            purged = await self._purge_coalescer.purge(
                pending.hostname, purge_hostname
            )
        except Exception as exc:
            # See ``_PURGE_FAILURE_EVENT`` for why the Sentry capture is
            # explicit and why the pair is spelled out at each raise
            # site instead of being factored into a helper.
            sentry_sdk.capture_exception(exc)
            logger.exception(_PURGE_FAILURE_EVENT)
        else:
            if purged:
                logger.info("Purged CDN cache")
            else:
                logger.info("Coalesced CDN cache purge")

    async def _prepare_cdn_purge(
        self,
        *,
        org: Organization,
        service_label: str,
        project_slug: str,
        edition: Edition,
        build: Build,
    ) -> PendingCdnPurge | None:
        """Resolve everything :meth:`purge_cdn_cache` needs.

        Runs inside the caller's publish transaction, because resolving
        the purger reads the org's CDN credentials from the database.
        Doing it here — rather than lazily inside the coalesced callback,
        where an absorbed request would skip it — is what lets the purge
        itself run with no transaction open. The extra read costs one
        query per long-profile publish, alongside the identical one the
        `EditionPublisher` already makes.

        Returns `None` when the purger cannot be resolved: resolution is
        as best-effort as the purge it feeds, so a misconfigured CDN
        service degrades to a stale edge copy rather than failing a
        publish that has otherwise succeeded.
        """
        hostname = compute_project_hostname(org, project_slug)
        try:
            purger = await self._purger_provider(
                org_id=org.id, service_label=service_label
            )
        except Exception as exc:
            logger = self._bind_purge_logger(
                org_id=org.id,
                project_slug=project_slug,
                edition_slug=edition.slug,
                build_id=build.id,
                service_label=service_label,
                hostname=hostname,
            )
            sentry_sdk.capture_exception(exc)
            logger.exception(_PURGE_FAILURE_EVENT)
            return None
        return PendingCdnPurge(
            hostname=hostname,
            purger=purger,
            org_id=org.id,
            project_slug=project_slug,
            edition_slug=edition.slug,
            build_id=build.id,
            cdn_service_label=service_label,
        )

    def _bind_purge_logger(
        self,
        *,
        org_id: int,
        project_slug: str,
        edition_slug: str,
        build_id: int,
        service_label: str,
        hostname: str,
    ) -> structlog.stdlib.BoundLogger:
        """Bind the shared context every purge log record carries."""
        return self._logger.bind(
            org_id=org_id,
            project_slug=project_slug,
            edition_slug=edition_slug,
            build_id=build_id,
            cdn_service_label=service_label,
            hostname=hostname,
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
