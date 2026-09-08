"""Planning and reclamation for the ``purgatory_cleanup`` sweep."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import structlog

from docverse_server.domain.build import Build
from docverse_server.domain.organization import Organization
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.objectstore import ObjectStore

__all__ = [
    "PurgatoryPlan",
    "PurgatoryService",
    "PurgeOutcome",
    "ReferencedBuild",
]


@dataclass(frozen=True, slots=True, kw_only=True)
class ReferencedBuild:
    """An expired build a live edition is still serving.

    Carries the edition slugs alongside the build because a skipped
    build is only actionable if the operator can see what is pinning it:
    the slugs are what tell them which edition to roll back before the
    next tick can reclaim the bytes.
    """

    build: Build
    """The build the sweep is holding back."""

    edition_slugs: tuple[str, ...]
    """Slugs of the live editions whose current build this is, sorted."""


@dataclass(frozen=True, slots=True, kw_only=True)
class PurgatoryPlan:
    """One organization's work for a single ``purgatory_cleanup`` tick.

    The partition is the point: everything in ``purgeable`` is a build
    whose objects the job may take, and everything in ``referenced`` is
    one it must leave alone and report. Both are already bounded by the
    per-job cap, so a plan is what one job will actually attempt rather
    than the org's whole backlog.
    """

    purgeable: tuple[Build, ...]
    """Builds to reclaim, oldest ``date_deleted`` first."""

    referenced: tuple[ReferencedBuild, ...]
    """Builds held back because a live edition still serves them."""


@dataclass(frozen=True, slots=True, kw_only=True)
class PurgeOutcome:
    """What one build's reclamation removed from the object store."""

    objects_deleted: int
    """Objects removed from under the build's ``storage_prefix``."""

    bytes_reclaimed: int
    """Bytes the build recorded occupying, or 0 if it never recorded any."""


class PurgatoryService:
    """Decide what the purgatory sweep reclaims, and reclaim it.

    Session-free by construction: neither entry point opens, flushes or
    commits a transaction, so the ``purgatory_cleanup`` worker can hold
    a read transaction across :meth:`plan`, drop it for the duration of
    the object-store work, and take a fresh short one per build to stamp
    ``date_purged``. That split is what keeps a job that reclaims
    hundreds of builds from pinning a connection idle-in-transaction for
    the whole run — and what makes a crash mid-run resumable, since
    every build the job got through is already committed.
    """

    def __init__(
        self,
        *,
        build_store: BuildStore,
        edition_store: EditionStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._build_store = build_store
        self._edition_store = edition_store
        self._logger = logger

    async def plan(
        self, *, org: Organization, now: datetime, limit: int
    ) -> PurgatoryPlan:
        """Partition an organization's expired builds into a tick's work.

        Reads the eligible builds — soft-deleted, unstamped, and deleted
        before ``now - org.purgatory_retention`` — and splits them on
        one further question the retention window cannot answer: does
        anything still resolve to this build? A live edition holding it
        as ``current_build_id`` is serving its tree right now, so
        reclaiming it would 404 a URL somebody is using, however long
        ago the row was deleted.

        The 409 on DELETE stops new references arising, and the project
        soft-delete cascade deletes a project's editions alongside its
        builds so they stop counting. What is left for this check are
        the rows that predate those guards, and the narrow window in
        which a rollback repoints an edition at an already-deleted
        build. Held-back builds keep ``date_purged`` null, so a later
        tick re-examines them once the edition moves on.

        The reference question is asked per build rather than in one
        batch: it is the same query the DELETE guard runs on the request
        path, and a plan is bounded by the per-job cap, so the cost is a
        few hundred indexed lookups inside one read transaction.

        Parameters
        ----------
        org
            The organization this tick is running for. Its
            ``purgatory_retention`` is the only source of the cutoff.
        now
            The instant to judge retention against, passed in so the
            whole tick shares one clock.
        limit
            The per-job cap. Bounds the two lists together, since the
            partition happens after the read.

        Returns
        -------
        PurgatoryPlan
            The builds to reclaim and the builds to report.
        """
        cutoff = now - org.purgatory_retention
        candidates = await self._build_store.list_purgeable(
            org_id=org.id, cutoff=cutoff, limit=limit
        )
        purgeable: list[Build] = []
        referenced: list[ReferencedBuild] = []
        for build in candidates:
            slugs = await self._edition_store.list_live_slugs_by_current_build(
                build_id=build.id
            )
            if slugs:
                referenced.append(
                    ReferencedBuild(build=build, edition_slugs=tuple(slugs))
                )
            else:
                purgeable.append(build)
        return PurgatoryPlan(
            purgeable=tuple(purgeable), referenced=tuple(referenced)
        )

    async def reclaim(
        self, *, build: Build, object_store: ObjectStore
    ) -> PurgeOutcome:
        """Delete one build's objects and report what went.

        Both halves of what a build occupies: the unpacked tree under
        ``storage_prefix``, and the staged tarball at ``staging_key``.
        ``build_processing`` drops the tarball at completion, so most
        builds arrive here with that key already gone — deleting an
        absent key is a no-op on every store, which is what lets this
        run unconditionally rather than branching on the build's status.

        Nothing is caught. A store that refuses part of a delete raises
        :exc:`~docverse_server.storage.objectstore.ObjectStoreError`,
        and the caller stamps ``date_purged`` on the strength of this
        method returning — a stamp that takes the build out of every
        later work list. Swallowing the failure would record the content
        as reclaimed while the bytes are still on the store with nothing
        pointing at them, so the error goes to the worker's per-build
        failure counter and the row stays in the next tick's queue.

        Parameters
        ----------
        build
            The build to reclaim. Read-only here: stamping the row is
            the caller's transaction to own.
        object_store
            The organization's staging store, which is where
            ``build_processing`` wrote both the tree and the tarball.

        Returns
        -------
        PurgeOutcome
            The object count from the tree delete, and the size the row
            recorded. The tarball is not counted: a delete of an absent
            key is indistinguishable from one that removed something, so
            claiming it would make the count unreliable in exactly the
            common case.
        """
        objects_deleted = await object_store.delete_prefix(
            prefix=build.storage_prefix
        )
        await object_store.delete_object(key=build.staging_key)
        return PurgeOutcome(
            objects_deleted=objects_deleted,
            bytes_reclaimed=build.total_size_bytes or 0,
        )
