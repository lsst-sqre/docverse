"""Service for managing editions."""

from __future__ import annotations

from typing import Any

import structlog
from safir.database import CountedPaginatedList, PaginationCursor

from docverse.models import EditionCreate, EditionKind, EditionUpdate
from docverse.models.queue_enums import JobKind, PublishStatus
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.build import Build
from docverse_server.domain.edition import (
    Edition,
    EditionWrite,
    RepointOutcome,
)
from docverse_server.domain.edition_build_history import (
    EditionBuildHistoryWithBuild,
)
from docverse_server.domain.organization import Organization
from docverse_server.domain.project import Project
from docverse_server.domain.queue import QueueJob
from docverse_server.exceptions import ConflictError, NotFoundError
from docverse_server.metrics import EditionPublishTrigger
from docverse_server.services.queue_dispatch import QueueDispatcher
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.keeper_sync import TombstoneReason
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.pagination import (
    EditionBuildHistoryPositionCursor,
)
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.validation import parse_base32_id

_SETTLED_PUBLISH_STATUSES = frozenset(
    {
        PublishStatus.pending,
        PublishStatus.publishing,
        PublishStatus.published,
    }
)
"""Publish states that leave a repoint onto the current build nothing
to do.

The complement — ``failed``, and the ``None`` of an edition that has
never been published — is what makes re-requesting the build an
edition already serves meaningful: it is the only operator-reachable
way to re-drive the publish. See :meth:`EditionService._repoint_and_publish`.
"""


class EditionService:
    """Business logic for edition management."""

    def __init__(
        self,
        *,
        store: EditionStore,
        org_store: OrganizationStore,
        project_store: ProjectStore,
        logger: structlog.stdlib.BoundLogger,
        history_store: EditionBuildHistoryStore,
        build_store: BuildStore,
        dispatcher: QueueDispatcher,
        queue_job_store: QueueJobStore,
    ) -> None:
        self._store = store
        self._org_store = org_store
        self._project_store = project_store
        self._logger = logger
        self._history_store = history_store
        self._build_store = build_store
        self._dispatcher = dispatcher
        self._queue_job_store = queue_job_store

    async def _resolve_org_project(
        self, org_slug: str, project_slug: str
    ) -> tuple[Organization, Project]:
        """Resolve org + project slugs to their domain objects."""
        org = await self._org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        project = await self._project_store.get_by_slug(
            org_id=org.id, slug=project_slug
        )
        if project is None:
            msg = f"Project {project_slug!r} not found"
            raise NotFoundError(msg)
        return org, project

    async def create(
        self,
        *,
        org_slug: str,
        project_slug: str,
        data: EditionCreate,
    ) -> tuple[Organization, Project, Edition]:
        """Create a new edition.

        Raises
        ------
        ConflictError
            If an edition with the same slug already exists.
        """
        org, project = await self._resolve_org_project(org_slug, project_slug)
        existing = await self._store.get_by_slug(
            project_id=project.id, slug=data.slug
        )
        if existing is not None:
            msg = f"Edition with slug {data.slug!r} already exists"
            raise ConflictError(msg)
        edition = await self._store.create(project_id=project.id, data=data)
        self._logger.info(
            "Created edition",
            slug=data.slug,
            org=org_slug,
            project=project_slug,
        )
        return org, project, edition

    async def get_by_slug(
        self,
        *,
        org_slug: str,
        project_slug: str,
        slug: str,
    ) -> tuple[Organization, Project, Edition]:
        """Get an edition by slug within a project.

        Raises
        ------
        NotFoundError
            If the edition is not found.
        """
        org, project = await self._resolve_org_project(org_slug, project_slug)
        edition = await self._store.get_by_slug(
            project_id=project.id, slug=slug
        )
        if edition is None:
            msg = f"Edition {slug!r} not found"
            raise NotFoundError(msg)
        return org, project, edition

    async def list_by_project(
        self,
        *,
        org_slug: str,
        project_slug: str,
        cursor_type: type[PaginationCursor[Edition]],
        cursor: PaginationCursor[Edition] | None = None,
        limit: int,
        kind: EditionKind | None = None,
    ) -> tuple[
        Organization,
        Project,
        CountedPaginatedList[Edition, PaginationCursor[Edition]],
    ]:
        """List all editions for a project."""
        org, project = await self._resolve_org_project(org_slug, project_slug)
        result = await self._store.list_by_project(
            project.id,
            cursor_type=cursor_type,
            cursor=cursor,
            limit=limit,
            kind=kind,
        )
        return org, project, result

    async def update(
        self,
        *,
        org_slug: str,
        project_slug: str,
        slug: str,
        data: EditionUpdate,
    ) -> EditionWrite:
        """Update an edition.

        If ``data.build`` is set, apply an emergency build override:
        point the edition at the target build (even one not in
        history), record a new history entry, mark the edition
        ``publish_status=pending``, and enqueue a ``publish_edition``
        job. Unlike rollback, this path bypasses the history-membership
        guard. Naming the build the edition already serves repoints
        nothing; see :meth:`_repoint_and_publish` for what that does
        and does not still do. A metadata field in the same payload is
        applied either way.

        The returned :class:`~docverse_server.domain.edition.EditionWrite`
        reports whether anything was written, so the handler can tell a
        real update from a request whose postcondition already held and
        skip announcing the latter. A payload carrying metadata is
        always a write, because the store sets whatever fields it names
        and moves the edition's ``date_updated`` with them; a payload
        carrying nothing but a ``build`` the edition already serves
        (with its publish settled) is not, and neither is an empty one.

        The metadata write is skipped entirely when the payload carries
        nothing but ``build``. It would have nothing to write, but it is
        not free: it re-selects the edition, flushes, refreshes, and
        re-reads it through the join — four round-trips taken while the
        transaction holds the project, edition, and build rows, on the
        commonest shape of this request.

        The override runs *before* the metadata write, which is a lock
        ordering requirement rather than a preference: it is the arm of
        this method that reaches the project row, and
        :mod:`docverse_server.storage.edition_store` fixes one order —
        projects, then editions, then builds — for every writer that
        touches more than one of them. Flushing the metadata first took
        the edition row ahead of the project row, which only a payload
        carrying *both* a metadata field and ``build`` could do: a
        build-only PATCH emits no ``UPDATE editions`` of its own.

        Raises
        ------
        NotFoundError
            If the edition or target build is not found.
        """
        org, project = await self._resolve_org_project(org_slug, project_slug)

        build_public_id = data.build
        other_updates = EditionUpdate.model_validate(
            data.model_dump(exclude={"build"}, exclude_unset=True)
        )

        edition: Edition | None = None
        repointed = False
        if build_public_id is not None:
            target = await self._store.get_by_slug(
                project_id=project.id, slug=slug
            )
            if target is None:
                msg = f"Edition {slug!r} not found"
                raise NotFoundError(msg)
            edition, repointed = await self._apply_build_override(
                org_id=org.id,
                project_id=project.id,
                project_slug=project_slug,
                edition=target,
                build_public_id=build_public_id,
            )

        # The metadata write runs when there is metadata to write, and
        # when it does it is the last thing to touch the row, so its
        # result is the one that describes the edition the caller gets
        # back. An override on its own already returned that row; a
        # payload that is neither still comes through here, because an
        # empty PATCH of a missing edition has to 404 like any other.
        wrote_metadata = bool(other_updates.model_fields_set)
        if wrote_metadata or edition is None:
            edition = await self._store.update(
                project_id=project.id, slug=slug, data=other_updates
            )
            if edition is None:
                msg = f"Edition {slug!r} not found"
                raise NotFoundError(msg)

        self._logger.info(
            "Updated edition", slug=slug, org=org_slug, project=project_slug
        )
        return EditionWrite(
            organization=org,
            project=project,
            edition=edition,
            changed=repointed or wrote_metadata,
        )

    async def _repoint_and_publish(
        self,
        *,
        org_id: int,
        project_id: int,
        project_slug: str,
        edition: Edition,
        build: Build,
        build_public_id: str,
        trigger: EditionPublishTrigger | None = None,
    ) -> tuple[Edition, QueueJob | None]:
        """Point an edition at a build and queue the publish it owes.

        The whole of what the two operator-driven repoints — the
        ``build`` override on ``PATCH .../editions/{slug}`` and
        :meth:`rollback` — do once they have resolved their target:
        move the binding, record the history row, mark both the edition
        and that row ``pending``, and enqueue the ``publish_edition``
        job. They differ only in how they choose the build and in what
        they log, so the sequence lives here and a fix to it is a fix
        to both.

        Both waive the stale-build guard, because both mean "serve this
        build regardless of what is newer", which is what makes the
        build the edition *already* serves reachable here. That case is
        answered by
        :meth:`~docverse_server.storage.edition_store.EditionStore.set_current_build`,
        under the edition's row lock, rather than by comparing against
        a read taken before it: a concurrent repoint committing in that
        window would otherwise have this path report "already serving
        that build" about a build the edition no longer serves.

        An unchanged binding still leaves a question the store cannot
        answer, which is whether the *publish* is settled. Where it is
        — ``pending``, ``publishing`` or ``published`` — there is
        nothing to do, and doing it anyway would announce a change that
        is not happening: a history row, a ``publish_status`` flip, a
        publish job, and (for ``__main``) a project clock stamp that
        retires every cached ``ETag`` and re-emits a byte-identical row
        into every consumer's ``updated_since`` window. Where it is
        ``failed`` or has never been set, re-requesting the current
        build is the only operator-reachable way to re-drive the
        publish — the reconcile worker leaves a failed pair alone by
        design, keeper-sync's self-heal skips it, and there is no
        republish endpoint — so the full sequence runs. The project
        clock stays put either way, because the served build is the
        same one.

        Returns the edition as it now stands, and the publish job if
        one was enqueued; ``None`` for the job says the request was a
        no-op, which is what the callers log (or decline to log).
        """
        repoint = await self._store.set_current_build(
            edition_id=edition.id,
            build_id=build.id,
            skip_date_guard=True,
        )
        current = repoint.edition
        if current is None:
            # The refused outcome. ``skip_date_guard`` waives the
            # ordering guard, so the only guard left is the
            # deleted-build one: the build this read as live was
            # soft-deleted between that read and this write.
            msg = (
                f"Build {build_public_id!r} was deleted while repointing "
                f"edition {edition.slug!r}"
            )
            raise RuntimeError(msg)

        if (
            repoint.outcome is RepointOutcome.unchanged
            and current.publish_status in _SETTLED_PUBLISH_STATUSES
        ):
            self._logger.info(
                "Skipped no-op edition repoint",
                edition_id=edition.id,
                edition_slug=edition.slug,
                build=build_public_id,
                publish_status=current.publish_status,
            )
            return current, None

        new_history_entry = await self._history_store.record(
            edition_id=edition.id, build_id=build.id
        )

        await self._store.set_publish_status(
            edition_id=edition.id, status=PublishStatus.pending
        )
        await self._history_store.set_publish_status(
            history_id=new_history_entry.id, status=PublishStatus.pending
        )
        current.publish_status = PublishStatus.pending

        child_job = await self._queue_job_store.create(
            kind=JobKind.publish_edition,
            org_id=org_id,
            project_id=project_id,
            build_id=build.id,
            edition_id=edition.id,
        )
        payload: dict[str, Any] = {
            "org_id": org_id,
            "project_slug": project_slug,
            "edition_id": edition.id,
            "edition_slug": edition.slug,
            "build_id": build.id,
            "build_public_id": serialize_base32_id(build.public_id),
            # Name the row just recorded. Rollback is what puts two
            # rows on one ``(edition, build)`` pair, so a worker that
            # resolved the pair instead could pick up — and overwrite —
            # whichever row a *later* repoint added.
            "history_id": new_history_entry.id,
        }
        if trigger is not None:
            # Tag the publish so its edition_published metric reports
            # the operator action rather than the default build fan-out
            # (the queue job carries no keeper_sync_run_id). SQR-112 D7.
            payload["trigger"] = trigger.value
        self._dispatcher.defer(
            queue_job=child_job,
            job_type="publish_edition",
            payload=payload,
        )
        return current, child_job

    async def _apply_build_override(
        self,
        *,
        org_id: int,
        project_id: int,
        project_slug: str,
        edition: Edition,
        build_public_id: str,
    ) -> tuple[Edition, bool]:
        """Point ``edition`` at an arbitrary build (emergency override).

        Naming the build the edition already serves repoints nothing
        and, unless the publish of that build failed, does nothing else
        either; see :meth:`_repoint_and_publish`.

        Returns the edition as it now stands and whether the repoint
        sequence actually ran — ``False`` for the inert case, which is
        what lets ``PATCH`` skip announcing a change that did not
        happen.
        """
        public_id = parse_base32_id(build_public_id, resource="build")

        build = await self._build_store.get_by_public_id(
            project_id=project_id, public_id=public_id
        )
        if build is None:
            msg = f"Build {build_public_id!r} not found"
            raise NotFoundError(msg)

        updated_edition, child_job = await self._repoint_and_publish(
            org_id=org_id,
            project_id=project_id,
            project_slug=project_slug,
            edition=edition,
            build=build,
            build_public_id=build_public_id,
        )
        if child_job is not None:
            self._logger.info(
                "Applied edition build override",
                edition_id=edition.id,
                build=build_public_id,
                publish_queue_job_public_id=serialize_base32_id(
                    child_job.public_id
                ),
            )
        return updated_edition, child_job is not None

    async def set_current_build(
        self, *, edition_id: int, build_id: int
    ) -> Edition | None:
        """Set the current build for an edition.

        Returns
        -------
        Edition or None
            The updated edition, or ``None`` if the update was skipped
            because the edition already points to a newer build.
        """
        edition = (
            await self._store.set_current_build(
                edition_id=edition_id, build_id=build_id
            )
        ).edition
        if edition is None:
            self._logger.info(
                "Skipped stale build for edition",
                edition_id=edition_id,
                build_id=build_id,
            )
        else:
            self._logger.info(
                "Set current build for edition",
                edition_id=edition_id,
                build_id=build_id,
            )
        return edition

    async def list_history(
        self,
        *,
        org_slug: str,
        project_slug: str,
        edition_slug: str,
        cursor: EditionBuildHistoryPositionCursor | None = None,
        limit: int,
        include_deleted: bool = False,
    ) -> CountedPaginatedList[
        EditionBuildHistoryWithBuild,
        EditionBuildHistoryPositionCursor,
    ]:
        """List build history for an edition."""
        _, project = await self._resolve_org_project(org_slug, project_slug)
        edition = await self._store.get_by_slug(
            project_id=project.id, slug=edition_slug
        )
        if edition is None:
            msg = f"Edition {edition_slug!r} not found"
            raise NotFoundError(msg)
        return await self._history_store.list_by_edition_with_build_info(
            edition.id,
            cursor=cursor,
            limit=limit,
            include_deleted=include_deleted,
        )

    async def rollback(
        self,
        *,
        org_slug: str,
        project_slug: str,
        edition_slug: str,
        build_public_id: str,
    ) -> EditionWrite:
        """Roll back an edition to a previously-recorded build.

        Rolling back to the build the edition already serves repoints
        nothing and, unless the publish of that build failed, does
        nothing else either; see :meth:`_repoint_and_publish`. The
        membership guard is still checked first, so a build outside
        this edition's history is a 404 even when it happens to be the
        one being served — an override can leave the edition on a build
        rollback was never offered.

        The returned
        :class:`~docverse_server.domain.edition.EditionWrite` reports
        which of those two it was, so the handler can skip announcing
        the inert one.

        Parameters
        ----------
        org_slug
            Organization slug.
        project_slug
            Project slug.
        edition_slug
            Edition slug.
        build_public_id
            Base32 public ID of the target build.

        Raises
        ------
        NotFoundError
            If the edition, build, or history entry is not found.
        """
        org, project = await self._resolve_org_project(org_slug, project_slug)

        edition = await self._store.get_by_slug(
            project_id=project.id, slug=edition_slug
        )
        if edition is None:
            msg = f"Edition {edition_slug!r} not found"
            raise NotFoundError(msg)

        public_id = parse_base32_id(build_public_id, resource="build")

        build = await self._build_store.get_by_public_id(
            project_id=project.id, public_id=public_id
        )
        if build is None:
            msg = f"Build {build_public_id!r} not found"
            raise NotFoundError(msg)

        history_entry = await self._history_store.get_by_edition_and_build(
            edition_id=edition.id, build_id=build.id
        )
        if history_entry is None:
            msg = "Build is not in this edition's history"
            raise NotFoundError(msg)

        updated_edition, child_job = await self._repoint_and_publish(
            org_id=org.id,
            project_id=project.id,
            project_slug=project_slug,
            edition=edition,
            build=build,
            build_public_id=build_public_id,
            trigger=EditionPublishTrigger.rollback,
        )
        if child_job is not None:
            self._logger.info(
                "Rolled back edition",
                slug=edition_slug,
                org=org_slug,
                project=project_slug,
                build=build_public_id,
                publish_queue_job_public_id=serialize_base32_id(
                    child_job.public_id
                ),
            )
        return EditionWrite(
            organization=org,
            project=project,
            edition=updated_edition,
            changed=child_job is not None,
        )

    async def soft_delete(
        self,
        *,
        org_id: int,
        project_id: int,
        edition_id: int,
        edition_slug: str,
        reason: TombstoneReason,
    ) -> bool:
        """Soft-delete one edition and stamp the keeper-sync tombstone.

        The single, id-based entrypoint shared by every Docverse-side
        deletion path (PRD #332): the API DELETE handler, the
        ``lifecycle_eval`` and ``git_ref_audit`` workers, and the
        ``ref_deleted`` webhook processor. The ``reason`` is threaded
        through to :meth:`EditionStore.soft_delete`, which records it
        on the matching ``keeper_sync_state`` row in the same flush as
        ``date_deleted`` (no-op when no state row exists).

        Returns ``False`` when the edition was not found / already
        soft-deleted so bulk callers iterating a candidate set can
        treat it as a no-op and continue; the handler raises
        :class:`NotFoundError` on ``False``.
        """
        deleted = await self._store.soft_delete(
            org_id=org_id,
            project_id=project_id,
            slug=edition_slug,
            reason=reason,
        )
        if deleted:
            self._logger.info(
                "Soft-deleted edition",
                org_id=org_id,
                project_id=project_id,
                edition_id=edition_id,
                edition_slug=edition_slug,
                reason=reason.value,
            )
        return deleted
