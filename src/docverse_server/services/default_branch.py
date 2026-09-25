"""Converge a project's ``__main`` edition on its default branch.

A project's ``__main`` edition stores a literal branch name in
``tracking_params.git_ref``. When the repository's default branch is
renamed (``master`` → ``main``), builds start arriving on the new name,
``__main`` never matches them, and the slug derivation auto-creates a
stray ``main`` draft instead — silently (PRD #721).

:class:`DefaultBranchService` is the one rule that repairs this. Each
trigger that learns the repository's default branch — the
``repository.edited`` webhook, the ``project_github_resolve`` worker,
and the daily ``git_ref_audit`` — hands it the branch and whatever
evidence it has that ``__main``'s own ref is gone, and the service
records the branch, rewrites ``__main`` only when that evidence holds,
retires the draft the old tracking let accumulate, and repoints
``__main`` at the new branch's newest build.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum

import structlog

from docverse.models import TrackingMode
from docverse_server.domain.edition import DEFAULT_EDITION_SLUG, Edition
from docverse_server.domain.project import Project
from docverse_server.services.edition import EditionService
from docverse_server.services.edition_publishing import (
    EditionPublishingService,
)
from docverse_server.services.lock_service import LockKey, LockService
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.keeper_sync import TombstoneReason
from docverse_server.storage.project_store import ProjectStore

__all__ = [
    "DefaultBranchOutcome",
    "DefaultBranchService",
    "DefaultBranchTrigger",
]


class DefaultBranchTrigger(StrEnum):
    """What told Docverse the repository's default branch.

    Logged as ``trigger`` on every line the service writes, so an
    operator can tell a webhook-driven rewrite from one the daily audit
    made after a missed delivery.
    """

    webhook = "webhook"
    """A ``repository.edited`` delivery changing the default branch."""

    resolve = "resolve"
    """The ``project_github_resolve`` worker's ``GET /repos`` read."""

    audit = "audit"
    """The daily ``git_ref_audit`` worker's ``GET /repos`` read."""


@dataclass(frozen=True, slots=True)
class DefaultBranchOutcome:
    """What one :meth:`DefaultBranchService.apply` call changed.

    Every field is in its "nothing happened" state for a call whose
    postcondition already held, which is what makes a redelivered
    webhook inert: the caller publishes no lifecycle event and enqueues
    no dashboard rebuild for it.
    """

    column_changed: bool
    """Whether ``projects.github_default_branch`` took a new value."""

    rewritten_from: str | None = None
    """The ref ``__main`` tracked before this call rewrote it.

    ``None`` when ``__main`` was left alone: it already tracked the
    default branch, it tracks a ref that still exists, it is not in
    ``git_ref`` mode, or the project has no ``__main``.
    """

    drafts_retired: tuple[int, ...] = ()
    """Ids of the ``draft`` editions soft-deleted as duplicates.

    Only ever non-empty after a rewrite.
    """

    repointed_build_id: int | None = None
    """The build ``__main`` was repointed at, when it was.

    Only ever set after a rewrite, and then only when the new branch
    has a completed build newer than the one ``__main`` served. A
    repoint always enqueues exactly one ``publish_edition`` job.
    """

    @property
    def main_rewritten(self) -> bool:
        """Whether ``__main``'s tracked ref was rewritten.

        The caller's cue to announce the change the way a ``PATCH`` of
        the edition would: an ``edition_lifecycle`` ``update`` event and
        a ``dashboard_build`` for the project.
        """
        return self.rewritten_from is not None


class DefaultBranchService:
    """Apply a repository's default branch to its project.

    The caller owns the transaction, per the handler-owns-the-
    transaction rule: this service flushes through store writes and
    defers the ``publish_edition`` enqueue to the factory's
    :class:`~docverse_server.services.queue_dispatch.QueueDispatcher`,
    so the caller commits, then dispatches, then enqueues the project's
    ``dashboard_build``.
    """

    def __init__(
        self,
        *,
        project_store: ProjectStore,
        edition_store: EditionStore,
        build_store: BuildStore,
        edition_service: EditionService,
        publishing_service: EditionPublishingService,
        lock_service: LockService,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._project_store = project_store
        self._edition_store = edition_store
        self._build_store = build_store
        self._edition_service = edition_service
        self._publishing_service = publishing_service
        self._lock_service = lock_service
        self._logger = logger

    async def apply(
        self,
        *,
        project: Project,
        default_branch: str,
        trigger: DefaultBranchTrigger,
        old_default_branch: str | None = None,
        live_refs: frozenset[str] | None = None,
    ) -> DefaultBranchOutcome:
        """Record ``default_branch`` and converge ``__main`` on it.

        In order, all under ``__main``'s ``EDITION_UPDATE`` advisory
        lock — the key edition tracking, keeper-sync, and
        ``publish_edition`` take for the same edition, so none of them
        interleaves a pointer or tracking write with this one:

        1. Write ``projects.github_default_branch``. A value that
           already matches is not written, so the project's clock stays
           put.
        2. Rewrite ``__main``'s ``tracking_params.git_ref`` to
           ``default_branch``, but only when ``__main`` is in
           ``git_ref`` mode, tracks something else, and that ref is
           *gone*: it equals ``old_default_branch``, or ``live_refs``
           was supplied and lacks it. A ``__main`` pinned to a branch or
           tag that still exists is never touched, and neither is an
           ``lsst_doc`` one, which carries no ``git_ref``. Nothing but
           the ref moves: ``kind``, ``kind_source``, ``title``, and
           ``lifecycle_exempt`` keep their values.
        3. After a rewrite, soft-delete and unpublish every ``draft``
           ``git_ref`` edition tracking ``default_branch`` — the one
           tracking auto-created while ``__main`` ignored the branch —
           so the two do not both match the next push.
        4. After a rewrite, repoint ``__main`` at the branch's newest
           completed build under the ordinary stale-build guard, which
           records the history row, marks the publish pending, defers a
           ``publish_edition`` job, and moves the project's clock.

        The advisory lock is taken before this transaction writes
        anything, following the convention the ``publish_edition`` and
        keeper-sync writers use (advisory lock, then rows): waiting on
        it while holding a row another holder of the lock needs would be
        a deadlock PostgreSQL cannot see.

        Parameters
        ----------
        project
            The project to converge.
        default_branch
            The repository's default branch as GitHub reports it.
        trigger
            What reported it, for the logs.
        old_default_branch
            The branch that was the default before, when the trigger
            knows it (the webhook's ``changes.default_branch.from``).
            ``__main`` tracking exactly this ref is taken as tracking a
            ref that is gone.
        live_refs
            The repository's live branch and tag names, when the trigger
            fetched them (the audit). ``__main`` tracking a ref absent
            from this set is taken as tracking a ref that is gone.
            ``None`` means "not known", never "no refs".

        Returns
        -------
        DefaultBranchOutcome
            What changed.
        """
        logger = self._logger.bind(
            trigger=trigger.value,
            project_id=project.id,
            project_slug=project.slug,
            old_default_branch=old_default_branch,
            new_default_branch=default_branch,
        )
        main = await self._edition_store.get_by_slug(
            project_id=project.id, slug=DEFAULT_EDITION_SLUG
        )
        async with self._main_lock(project=project, main=main):
            column_changed = (
                await self._project_store.set_github_default_branch(
                    project_id=project.id, value=default_branch
                )
            )
            if column_changed:
                logger.info("Recorded repository default branch")
            rewritten_from = None
            if main is not None:
                rewritten_from = await self._rewrite_main(
                    main_id=main.id,
                    default_branch=default_branch,
                    old_default_branch=old_default_branch,
                    live_refs=live_refs,
                    logger=logger,
                )
            if main is None or rewritten_from is None:
                outcome = DefaultBranchOutcome(column_changed=column_changed)
            else:
                outcome = DefaultBranchOutcome(
                    column_changed=column_changed,
                    rewritten_from=rewritten_from,
                    drafts_retired=await self._retire_drafts(
                        project=project, ref=default_branch, logger=logger
                    ),
                    repointed_build_id=await self._repoint_main(
                        project=project,
                        main=main,
                        ref=default_branch,
                        logger=logger,
                    ),
                )
        logger.info(
            "Applied repository default branch",
            column_changed=outcome.column_changed,
            main_rewritten=outcome.main_rewritten,
            main_rewritten_from=outcome.rewritten_from,
            drafts_retired=len(outcome.drafts_retired),
            repointed_build_id=outcome.repointed_build_id,
        )
        return outcome

    @asynccontextmanager
    async def _main_lock(
        self, *, project: Project, main: Edition | None
    ) -> AsyncGenerator[None]:
        """Hold ``__main``'s ``EDITION_UPDATE`` lock, when it has one.

        A project with no live ``__main`` has nothing to converge but
        the column, which no other lock holder writes.
        """
        if main is None:
            yield
            return
        lock_key = LockKey.for_edition_update(
            org_id=project.org_id, project_id=project.id, edition_id=main.id
        )
        async with self._lock_service.acquire(lock_key):
            yield

    async def _rewrite_main(
        self,
        *,
        main_id: int,
        default_branch: str,
        old_default_branch: str | None,
        live_refs: frozenset[str] | None,
        logger: structlog.stdlib.BoundLogger,
    ) -> str | None:
        """Point ``__main`` at ``default_branch`` if its own ref is gone.

        Re-reads the edition now that the lock is held, so the decision
        rests on the tracking as the last writer left it rather than as
        it stood before the wait.

        Returns the ref ``__main`` tracked before the rewrite, or
        ``None`` when the guard left it alone.
        """
        main = await self._edition_store.get_by_id(main_id)
        if main is None or main.tracking_mode is not TrackingMode.git_ref:
            return None
        params = dict(main.tracking_params or {})
        current_ref = params.get("git_ref")
        if not isinstance(current_ref, str) or current_ref == default_branch:
            return None
        if not _ref_is_gone(
            current_ref,
            old_default_branch=old_default_branch,
            live_refs=live_refs,
        ):
            logger.info(
                "Left __main tracking a ref that still exists",
                edition_id=main.id,
                main_git_ref=current_ref,
            )
            return None
        params["git_ref"] = default_branch
        await self._edition_store.update_tracking(
            edition_id=main.id,
            tracking_mode=TrackingMode.git_ref,
            tracking_params=params,
        )
        logger.info(
            "Rewrote __main to track the default branch",
            edition_id=main.id,
            main_rewritten_from=current_ref,
        )
        return current_ref

    async def _retire_drafts(
        self,
        *,
        project: Project,
        ref: str,
        logger: structlog.stdlib.BoundLogger,
    ) -> tuple[int, ...]:
        """Soft-delete and unpublish the drafts duplicating ``__main``.

        The ``RefDeletedWebhookProcessor`` recipe, over the same
        candidate set (live, non-exempt ``draft`` editions tracking
        ``ref``) narrowed to plain ``git_ref`` mode: a deployment-scoped
        ``alternate_git_ref`` draft never matches a build alongside
        ``__main``, so it is not a duplicate. ``unpublish`` runs inside
        the caller's transaction, as it does there, so a CDN failure
        rolls the whole convergence back and the delivery is retried.
        """
        candidates = await self._edition_store.list_draft_editions_by_git_ref(
            project_id=project.id, git_ref=ref
        )
        retired: list[int] = []
        for edition in candidates:
            if edition.tracking_mode is not TrackingMode.git_ref:
                continue
            deleted = await self._edition_service.soft_delete(
                org_id=project.org_id,
                project_id=project.id,
                edition_id=edition.id,
                edition_slug=edition.slug,
                reason=TombstoneReason.lifecycle_delete,
            )
            if not deleted:
                continue
            await self._publishing_service.unpublish(
                org_id=project.org_id,
                project_slug=project.slug,
                edition_slug=edition.slug,
            )
            retired.append(edition.id)
            logger.info(
                "Retired draft duplicating __main",
                edition_id=edition.id,
                edition_slug=edition.slug,
                github_ref=ref,
            )
        return tuple(retired)

    async def _repoint_main(
        self,
        *,
        project: Project,
        main: Edition,
        ref: str,
        logger: structlog.stdlib.BoundLogger,
    ) -> int | None:
        """Serve the newest completed build of ``ref`` from ``__main``.

        A build still processing is left to edition tracking, which
        now matches ``__main`` and advances it when that build
        completes. Returns the build repointed at, or ``None``.
        """
        build = await self._build_store.get_latest_completed_for_ref(
            project_id=project.id, git_ref=ref
        )
        if build is None:
            logger.info(
                "No completed build to repoint __main at",
                edition_id=main.id,
                github_ref=ref,
            )
            return None
        job = await self._edition_service.advance_to_build(
            org_id=project.org_id,
            project_slug=project.slug,
            edition=main,
            build=build,
        )
        if job is None:
            logger.info(
                "Kept __main on a build at least as new",
                edition_id=main.id,
                github_ref=ref,
                build_id=build.id,
                current_build_id=main.current_build_id,
            )
            return None
        logger.info(
            "Repointed __main at the default branch",
            edition_id=main.id,
            github_ref=ref,
            repointed_build_id=build.id,
            previous_build_id=main.current_build_id,
        )
        return build.id


def _ref_is_gone(
    ref: str,
    *,
    old_default_branch: str | None,
    live_refs: frozenset[str] | None,
) -> bool:
    """Whether the evidence at hand says ``ref`` no longer exists.

    Either arm suffices. The branch that just stopped being the default
    is taken as gone because that is what a rename leaves behind; a ref
    missing from a live set that was actually fetched is gone by
    definition. With neither — a first resolve that only learns the
    branch — nothing is known, and ``__main`` is left for a later audit
    tick to judge.
    """
    if old_default_branch is not None and ref == old_default_branch:
        return True
    return live_refs is not None and ref not in live_refs
