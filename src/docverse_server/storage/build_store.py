"""Database operations for the builds table."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import overload

import structlog
from safir.database import CountedPaginatedList, CountedPaginatedQueryRunner
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from docverse.models import BuildCreate, BuildStatus, JobStatus
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.build import Build
from docverse_server.domain.content_hash import PLACEHOLDER_CONTENT_HASH
from docverse_server.exceptions import InvalidBuildStateError
from docverse_server.storage._public_id import (
    insert_with_time_ordered_public_id,
)
from docverse_server.storage.pagination import BuildDateCreatedCursor

# Valid status transitions, keyed by the status a build is leaving. The
# keys are exactly the statuses ``BuildStatus.is_unfinished`` calls live
# — the edges themselves cannot be derived, since the two live statuses
# lead to different places, but which statuses *have* edges is the same
# partition the service's retirement helpers and the worker branch on
# (pinned by ``tests/storage/build_store_test.py``). Everything absent
# is terminal and rejects any further transition, which is what lets a
# reader treat it as a final answer about the build.
_VALID_TRANSITIONS: dict[BuildStatus, set[BuildStatus]] = {
    BuildStatus.pending: {
        BuildStatus.processing,
        BuildStatus.failed,
        BuildStatus.cancelled,
    },
    BuildStatus.processing: {
        BuildStatus.completed,
        BuildStatus.failed,
        BuildStatus.superseded,
        BuildStatus.cancelled,
    },
}


class BuildStore:
    """Direct database operations for builds."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(
        self,
        *,
        project_id: int,
        project_slug: str,
        data: BuildCreate,
        uploader: str,
    ) -> Build:
        """Insert a new build row with status=pending.

        The ``public_id`` is a time-ordered Crockford Base32 resource ID
        minted at insert time. Because that ID is embedded in ``staging_key``
        and ``storage_prefix``, the object-store keys are recomputed for each
        mint attempt inside :func:`insert_with_time_ordered_public_id`, which
        re-mints on the (rare) same-millisecond ``public_id`` collision.

        A ``data.content_hash`` of ``None`` — the client omitted the
        deprecated transport digest — becomes
        :data:`~docverse_server.domain.content_hash.PLACEHOLDER_CONTENT_HASH`,
        because the column is ``NOT NULL`` and the build's real content
        identity is not known until the worker has hashed the extracted
        content. The substitution lives here, at the one place a pending
        row is constructed, rather than in a caller: a service that
        forgot it would trip the ``NOT NULL`` constraint at flush time
        instead of writing a well-formed placeholder.
        """

        def _make_row(public_id: int) -> SqlBuild:
            base32_str = serialize_base32_id(public_id)
            return SqlBuild(
                public_id=public_id,
                project_id=project_id,
                git_ref=data.git_ref,
                alternate_name=data.alternate_name,
                content_hash=(
                    data.content_hash
                    if data.content_hash is not None
                    else PLACEHOLDER_CONTENT_HASH
                ),
                status=BuildStatus.pending,
                staging_key=f"__staging/{base32_str}.tar.gz",
                storage_prefix=f"{project_slug}/__builds/{base32_str}/",
                uploader=uploader,
                annotations=(
                    data.annotations.model_dump(mode="json", exclude_none=True)
                    if data.annotations is not None
                    else None
                ),
            )

        row = await insert_with_time_ordered_public_id(
            self._session, _make_row
        )
        await self._session.refresh(row)
        return Build.model_validate(row)

    async def get_by_id(self, build_id: int) -> Build | None:
        """Fetch a build by internal ID.

        An unlocked snapshot read. A caller that decides *what to write*
        from what it reads — "is this build still unfinished?" — wants
        :meth:`get_for_update` instead, so its decision and its write
        cannot straddle somebody else's commit.
        """
        result = await self._session.execute(
            select(SqlBuild).where(SqlBuild.id == build_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return Build.model_validate(row)

    async def _load_locked(self, build_id: int) -> SqlBuild | None:
        """Read a build row under a ``SELECT ... FOR UPDATE`` row lock.

        Every caller that reaches for this decides what to write from
        what it reads, and those two statements are not otherwise
        atomic. Under PostgreSQL's READ COMMITTED default a DELETE
        cancelling a build and a worker completing it could each pass
        their own status check against the same pre-race ``processing``
        snapshot, and the second UPDATE would then land on top of the
        first transaction's terminal status — losing it silently, and
        leaving a row that had already been cancelled claiming to be
        ``completed`` (review of PR #583, finding f1).

        Locking the row makes the loser block until the winner commits
        and then re-read what the winner actually wrote, so it can raise
        or stand down on the current status rather than a stale one. The
        lock is held to the end of the caller's transaction, which is
        what carries the guarantee across the read/write pair.

        ``populate_existing`` keeps that guarantee from resting on the
        identity map. Sessions are created with
        ``expire_on_commit=False``, so an instance this session loaded
        in an earlier transaction — the worker's pre-lock metadata read,
        say — would be returned with its old attributes rather than the
        locked row's. In practice the stores convert every row to a
        domain object and drop the ORM instance immediately, so the
        identity map (which holds only weak references) is usually empty
        by the time anyone asks again; relying on that is relying on
        garbage-collection timing for a correctness property. This asks
        for the locked row's values outright.
        """
        result = await self._session.execute(
            select(SqlBuild)
            .where(SqlBuild.id == build_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        return result.scalar_one_or_none()

    async def get_for_update(self, *, build_id: int) -> Build | None:
        """Fetch a build by internal ID, locking the row until commit.

        The read half of a read-then-write on a build's status: it
        returns the row as it stands *and* holds it, so the status the
        caller branches on cannot change under it before it writes. Use
        it wherever a decision is made from a build's status or
        ``date_deleted`` and acted on in the same transaction; use
        :meth:`get_by_id` for a plain look.

        Returns
        -------
        Build or None
            The locked build, or ``None`` if no such row exists (in
            which case nothing is locked).
        """
        row = await self._load_locked(build_id)
        if row is None:
            return None
        return Build.model_validate(row)

    async def get_by_public_id(
        self, *, project_id: int, public_id: int
    ) -> Build | None:
        """Fetch a build by project_id and public_id."""
        result = await self._session.execute(
            select(SqlBuild).where(
                SqlBuild.project_id == project_id,
                SqlBuild.public_id == public_id,
                SqlBuild.date_deleted.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return Build.model_validate(row)

    async def list_all_by_project_ids(
        self, project_ids: list[int]
    ) -> list[Build]:
        """List every non-deleted build across the given projects.

        Single-query batch over multiple projects, ordered by
        ``(project_id, id)`` for stable iteration. Used by the
        ``lifecycle_eval`` per-org worker to load every project's
        builds in one round-trip rather than N. Passing an empty
        ``project_ids`` returns ``[]`` without hitting the database.
        """
        if not project_ids:
            return []
        result = await self._session.execute(
            select(SqlBuild)
            .where(
                SqlBuild.project_id.in_(project_ids),
                SqlBuild.date_deleted.is_(None),
            )
            .order_by(SqlBuild.project_id, SqlBuild.id)
        )
        return [Build.model_validate(row) for row in result.scalars().all()]

    async def list_pending_older_than(
        self,
        *,
        project_id: int,
        git_ref: str,
        uploader: str,
        older_than: datetime,
    ) -> list[Build]:
        """Return stale ``pending`` builds matching the given filters.

        Filters by ``(project_id, git_ref, uploader)`` and returns rows
        whose ``date_created`` is strictly before ``older_than``. The
        keeper-sync engine uses this to find placeholder builds left
        behind by a run that crashed between placeholder creation and
        finalize, so they can be transitioned to ``failed`` before a
        retry creates a fresh placeholder. Soft-deleted rows are
        excluded.
        """
        result = await self._session.execute(
            select(SqlBuild).where(
                SqlBuild.project_id == project_id,
                SqlBuild.git_ref == git_ref,
                SqlBuild.uploader == uploader,
                SqlBuild.status == BuildStatus.pending,
                SqlBuild.date_created < older_than,
                SqlBuild.date_deleted.is_(None),
            )
        )
        return [Build.model_validate(row) for row in result.scalars().all()]

    async def get_completed_by_content_hash(
        self, *, project_id: int, content_hash: str
    ) -> Build | None:
        """Return the oldest non-deleted ``completed`` build with this hash.

        Used by the keeper-sync engine for dual-upload convergence:
        when an inbound LTD build's content hash matches a build that
        already exists in Docverse for the same project, the sync links
        its state row to that build instead of re-copying the same
        content into a fresh row.

        The match reaches across producers because both write the same
        server-computed manifest hash (see
        :mod:`docverse_server.domain.content_hash`): keeper-sync's
        copier as it copies, and the build-processing worker as it
        extracts a client-uploaded tarball. A build that arrived by
        direct Docverse upload is therefore a real candidate here.
        Until the worker computed that hash (DM-55762) such rows held
        the client's gzipped-tarball digest instead, so in practice
        only copier-produced builds could ever match.

        Restricting to ``completed`` is what makes the comparison
        sound rather than merely tidy: a row carries its true content
        identity only once the worker stamps it at completion, and
        before that it holds the placeholder or the deprecated
        transport digest. Soft-deleted rows are excluded as well, so
        the row linked to is canonical and stable.
        """
        result = await self._session.execute(
            select(SqlBuild)
            .where(
                SqlBuild.project_id == project_id,
                SqlBuild.content_hash == content_hash,
                SqlBuild.status == BuildStatus.completed,
                SqlBuild.date_deleted.is_(None),
            )
            .order_by(SqlBuild.date_created.asc(), SqlBuild.id.asc())
            .limit(1)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return Build.model_validate(row)

    async def get_latest_build_id_for_ref(
        self, *, project_id: int, git_ref: str
    ) -> int | None:
        """Return the max live build id for a ``(project_id, git_ref)`` pair.

        Used by the ``build_processing`` stale-build guard: a job whose
        build id is less than the latest live id for the same ref has
        been superseded and must skip its expensive work. Soft-deleted
        rows are excluded, so "latest" means the latest build that can
        still be published.

        Counting deleted rows (the original f9ab830 rule) was meant to
        stop a deletion mid-processing from making a superseded build
        look current, but it stranded the ref instead: deleting a newer
        build that had never been processed left the older one skipping
        against a tombstone, with no live build for the ref at all and
        nothing left to publish it (#575). A deleted build's own job is
        stopped at its source instead: ``build_processing``'s
        deleted-self guard (PRD #577) reads ``date_deleted`` before
        asking for the latest id and cancels the build rather than
        running it, so nothing processes "against" a deletion and this
        lookup has no reason to keep counting tombstones.
        """
        result = await self._session.execute(
            select(func.max(SqlBuild.id)).where(
                SqlBuild.project_id == project_id,
                SqlBuild.git_ref == git_ref,
                SqlBuild.date_deleted.is_(None),
            )
        )
        return result.scalar_one_or_none()

    async def list_by_project(
        self,
        project_id: int,
        *,
        cursor: BuildDateCreatedCursor | None = None,
        limit: int,
        status: BuildStatus | None = None,
    ) -> CountedPaginatedList[Build, BuildDateCreatedCursor]:
        """List non-deleted builds for a project with pagination."""
        stmt = select(SqlBuild).where(
            SqlBuild.project_id == project_id,
            SqlBuild.date_deleted.is_(None),
        )
        if status is not None:
            stmt = stmt.where(SqlBuild.status == status)
        runner = CountedPaginatedQueryRunner(
            entry_type=Build, cursor_type=BuildDateCreatedCursor
        )
        return await runner.query_object(
            self._session, stmt, cursor=cursor, limit=limit
        )

    @overload
    async def transition_status(
        self,
        *,
        build_id: int,
        new_status: BuildStatus,
        content_hash: str | None = None,
        only_from: None = None,
        org_slug: str | None = None,
        project_slug: str | None = None,
        edition_slug: str | None = None,
    ) -> Build: ...

    @overload
    async def transition_status(
        self,
        *,
        build_id: int,
        new_status: BuildStatus,
        content_hash: str | None = None,
        only_from: frozenset[BuildStatus],
        org_slug: str | None = None,
        project_slug: str | None = None,
        edition_slug: str | None = None,
    ) -> Build | None: ...

    async def transition_status(
        self,
        *,
        build_id: int,
        new_status: BuildStatus,
        content_hash: str | None = None,
        only_from: frozenset[BuildStatus] | None = None,
        org_slug: str | None = None,
        project_slug: str | None = None,
        edition_slug: str | None = None,
    ) -> Build | None:
        """Transition a build to a new status.

        Validates the transition is allowed. Sets ``date_uploaded`` on
        transition to ``processing`` and ``date_completed`` on entry to
        any terminal status — ``completed``, ``failed``, ``superseded``
        or ``cancelled``. The two never-published terminals are stamped
        for the same reason the other two are: the row is finished with,
        and a reader asking "when did a worker stop holding this?" needs
        an answer whether or not the build was published.

        ``content_hash`` is the server-computed content identity (see
        :mod:`docverse_server.domain.content_hash`) and may only
        accompany the transition to ``completed``: that is the first
        moment the content is both fully known and final. Writing it
        here rather than in a follow-up update means a row can never be
        observed as ``completed`` while still holding the pending
        hash — the client's transport digest, or the placeholder — which
        is what makes the content-hash lookup in
        :meth:`get_completed_by_content_hash` trustworthy. Omit it to
        leave whatever hash the row already carries in place, as
        keeper-sync does after its copier has written one.

        ``org_slug`` / ``project_slug`` / ``edition_slug`` are optional
        API-facing identifiers carried into :class:`InvalidBuildStateError`
        so a Sentry triager sees slugs rather than internal row ids.

        ``only_from`` turns the transition into a best-effort one: the
        locked read this method already takes decides whether the write
        happens at all, and a row outside the set — or gone — is left as
        it stands and reported with ``None`` rather than an exception.
        That is what the service's retirement helpers
        (:meth:`~docverse_server.services.build.BuildService.cancel_if_unfinished`
        and its siblings) need, and doing it here rather than in a
        caller's own pre-read is the difference between one
        ``SELECT ... FOR UPDATE`` on the row and two: the decision and
        the write are the same statement pair, so the lock is held for
        the shortest window that still makes them atomic. Omit it for
        the strict paths, which want :exc:`InvalidBuildStateError` and a
        Sentry event when the row is not where they think it is.

        Returns
        -------
        Build or None
            The transitioned build. ``None`` only when ``only_from`` was
            given and the row was outside it, or had vanished.

        Raises
        ------
        InvalidBuildStateError
            If the build is not found or the transition is not valid,
            and ``only_from`` was not given.
        ValueError
            If ``content_hash`` is passed with a target other than
            ``completed``.
        """
        # Caller misuse rather than a state problem, so it is checked
        # before any row is read and does not raise the Slack-routed
        # InvalidBuildStateError: no operator action can fix it.
        if content_hash is not None and new_status != BuildStatus.completed:
            msg = (
                f"content_hash may only be written on the transition to "
                f"{BuildStatus.completed.value!r}, not "
                f"{new_status.value!r}"
            )
            raise ValueError(msg)

        # Locked read: the transition check below and the write that
        # follows it have to be one atomic step, or two transactions
        # racing to a terminal status can both pass the check and then
        # overwrite each other. See :meth:`_load_locked`.
        row = await self._load_locked(build_id)
        if row is None:
            if only_from is not None:
                self._logger.info(
                    "Build row is gone; leaving its transition unwritten",
                    build_id=build_id,
                    target_status=new_status.value,
                )
                return None
            raise InvalidBuildStateError(
                target_state=new_status.value,
                org_slug=org_slug,
                project_slug=project_slug,
                edition_slug=edition_slug,
                message=f"Build id={build_id} not found",
            )

        current = BuildStatus(row.status)
        build_public_id = serialize_base32_id(row.public_id)
        if only_from is not None and current not in only_from:
            self._logger.info(
                "Build is outside this transition's starting statuses; "
                "leaving its status as it stands",
                build_id=build_id,
                build=build_public_id,
                status=current.value,
                target_status=new_status.value,
            )
            return None
        allowed = _VALID_TRANSITIONS.get(current, set())
        if new_status not in allowed:
            raise InvalidBuildStateError(
                current_state=current.value,
                target_state=new_status.value,
                build_public_id=build_public_id,
                org_slug=org_slug,
                project_slug=project_slug,
                edition_slug=edition_slug,
            )

        row.status = new_status
        now = datetime.now(tz=UTC)

        if new_status == BuildStatus.processing:
            row.date_uploaded = now
        elif new_status.is_terminal:
            row.date_completed = now

        if content_hash is not None:
            row.content_hash = content_hash

        # No ``refresh`` afterwards: ``_load_locked`` asked for the
        # row's values outright, every column written above was written
        # from Python, and nothing on ``builds`` has a server-side
        # ``onupdate``, so a re-read would return exactly what is
        # already loaded — a third statement inside the row lock for no
        # new information.
        await self._session.flush()
        return Build.model_validate(row)

    async def fail_stranded_processing(
        self, *, idle_after: timedelta
    ) -> list[Build]:
        """Fail ``processing`` builds no live queue job is working on.

        ``processing`` is supposed to mean "a worker is on it". A row
        left in that status with no ``queued`` or ``in_progress``
        ``queue_jobs`` row naming it has lost its worker and will never
        move on its own, so the ``build_processing`` reaper retires it
        here. Selects rows that are ``processing``, not soft-deleted,
        and whose ``date_uploaded`` is strictly before
        ``now - idle_after``, then transitions each to ``failed`` —
        which stamps ``date_completed`` like any other terminal entry.

        Always ``failed``, never ``superseded``: the sweep deliberately
        does not read ``queue_jobs.progress`` to reconstruct why the
        build stopped. A row it can see is one today's code stranded
        without recording an outcome, and guessing at intent from a job
        that may not even exist any more would make the status less
        trustworthy, not more. Operators re-upload; the reaper only
        clears the false "in flight" reading.

        The liveness test is deliberately not scoped to
        ``kind='build_processing'``: any live job naming the build is
        reason enough to leave it alone.

        Parameters
        ----------
        idle_after
            How long a build may sit ``processing`` before the sweep
            claims it. The cutoff is ``now - idle_after``, with ``now``
            read from the database via ``func.now()`` rather than the
            worker's clock — the three queue-job sweeps this one shares
            a transaction with derive their cutoffs the same way, so
            using one clock for all four keeps them from disagreeing at
            the boundary under skew. Rows uploaded at or after the
            cutoff are still within the reaper's patience window and are
            left alone, as are rows with no ``date_uploaded`` at all
            (the ``NULL`` comparison is never true).

        A row whose status changes between the candidate select and its
        transition — a DELETE-cancel committing in that window, say — is
        skipped rather than allowed to raise. The sweep shares the
        reaper's first transaction with the silent and orphan queue-job
        sweeps, so an :exc:`InvalidBuildStateError` escaping here would
        roll all three back and fail the whole tick over one build that
        somebody else had already retired. The skip is the right answer
        on its own terms too: the row is no longer stranded.

        Returns
        -------
        list of Build
            The builds this sweep transitioned to ``failed``, in id
            order. Empty when nothing was stranded.
        """
        now = (await self._session.execute(select(func.now()))).scalar_one()
        cutoff = now - idle_after
        live_job = select(SqlQueueJob.id).where(
            SqlQueueJob.build_id == SqlBuild.id,
            SqlQueueJob.status.in_(
                (JobStatus.queued.value, JobStatus.in_progress.value)
            ),
        )
        result = await self._session.execute(
            select(SqlBuild.id)
            .where(
                SqlBuild.status == BuildStatus.processing,
                SqlBuild.date_deleted.is_(None),
                SqlBuild.date_uploaded < cutoff,
                ~live_job.exists(),
            )
            .order_by(SqlBuild.id)
        )
        failed: list[Build] = []
        for build_id in result.scalars().all():
            try:
                failed.append(
                    await self.transition_status(
                        build_id=build_id, new_status=BuildStatus.failed
                    )
                )
            except InvalidBuildStateError:
                self._logger.info(
                    "Stranded build changed status during the sweep; "
                    "leaving it alone",
                    build_id=build_id,
                )
        return failed

    async def update_inventory(
        self,
        *,
        build_id: int,
        object_count: int,
        total_size_bytes: int,
        org_slug: str | None = None,
        project_slug: str | None = None,
        edition_slug: str | None = None,
    ) -> Build:
        """Update the inventory counts for a build."""
        result = await self._session.execute(
            select(SqlBuild).where(SqlBuild.id == build_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            raise InvalidBuildStateError(
                org_slug=org_slug,
                project_slug=project_slug,
                edition_slug=edition_slug,
                message=f"Build id={build_id} not found",
            )
        row.object_count = object_count
        row.total_size_bytes = total_size_bytes
        await self._session.flush()
        await self._session.refresh(row)
        return Build.model_validate(row)

    async def update_content_hash(
        self,
        *,
        build_id: int,
        content_hash: str,
        org_slug: str | None = None,
        project_slug: str | None = None,
        edition_slug: str | None = None,
    ) -> Build:
        """Overwrite the content hash on a build row.

        Used by the keeper-sync engine: the synced build is created
        with a placeholder hash before the bucket-to-bucket copy runs,
        then this method writes the deterministic manifest hash the
        copier produces. The regular upload-signalling path never
        needs this — the client supplies the hash up front.

        Only permitted while the build is ``pending``; once it has
        moved to ``processing`` or beyond, the hash is part of the
        committed build identity and must not change.

        Raises
        ------
        InvalidBuildStateError
            If the build is not found or is not in ``pending`` status.
        """
        result = await self._session.execute(
            select(SqlBuild).where(SqlBuild.id == build_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            raise InvalidBuildStateError(
                org_slug=org_slug,
                project_slug=project_slug,
                edition_slug=edition_slug,
                message=f"Build id={build_id} not found",
            )
        if row.status != BuildStatus.pending:
            current = BuildStatus(row.status)
            raise InvalidBuildStateError(
                current_state=current.value,
                target_state=BuildStatus.pending.value,
                build_public_id=serialize_base32_id(row.public_id),
                org_slug=org_slug,
                project_slug=project_slug,
                edition_slug=edition_slug,
                message=(
                    f"Cannot update content hash on build "
                    f"{serialize_base32_id(row.public_id)}: "
                    f"status is {current.value!r}, expected "
                    f"{BuildStatus.pending.value!r}"
                ),
            )
        row.content_hash = content_hash
        await self._session.flush()
        await self._session.refresh(row)
        return Build.model_validate(row)

    async def soft_delete(self, *, build_id: int) -> bool:
        """Soft-delete a build by setting date_deleted.

        Returns
        -------
        bool
            True if the build was soft-deleted, False if not found.
        """
        result = await self._session.execute(
            select(SqlBuild).where(
                SqlBuild.id == build_id,
                SqlBuild.date_deleted.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return False
        row.date_deleted = func.now()
        await self._session.flush()
        return True
