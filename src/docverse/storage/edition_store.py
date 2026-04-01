"""Database operations for the editions table."""

from __future__ import annotations

from typing import Any

import structlog
from safir.database import (
    CountedPaginatedList,
    CountedPaginatedQueryRunner,
    PaginationCursor,
)
from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session
from sqlalchemy.sql import func

from docverse.client.models import (
    EditionCreate,
    EditionKind,
    EditionUpdate,
    TrackingMode,
)
from docverse.dbschema.build import SqlBuild
from docverse.dbschema.edition import SqlEdition
from docverse.domain.edition import Edition
from docverse.domain.version import (
    EupsDailyVersion,
    EupsMajorVersion,
    EupsWeeklyVersion,
    LsstDocVersion,
    SemverVersion,
)


class EditionStore:
    """Direct database operations for editions."""

    def __init__(
        self,
        session: async_scoped_session[AsyncSession],
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    def _base_query(self) -> Select[tuple[SqlEdition, int | None, str | None]]:
        """Build the base query with optional build public_id + git_ref."""
        return select(  # type: ignore[return-value]
            SqlEdition,
            SqlBuild.public_id.label("current_build_public_id"),
            SqlBuild.git_ref.label("current_build_git_ref"),
        ).outerjoin(SqlBuild, SqlEdition.current_build_id == SqlBuild.id)

    def _column_query(self) -> Select[tuple[Any, ...]]:
        """Build a column-based query for paginated results.

        Returns all Edition domain fields as flat columns, including
        the joined build public_id. Suitable for use with
        ``query_row`` so the flat row validates directly into Edition.
        """
        return select(
            SqlEdition.id,
            SqlEdition.slug,
            SqlEdition.title,
            SqlEdition.project_id,
            SqlEdition.kind,
            SqlEdition.tracking_mode,
            SqlEdition.tracking_params,
            SqlEdition.current_build_id,
            SqlBuild.public_id.label("current_build_public_id"),
            SqlBuild.git_ref.label("current_build_git_ref"),
            SqlEdition.lifecycle_exempt,
            SqlEdition.date_created,
            SqlEdition.date_updated,
            SqlEdition.date_deleted,
        ).outerjoin(SqlBuild, SqlEdition.current_build_id == SqlBuild.id)

    def _validate(
        self,
        row: SqlEdition,
        build_public_id: int | None,
        build_git_ref: str | None = None,
    ) -> Edition:
        """Validate a row into an Edition domain model."""
        edition = Edition.model_validate(row)
        edition.current_build_public_id = build_public_id
        edition.current_build_git_ref = build_git_ref
        return edition

    async def create(self, *, project_id: int, data: EditionCreate) -> Edition:
        """Insert a new edition row."""
        row = SqlEdition(
            slug=data.slug,
            title=data.title,
            project_id=project_id,
            kind=data.kind,
            tracking_mode=data.tracking_mode,
            tracking_params=data.tracking_params,
            lifecycle_exempt=data.lifecycle_exempt,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return self._validate(row, None)

    async def create_internal(  # noqa: PLR0913
        self,
        *,
        project_id: int,
        slug: str,
        title: str,
        kind: EditionKind,
        tracking_mode: TrackingMode,
        tracking_params: dict[str, Any] | None = None,
        lifecycle_exempt: bool = False,
    ) -> Edition:
        """Insert an edition row, bypassing slug validation.

        Used for system-created editions like ``__main`` where the slug
        does not conform to the user-facing pattern constraints in
        ``EditionCreate``.
        """
        row = SqlEdition(
            slug=slug,
            title=title,
            project_id=project_id,
            kind=kind,
            tracking_mode=tracking_mode,
            tracking_params=tracking_params,
            lifecycle_exempt=lifecycle_exempt,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return self._validate(row, None)

    async def get_by_slug(
        self, *, project_id: int, slug: str
    ) -> Edition | None:
        """Fetch an edition by project_id and slug."""
        stmt = self._base_query().where(
            SqlEdition.project_id == project_id,
            SqlEdition.slug == slug,
            SqlEdition.date_deleted.is_(None),
        )
        result = await self._session.execute(stmt)
        row_tuple = result.one_or_none()
        if row_tuple is None:
            return None
        edition_row, build_public_id, build_git_ref = row_tuple
        return self._validate(edition_row, build_public_id, build_git_ref)

    async def list_by_project(
        self,
        project_id: int,
        *,
        cursor_type: type[PaginationCursor[Edition]],
        cursor: PaginationCursor[Edition] | None = None,
        limit: int,
        kind: EditionKind | None = None,
    ) -> CountedPaginatedList[Edition, PaginationCursor[Edition]]:
        """List non-deleted editions for a project with pagination."""
        stmt = self._column_query().where(
            SqlEdition.project_id == project_id,
            SqlEdition.date_deleted.is_(None),
        )
        if kind is not None:
            stmt = stmt.where(SqlEdition.kind == kind)
        runner = CountedPaginatedQueryRunner(
            entry_type=Edition, cursor_type=cursor_type
        )
        return await runner.query_row(
            self._session, stmt, cursor=cursor, limit=limit
        )

    async def update(
        self, *, project_id: int, slug: str, data: EditionUpdate
    ) -> Edition | None:
        """Update an edition by project_id and slug."""
        result = await self._session.execute(
            select(SqlEdition).where(
                SqlEdition.project_id == project_id,
                SqlEdition.slug == slug,
                SqlEdition.date_deleted.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        updates = data.model_dump(exclude_unset=True)
        for key, value in updates.items():
            setattr(row, key, value)
        await self._session.flush()
        await self._session.refresh(row)
        # Re-query to get current_build_public_id via join
        return await self.get_by_slug(project_id=project_id, slug=slug)

    async def set_current_build(
        self,
        *,
        edition_id: int,
        build_id: int,
        skip_date_guard: bool = False,
    ) -> Edition | None:
        """Set the current build for an edition.

        Compares the incoming build's ``date_created`` against the
        current build's ``date_created``.  If the edition already points
        to a build that is equally new or newer, the update is skipped
        and ``None`` is returned (stale-build guard per SQR-112).

        Parameters
        ----------
        edition_id
            The edition to update.
        build_id
            The build to point to.
        skip_date_guard
            When ``True``, bypass the date-based stale guard.  Used by
            version-based tracking modes where the version comparison
            in the service layer is the authoritative ordering.

        Returns
        -------
        Edition or None
            The updated edition, or ``None`` if the update was skipped
            because the edition already points to a newer build.
        """
        # Fetch edition row
        stmt = (
            select(
                SqlEdition,
                SqlBuild.date_created.label("current_build_date"),
            )
            .outerjoin(SqlBuild, SqlEdition.current_build_id == SqlBuild.id)
            .where(SqlEdition.id == edition_id)
        )
        result = await self._session.execute(stmt)
        row, current_build_date = result.one()

        if not skip_date_guard:
            # Fetch incoming build's date_created
            incoming_result = await self._session.execute(
                select(SqlBuild.date_created).where(SqlBuild.id == build_id)
            )
            incoming_date = incoming_result.scalar_one()

            # Stale-build guard: skip if current build is equally new or newer
            if (
                current_build_date is not None
                and current_build_date >= incoming_date
            ):
                return None

        row.current_build_id = build_id
        await self._session.flush()
        await self._session.refresh(row)
        # Re-query to get current_build_public_id + git_ref
        stmt2 = self._base_query().where(SqlEdition.id == edition_id)
        result2 = await self._session.execute(stmt2)
        edition_row, build_public_id, build_git_ref = result2.one()
        return self._validate(edition_row, build_public_id, build_git_ref)

    async def soft_delete(self, *, project_id: int, slug: str) -> bool:
        """Soft-delete an edition by setting date_deleted.

        Returns
        -------
        bool
            True if the edition was soft-deleted, False if not found.
        """
        result = await self._session.execute(
            select(SqlEdition).where(
                SqlEdition.project_id == project_id,
                SqlEdition.slug == slug,
                SqlEdition.date_deleted.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return False
        row.date_deleted = func.now()
        await self._session.flush()
        return True

    async def find_matching_editions(
        self,
        *,
        project_id: int,
        git_ref: str,
        alternate_name: str | None = None,
    ) -> list[Edition]:
        """Find editions that match a git_ref or alternate_name.

        Used by the build processing worker to determine which editions
        should be updated when a build completes.
        """
        conditions = [
            SqlEdition.project_id == project_id,
            SqlEdition.date_deleted.is_(None),
        ]

        stmt = self._base_query().where(*conditions).order_by(SqlEdition.slug)
        result = await self._session.execute(stmt)
        rows = result.all()

        matching = []
        for edition_row, build_public_id, build_git_ref in rows:
            edition = self._validate(
                edition_row, build_public_id, build_git_ref
            )
            if self._edition_matches(edition, git_ref, alternate_name):
                matching.append(edition)

        return matching

    @staticmethod
    def _edition_matches(  # noqa: PLR0911
        edition: Edition,
        git_ref: str,
        alternate_name: str | None,
    ) -> bool:
        """Test whether *edition* matches the given git ref."""
        mode = edition.tracking_mode
        params = edition.tracking_params or {}

        if mode == TrackingMode.git_ref:
            return alternate_name is None and params.get("git_ref") == git_ref

        if mode == TrackingMode.alternate_git_ref:
            return (
                alternate_name is not None
                and params.get("git_ref") == git_ref
                and params.get("alternate_name") == alternate_name
            )

        if mode in (
            TrackingMode.semver_release,
            TrackingMode.semver_major,
            TrackingMode.semver_minor,
        ):
            return _semver_matches(mode, params, git_ref)

        if mode == TrackingMode.eups_major_release:
            return EupsMajorVersion.parse(git_ref) is not None

        if mode == TrackingMode.eups_weekly_release:
            return EupsWeeklyVersion.parse(git_ref) is not None

        if mode == TrackingMode.eups_daily_release:
            return EupsDailyVersion.parse(git_ref) is not None

        if mode == TrackingMode.lsst_doc:
            return _lsst_doc_matches(edition, git_ref)

        return False


def _semver_matches(
    mode: TrackingMode,
    params: dict[str, Any],
    git_ref: str,
) -> bool:
    """Check whether *git_ref* matches a semver-based tracking mode."""
    v = SemverVersion.parse(git_ref)
    if v is None or v.prerelease is not None:
        return False
    if mode == TrackingMode.semver_release:
        return True
    if mode == TrackingMode.semver_major:
        return v.major == params.get("major_version")
    # semver_minor
    return v.major == params.get("major_version") and v.minor == params.get(
        "minor_version"
    )


def _lsst_doc_matches(edition: Edition, git_ref: str) -> bool:
    """Check whether *git_ref* matches an lsst_doc edition."""
    if LsstDocVersion.parse(git_ref) is not None:
        return True
    return git_ref == "main" and (
        edition.current_build_id is None
        or edition.current_build_git_ref == "main"
    )
