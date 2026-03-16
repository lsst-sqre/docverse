"""Database operations for the builds table."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session
from sqlalchemy.sql import func

from docverse.client.models import BuildCreate, BuildStatus
from docverse.dbschema.build import SqlBuild
from docverse.domain.base32id import (
    generate_base32_id,
    serialize_base32_id,
    validate_base32_id,
)
from docverse.domain.build import Build
from docverse.exceptions import InvalidBuildStateError

# Valid status transitions
_VALID_TRANSITIONS: dict[BuildStatus, set[BuildStatus]] = {
    BuildStatus.pending: {BuildStatus.processing, BuildStatus.failed},
    BuildStatus.processing: {BuildStatus.completed, BuildStatus.failed},
}


class BuildStore:
    """Direct database operations for builds."""

    def __init__(
        self,
        session: async_scoped_session[AsyncSession],
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(
        self,
        *,
        project_id: int,
        data: BuildCreate,
        uploader: str,
    ) -> Build:
        """Insert a new build row with status=pending."""
        public_id = validate_base32_id(generate_base32_id())
        staging_key = f"__staging/{serialize_base32_id(public_id)}.tar.gz"
        row = SqlBuild(
            public_id=public_id,
            project_id=project_id,
            git_ref=data.git_ref,
            alternate_name=data.alternate_name,
            content_hash=data.content_hash,
            status=BuildStatus.pending,
            staging_key=staging_key,
            uploader=uploader,
            annotations=data.annotations,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
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

    async def list_by_project(self, project_id: int) -> list[Build]:
        """List all non-deleted builds for a project."""
        result = await self._session.execute(
            select(SqlBuild)
            .where(
                SqlBuild.project_id == project_id,
                SqlBuild.date_deleted.is_(None),
            )
            .order_by(SqlBuild.date_created.desc())
        )
        rows = result.scalars().all()
        return [Build.model_validate(r) for r in rows]

    async def transition_status(
        self, *, build_id: int, new_status: BuildStatus
    ) -> Build:
        """Transition a build to a new status.

        Validates the transition is allowed. Sets ``date_uploaded`` on
        transition to ``processing`` and ``date_completed`` on transition
        to ``completed`` or ``failed``.

        Raises
        ------
        InvalidBuildStateError
            If the transition is not valid.
        """
        result = await self._session.execute(
            select(SqlBuild).where(SqlBuild.id == build_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            msg = f"Build {build_id} not found"
            raise InvalidBuildStateError(msg)

        current = BuildStatus(row.status)
        allowed = _VALID_TRANSITIONS.get(current, set())
        if new_status not in allowed:
            msg = (
                f"Cannot transition build {build_id} from "
                f"{current.value!r} to {new_status.value!r}"
            )
            raise InvalidBuildStateError(msg)

        row.status = new_status
        now = datetime.now(tz=UTC)

        if new_status == BuildStatus.processing:
            row.date_uploaded = now
        elif new_status in (BuildStatus.completed, BuildStatus.failed):
            row.date_completed = now

        await self._session.flush()
        await self._session.refresh(row)
        return Build.model_validate(row)

    async def update_inventory(
        self, *, build_id: int, object_count: int, total_size_bytes: int
    ) -> Build:
        """Update the inventory counts for a build."""
        result = await self._session.execute(
            select(SqlBuild).where(SqlBuild.id == build_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            msg = f"Build {build_id} not found"
            raise InvalidBuildStateError(msg)
        row.object_count = object_count
        row.total_size_bytes = total_size_bytes
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
