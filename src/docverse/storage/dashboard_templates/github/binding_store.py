"""Database operations for the ``dashboard_github_template_bindings`` table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.dashboard_github_template_binding import (
    SqlDashboardGitHubTemplateBinding,
)
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.dashboard_github_template import (
    DashboardGitHubTemplateBinding,
)

__all__ = [
    "DashboardGitHubTemplateBindingCreate",
    "DashboardGitHubTemplateBindingStore",
]


@dataclass(frozen=True)
class DashboardGitHubTemplateBindingCreate:
    """Inputs for creating a binding row."""

    org_id: int
    project_id: int | None
    github_owner: str
    github_repo: str
    github_ref: str
    root_path: str = "/"
    github_owner_id: int | None = None
    github_repo_id: int | None = None
    github_installation_id: int | None = None


def _select_with_queue_job() -> Any:
    """Return a SELECT that left-joins ``queue_jobs`` for the FK back-pointer.

    Materializing ``queue_jobs.public_id`` in the same query lets the
    response layer build the ``last_sync_queue_job_url`` without a
    second round-trip. The join is a left join because the FK is
    nullable (pre-first-enqueue rows and rows whose job was pruned).

    The ``Any`` return type sidesteps SQLAlchemy's typed-Select claim
    that ``public_id`` is ``int``: a left-join produces ``int | None``
    when there is no matching ``queue_jobs`` row, which the type stub
    cannot express.
    """
    return select(
        SqlDashboardGitHubTemplateBinding, SqlQueueJob.public_id
    ).outerjoin(
        SqlQueueJob,
        SqlDashboardGitHubTemplateBinding.last_sync_queue_job_id
        == SqlQueueJob.id,
    )


def _to_domain(
    row: SqlDashboardGitHubTemplateBinding,
    queue_job_public_id: int | None,
) -> DashboardGitHubTemplateBinding:
    """Build the domain object from a row + joined queue-job public_id."""
    binding = DashboardGitHubTemplateBinding.model_validate(row)
    if queue_job_public_id is None:
        return binding
    return binding.model_copy(
        update={
            "last_sync_queue_job_public_id": serialize_base32_id(
                queue_job_public_id
            ),
        }
    )


class DashboardGitHubTemplateBindingStore:
    """Direct database operations for dashboard GitHub template bindings."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(
        self, data: DashboardGitHubTemplateBindingCreate
    ) -> DashboardGitHubTemplateBinding:
        """Insert a new binding row."""
        row = SqlDashboardGitHubTemplateBinding(
            org_id=data.org_id,
            project_id=data.project_id,
            github_owner=data.github_owner,
            github_repo=data.github_repo,
            github_ref=data.github_ref,
            root_path=data.root_path,
            github_owner_id=data.github_owner_id,
            github_repo_id=data.github_repo_id,
            github_installation_id=data.github_installation_id,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        # A freshly-created binding has no queue job yet, so the FK is
        # NULL and there is no public_id to materialize.
        return DashboardGitHubTemplateBinding.model_validate(row)

    async def get_by_id(
        self, binding_id: int
    ) -> DashboardGitHubTemplateBinding | None:
        """Fetch a binding by internal ID."""
        result = await self._session.execute(
            _select_with_queue_job().where(
                SqlDashboardGitHubTemplateBinding.id == binding_id,
            )
        )
        row = result.one_or_none()
        if row is None:
            return None
        binding_row, public_id = row
        return _to_domain(binding_row, public_id)

    async def get_org_default(
        self, org_id: int
    ) -> DashboardGitHubTemplateBinding | None:
        """Fetch the org-default binding (``project_id IS NULL``)."""
        result = await self._session.execute(
            _select_with_queue_job().where(
                SqlDashboardGitHubTemplateBinding.org_id == org_id,
                SqlDashboardGitHubTemplateBinding.project_id.is_(None),
            )
        )
        row = result.one_or_none()
        if row is None:
            return None
        binding_row, public_id = row
        return _to_domain(binding_row, public_id)

    async def get_project_override(
        self, *, org_id: int, project_id: int
    ) -> DashboardGitHubTemplateBinding | None:
        """Fetch the project-specific binding for ``project_id``."""
        result = await self._session.execute(
            _select_with_queue_job().where(
                SqlDashboardGitHubTemplateBinding.org_id == org_id,
                SqlDashboardGitHubTemplateBinding.project_id == project_id,
            )
        )
        row = result.one_or_none()
        if row is None:
            return None
        binding_row, public_id = row
        return _to_domain(binding_row, public_id)

    async def list_by_github_template_id(
        self, github_template_id: int
    ) -> list[DashboardGitHubTemplateBinding]:
        """List every binding that currently points at a template row."""
        result = await self._session.execute(
            _select_with_queue_job().where(
                SqlDashboardGitHubTemplateBinding.github_template_id
                == github_template_id,
            )
        )
        return [
            _to_domain(binding_row, public_id)
            for binding_row, public_id in result
        ]

    async def list_by_repo_ref(
        self,
        *,
        github_owner: str,
        github_repo: str,
        github_ref: str,
    ) -> list[DashboardGitHubTemplateBinding]:
        """List every binding for a ``(owner, repo, ref)`` triple.

        Backed by the ``idx_dashboard_github_template_bindings_repo_ref``
        composite index. Matches both org-default and project-override
        bindings; the caller filters further (typically by intersecting
        the push event's changed-path set with each binding's
        ``root_path``).
        """
        result = await self._session.execute(
            _select_with_queue_job().where(
                SqlDashboardGitHubTemplateBinding.github_owner == github_owner,
                SqlDashboardGitHubTemplateBinding.github_repo == github_repo,
                SqlDashboardGitHubTemplateBinding.github_ref == github_ref,
            )
        )
        return [
            _to_domain(binding_row, public_id)
            for binding_row, public_id in result
        ]

    async def list_project_overrides_for_org(
        self, org_id: int
    ) -> list[DashboardGitHubTemplateBinding]:
        """List every project-override binding within an organization."""
        result = await self._session.execute(
            _select_with_queue_job().where(
                SqlDashboardGitHubTemplateBinding.org_id == org_id,
                SqlDashboardGitHubTemplateBinding.project_id.is_not(None),
            )
        )
        return [
            _to_domain(binding_row, public_id)
            for binding_row, public_id in result
        ]

    async def update_source(
        self,
        *,
        binding_id: int,
        github_owner: str,
        github_repo: str,
        github_ref: str,
        root_path: str,
    ) -> DashboardGitHubTemplateBinding | None:
        """Update the GitHub source coordinates of an existing binding."""
        row = await self._get_row(binding_id)
        if row is None:
            return None
        row.github_owner = github_owner
        row.github_repo = github_repo
        row.github_ref = github_ref
        row.root_path = root_path
        await self._session.flush()
        return await self.get_by_id(binding_id)

    async def update_sync_state(  # noqa: PLR0913
        self,
        *,
        binding_id: int,
        last_sync_status: str,
        last_sync_error: str | None = None,
        github_template_id: int | None = None,
        github_owner_id: int | None = None,
        github_repo_id: int | None = None,
        github_installation_id: int | None = None,
    ) -> DashboardGitHubTemplateBinding | None:
        """Update sync-state fields after a sync attempt.

        ``github_template_id`` and the three ``github_*_id`` fields are
        only assigned when provided; passing ``None`` leaves the
        existing values in place so a failed sync keeps the last-good
        template reference and previously-captured GitHub identities.
        """
        row = await self._get_row(binding_id)
        if row is None:
            return None
        row.last_sync_status = last_sync_status
        row.last_sync_error = last_sync_error
        if github_template_id is not None:
            row.github_template_id = github_template_id
        if github_owner_id is not None:
            row.github_owner_id = github_owner_id
        if github_repo_id is not None:
            row.github_repo_id = github_repo_id
        if github_installation_id is not None:
            row.github_installation_id = github_installation_id
        await self._session.flush()
        return await self.get_by_id(binding_id)

    async def set_last_sync_queue_job(
        self, *, binding_id: int, queue_job_id: int
    ) -> DashboardGitHubTemplateBinding | None:
        """Point the binding at a freshly-created ``dashboard_sync`` job.

        Called from ``DashboardSyncEnqueuer.enqueue`` immediately after
        the queue-job row exists, inside the same transaction. The
        previous FK is overwritten so the binding always points at the
        most recent sync attempt.

        ``date_updated`` is explicitly preserved — this column tracks
        operator-visible source-coordinate changes (owner/repo/ref/
        root_path), and a sync-bookkeeping write should not bump it.
        Including ``date_updated`` in the ``values()`` dict suppresses
        the column's ``onupdate=now()`` server-side default.
        """
        existing = await self._get_row(binding_id)
        if existing is None:
            return None
        stmt = (
            update(SqlDashboardGitHubTemplateBinding)
            .where(SqlDashboardGitHubTemplateBinding.id == binding_id)
            .values(
                last_sync_queue_job_id=queue_job_id,
                date_updated=SqlDashboardGitHubTemplateBinding.date_updated,
            )
        )
        await self._session.execute(stmt)
        await self._session.flush()
        return await self.get_by_id(binding_id)

    async def delete(self, binding_id: int) -> bool:
        """Delete a binding row.

        Returns ``True`` if a row was deleted, ``False`` if not found.
        """
        row = await self._get_row(binding_id)
        if row is None:
            return False
        await self._session.delete(row)
        await self._session.flush()
        return True

    async def _get_row(
        self, binding_id: int
    ) -> SqlDashboardGitHubTemplateBinding | None:
        result = await self._session.execute(
            select(SqlDashboardGitHubTemplateBinding).where(
                SqlDashboardGitHubTemplateBinding.id == binding_id,
            )
        )
        return result.scalar_one_or_none()
