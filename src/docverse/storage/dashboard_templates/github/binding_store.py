"""Database operations for the ``dashboard_github_template_bindings`` table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.dashboard_github_template import (
    SqlDashboardGitHubTemplate,
)
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


def _select_with_joins() -> Any:
    """Return a SELECT that left-joins queue_jobs and template-content rows.

    Two left joins materialize the read-side fields the response layer
    needs without a second round-trip:

    * ``queue_jobs.public_id`` → builds ``last_sync_queue_job_url``.
    * ``dashboard_github_templates.commit_sha`` → exposes the latest
      synced commit on the binding response.

    Both joins are left joins because the FKs are nullable: pre-first-
    enqueue rows have no queue job, and pre-first-sync (or sync-failed)
    rows have no linked content row.

    The ``Any`` return type sidesteps SQLAlchemy's typed-Select claim
    that the joined columns are non-nullable: a left join produces
    ``int | None`` / ``str | None`` for unmatched rows, which the type
    stubs cannot express.
    """
    return (
        select(
            SqlDashboardGitHubTemplateBinding,
            SqlQueueJob.public_id,
            SqlDashboardGitHubTemplate.commit_sha,
        )
        .outerjoin(
            SqlQueueJob,
            SqlDashboardGitHubTemplateBinding.last_sync_queue_job_id
            == SqlQueueJob.id,
        )
        .outerjoin(
            SqlDashboardGitHubTemplate,
            SqlDashboardGitHubTemplateBinding.github_template_id
            == SqlDashboardGitHubTemplate.id,
        )
    )


def _to_domain(
    row: SqlDashboardGitHubTemplateBinding,
    queue_job_public_id: int | None,
    commit_sha: str | None,
) -> DashboardGitHubTemplateBinding:
    """Build the domain object from a row + joined read-side columns."""
    binding = DashboardGitHubTemplateBinding.model_validate(row)
    updates: dict[str, Any] = {}
    if queue_job_public_id is not None:
        updates["last_sync_queue_job_public_id"] = serialize_base32_id(
            queue_job_public_id
        )
    if commit_sha is not None:
        updates["commit_sha"] = commit_sha
    if not updates:
        return binding
    return binding.model_copy(update=updates)


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
            _select_with_joins().where(
                SqlDashboardGitHubTemplateBinding.id == binding_id,
            )
        )
        row = result.one_or_none()
        if row is None:
            return None
        binding_row, public_id, commit_sha = row
        return _to_domain(binding_row, public_id, commit_sha)

    async def get_org_default(
        self, org_id: int
    ) -> DashboardGitHubTemplateBinding | None:
        """Fetch the org-default binding (``project_id IS NULL``)."""
        result = await self._session.execute(
            _select_with_joins().where(
                SqlDashboardGitHubTemplateBinding.org_id == org_id,
                SqlDashboardGitHubTemplateBinding.project_id.is_(None),
            )
        )
        row = result.one_or_none()
        if row is None:
            return None
        binding_row, public_id, commit_sha = row
        return _to_domain(binding_row, public_id, commit_sha)

    async def get_project_override(
        self, *, org_id: int, project_id: int
    ) -> DashboardGitHubTemplateBinding | None:
        """Fetch the project-specific binding for ``project_id``."""
        result = await self._session.execute(
            _select_with_joins().where(
                SqlDashboardGitHubTemplateBinding.org_id == org_id,
                SqlDashboardGitHubTemplateBinding.project_id == project_id,
            )
        )
        row = result.one_or_none()
        if row is None:
            return None
        binding_row, public_id, commit_sha = row
        return _to_domain(binding_row, public_id, commit_sha)

    async def list_by_github_template_id(
        self, github_template_id: int
    ) -> list[DashboardGitHubTemplateBinding]:
        """List every binding that currently points at a template row."""
        result = await self._session.execute(
            _select_with_joins().where(
                SqlDashboardGitHubTemplateBinding.github_template_id
                == github_template_id,
            )
        )
        return [
            _to_domain(binding_row, public_id, commit_sha)
            for binding_row, public_id, commit_sha in result
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
            _select_with_joins().where(
                SqlDashboardGitHubTemplateBinding.github_owner == github_owner,
                SqlDashboardGitHubTemplateBinding.github_repo == github_repo,
                SqlDashboardGitHubTemplateBinding.github_ref == github_ref,
            )
        )
        return [
            _to_domain(binding_row, public_id, commit_sha)
            for binding_row, public_id, commit_sha in result
        ]

    async def list_by_repo_id_and_ref(
        self,
        *,
        github_repo_id: int,
        github_ref: str,
    ) -> list[DashboardGitHubTemplateBinding]:
        """List every binding pinned to a stable GitHub repo ID + ref.

        Backed by the
        ``idx_dashboard_github_template_bindings_repo_id_ref`` composite
        index. Used by the push event processor as the primary
        rename-robust lookup: bindings keep matching their upstream
        repository even after a GitHub rename or transfer because the
        numeric ID is invariant.
        """
        result = await self._session.execute(
            select(SqlDashboardGitHubTemplateBinding).where(
                SqlDashboardGitHubTemplateBinding.github_repo_id
                == github_repo_id,
                SqlDashboardGitHubTemplateBinding.github_ref == github_ref,
            )
        )
        return [
            DashboardGitHubTemplateBinding.model_validate(row)
            for row in result.scalars().all()
        ]

    async def list_unsynced_by_repo_ref(
        self,
        *,
        github_owner: str,
        github_repo: str,
        github_ref: str,
    ) -> list[DashboardGitHubTemplateBinding]:
        """List ``(owner, repo, ref)`` matches that have no captured repo ID.

        The push event processor unions this with
        :meth:`list_by_repo_id_and_ref` to cover bindings that have
        never completed a successful sync (and therefore lack a
        ``github_repo_id``). Already-synced bindings whose name
        coincidentally matches a different repo are excluded — the
        ID lookup is the authoritative match for those.
        """
        result = await self._session.execute(
            select(SqlDashboardGitHubTemplateBinding).where(
                SqlDashboardGitHubTemplateBinding.github_owner == github_owner,
                SqlDashboardGitHubTemplateBinding.github_repo == github_repo,
                SqlDashboardGitHubTemplateBinding.github_ref == github_ref,
                SqlDashboardGitHubTemplateBinding.github_repo_id.is_(None),
            )
        )
        return [
            DashboardGitHubTemplateBinding.model_validate(row)
            for row in result.scalars().all()
        ]

    async def list_project_overrides_for_org(
        self, org_id: int
    ) -> list[DashboardGitHubTemplateBinding]:
        """List every project-override binding within an organization."""
        result = await self._session.execute(
            _select_with_joins().where(
                SqlDashboardGitHubTemplateBinding.org_id == org_id,
                SqlDashboardGitHubTemplateBinding.project_id.is_not(None),
            )
        )
        return [
            _to_domain(binding_row, public_id, commit_sha)
            for binding_row, public_id, commit_sha in result
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

    async def update_sync_state(
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

    async def rename_repo_by_repo_id(
        self,
        *,
        github_repo_id: int,
        new_repo: str,
    ) -> list[int]:
        """Rewrite ``github_repo`` on all bindings keyed by stable repo ID.

        Used by the ``repository.renamed`` webhook handler: any binding
        whose first sync captured ``github_repo_id`` matches here, even
        when its display name is now stale. ``date_updated`` is
        preserved because operator-visible source-coordinate writes
        come through PUT, not through GitHub-side metadata sync.
        """
        stmt = (
            update(SqlDashboardGitHubTemplateBinding)
            .where(
                SqlDashboardGitHubTemplateBinding.github_repo_id
                == github_repo_id,
            )
            .values(
                github_repo=new_repo,
                date_updated=SqlDashboardGitHubTemplateBinding.date_updated,
            )
            .returning(SqlDashboardGitHubTemplateBinding.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return [row[0] for row in result.all()]

    async def rename_repo_for_unsynced_by_old_name(
        self,
        *,
        github_owner: str,
        old_repo: str,
        new_repo: str,
    ) -> list[int]:
        """Rewrite ``github_repo`` on un-synced bindings matching old name.

        Fallback for bindings registered via PUT but never synced —
        ``github_repo_id IS NULL``, so ID-keyed matching cannot reach
        them. The owner-side filter pins the update to the namespace
        where the rename actually happened so a same-name binding in
        another GitHub owner is not collateral damage.
        """
        stmt = (
            update(SqlDashboardGitHubTemplateBinding)
            .where(
                SqlDashboardGitHubTemplateBinding.github_owner == github_owner,
                SqlDashboardGitHubTemplateBinding.github_repo == old_repo,
                SqlDashboardGitHubTemplateBinding.github_repo_id.is_(None),
            )
            .values(
                github_repo=new_repo,
                date_updated=SqlDashboardGitHubTemplateBinding.date_updated,
            )
            .returning(SqlDashboardGitHubTemplateBinding.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return [row[0] for row in result.all()]

    async def transfer_repo_by_repo_id(
        self,
        *,
        github_repo_id: int,
        new_owner: str,
        new_owner_id: int,
        new_repo: str,
    ) -> list[int]:
        """Rewrite owner + repo strings + ``github_owner_id`` on transfer.

        ``repository.transferred`` payloads carry the same ``repository
        .id`` but a new ``repository.owner`` (login + id) and may
        carry a new ``repository.name`` if the transfer was followed by
        a rename. All four columns flip together so a subsequent push
        from the new namespace matches the binding by stable repo ID
        and the display name does not lag.
        """
        stmt = (
            update(SqlDashboardGitHubTemplateBinding)
            .where(
                SqlDashboardGitHubTemplateBinding.github_repo_id
                == github_repo_id,
            )
            .values(
                github_owner=new_owner,
                github_owner_id=new_owner_id,
                github_repo=new_repo,
                date_updated=SqlDashboardGitHubTemplateBinding.date_updated,
            )
            .returning(SqlDashboardGitHubTemplateBinding.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return [row[0] for row in result.all()]

    async def rename_owner_by_owner_id(
        self,
        *,
        github_owner_id: int,
        new_owner: str,
    ) -> list[int]:
        """Rewrite ``github_owner`` on all bindings keyed by owner ID.

        The ``organization.renamed`` webhook handler uses this for
        synced bindings. ``github_owner_id`` is the stable handle —
        the org's display login is the only field that flips on a
        rename.
        """
        stmt = (
            update(SqlDashboardGitHubTemplateBinding)
            .where(
                SqlDashboardGitHubTemplateBinding.github_owner_id
                == github_owner_id,
            )
            .values(
                github_owner=new_owner,
                date_updated=SqlDashboardGitHubTemplateBinding.date_updated,
            )
            .returning(SqlDashboardGitHubTemplateBinding.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return [row[0] for row in result.all()]

    async def rename_owner_for_unsynced_by_old_login(
        self,
        *,
        old_login: str,
        new_owner: str,
    ) -> list[int]:
        """Rewrite ``github_owner`` on un-synced bindings matching old login.

        Fallback for bindings whose ``github_owner_id`` was never
        captured. Restricted to ``github_owner_id IS NULL`` so a
        coincidentally-named org (different numeric id, same login at
        some past point) cannot be moved by a rename event aimed at
        a different namespace.
        """
        stmt = (
            update(SqlDashboardGitHubTemplateBinding)
            .where(
                SqlDashboardGitHubTemplateBinding.github_owner == old_login,
                SqlDashboardGitHubTemplateBinding.github_owner_id.is_(None),
            )
            .values(
                github_owner=new_owner,
                date_updated=SqlDashboardGitHubTemplateBinding.date_updated,
            )
            .returning(SqlDashboardGitHubTemplateBinding.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return [row[0] for row in result.all()]

    async def mark_unreachable_by_installation_id(
        self,
        *,
        github_installation_id: int,
        reason: str,
    ) -> list[int]:
        """Mark all bindings under an installation as ``failed``.

        Used for ``installation.deleted`` and ``installation.suspend``.
        ``reason`` lands in ``last_sync_error`` as a machine-readable
        tag (e.g. ``installation_suspended``) so the
        ``installation.unsuspend`` clearer can target the same set of
        rows. ``date_updated`` is preserved — installation-state flips
        are not source-coordinate edits.
        """
        stmt = (
            update(SqlDashboardGitHubTemplateBinding)
            .where(
                SqlDashboardGitHubTemplateBinding.github_installation_id
                == github_installation_id,
            )
            .values(
                last_sync_status="failed",
                last_sync_error=reason,
                date_updated=SqlDashboardGitHubTemplateBinding.date_updated,
            )
            .returning(SqlDashboardGitHubTemplateBinding.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return [row[0] for row in result.all()]

    async def clear_failure_by_installation_id_and_reason(
        self,
        *,
        github_installation_id: int,
        reason: str,
    ) -> list[int]:
        """Clear a previously-recorded installation-failure flag.

        ``installation.unsuspend`` uses this to reverse the
        ``installation_suspended`` mark left by ``installation.suspend``.
        The ``reason`` filter prevents the unsuspend from clobbering
        a different failure (e.g. a real GitHub 5xx from the syncer)
        that happened to land between the suspend and the unsuspend.
        """
        stmt = (
            update(SqlDashboardGitHubTemplateBinding)
            .where(
                SqlDashboardGitHubTemplateBinding.github_installation_id
                == github_installation_id,
                SqlDashboardGitHubTemplateBinding.last_sync_status == "failed",
                SqlDashboardGitHubTemplateBinding.last_sync_error == reason,
            )
            .values(
                last_sync_status="pending",
                last_sync_error=None,
                date_updated=SqlDashboardGitHubTemplateBinding.date_updated,
            )
            .returning(SqlDashboardGitHubTemplateBinding.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return [row[0] for row in result.all()]

    async def _get_row(
        self, binding_id: int
    ) -> SqlDashboardGitHubTemplateBinding | None:
        result = await self._session.execute(
            select(SqlDashboardGitHubTemplateBinding).where(
                SqlDashboardGitHubTemplateBinding.id == binding_id,
            )
        )
        return result.scalar_one_or_none()
