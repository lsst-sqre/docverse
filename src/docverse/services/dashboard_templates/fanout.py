"""Fan out dashboard rebuilds for every project using a synced template."""

from __future__ import annotations

import structlog

from docverse.domain.queue import QueueJob
from docverse.services.dashboard.enqueue import DashboardBuildEnqueuer
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.project_store import ProjectStore

__all__ = ["DashboardRebuildFanout"]


class DashboardRebuildFanout:
    """Enqueue ``dashboard_build`` jobs for projects that use a template.

    The fan-out set is derived from the binding table plus the
    resolution order (project override → org default → built-in), not
    from a denormalized reverse-index column on projects. For a given
    synced template ``T`` the affected projects are:

    - every project whose override binding points at ``T``; and
    - every project whose org-default binding points at ``T`` and which
      does not have its own override binding (an override always wins,
      even one that points elsewhere).
    """

    def __init__(
        self,
        *,
        binding_store: DashboardGitHubTemplateBindingStore,
        project_store: ProjectStore,
        enqueuer: DashboardBuildEnqueuer,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._binding_store = binding_store
        self._project_store = project_store
        self._enqueuer = enqueuer
        self._logger = logger

    async def fan_out(self, github_template_id: int) -> list[QueueJob]:
        """Enqueue ``dashboard_build`` for every project using the template.

        Returns the enqueued queue jobs in the order they were created.
        Returns an empty list when no bindings reference the template.
        """
        bindings = await self._binding_store.list_by_github_template_id(
            github_template_id
        )
        if not bindings:
            return []

        affected: list[tuple[int, int]] = []
        seen: set[tuple[int, int]] = set()
        for binding in bindings:
            if binding.project_id is not None:
                key = (binding.org_id, binding.project_id)
                if key not in seen:
                    seen.add(key)
                    affected.append(key)
                continue

            # Org-default binding: every non-deleted project in the org
            # whose override (if any) does not shadow this default.
            overrides = (
                await self._binding_store.list_project_overrides_for_org(
                    binding.org_id
                )
            )
            override_ids = {
                o.project_id for o in overrides if o.project_id is not None
            }
            projects = await self._project_store.list_all_by_org(
                binding.org_id
            )
            for project in projects:
                if project.id in override_ids:
                    continue
                key = (binding.org_id, project.id)
                if key not in seen:
                    seen.add(key)
                    affected.append(key)

        jobs: list[QueueJob] = []
        for org_id, project_id in affected:
            job = await self._enqueuer.enqueue_for_project(
                org_id=org_id, project_id=project_id
            )
            if job is None:
                # Dedup: a dashboard_build is already queued or in
                # progress for this project. Skip the duplicate; the
                # in-flight job picks up the new template state when
                # it runs.
                continue
            jobs.append(job)
        self._logger.info(
            "Fanned out dashboard rebuilds",
            github_template_id=github_template_id,
            project_count=len(jobs),
        )
        return jobs
