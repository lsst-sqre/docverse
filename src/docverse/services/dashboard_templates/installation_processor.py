"""Service that records installation reachability on bindings."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import structlog

from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.project_store import ProjectStore

__all__ = [
    "INSTALLATION_DELETED_REASON",
    "INSTALLATION_SUSPENDED_REASON",
    "InstallationEventProcessor",
]


# Machine-readable tags written to ``dashboard_github_template_bindings
# .last_sync_error``. ``installation.unsuspend`` clears only rows whose
# error matches ``INSTALLATION_SUSPENDED_REASON``, so the suspend and
# delete tags are distinct strings rather than a shared "installation".
INSTALLATION_SUSPENDED_REASON = "installation_suspended"
INSTALLATION_DELETED_REASON = "installation_deleted"


class InstallationEventProcessor:
    """Translate ``installation.*`` webhooks into reachability flips.

    GitHub fires an installation event whenever the Docverse GitHub
    App's install state on a tenant changes:

    - ``installation.created`` — log only. The binding side cannot
      know about the install before it is registered through the
      binding PUT, so created is a no-op until that PUT runs and the
      next sync captures the installation id.
    - ``installation.suspend`` — mark every binding keyed by the
      installation id as ``last_sync_status='failed'`` with
      :data:`INSTALLATION_SUSPENDED_REASON`. Next render falls back to
      the previously-cached content; the operator sees the failure on
      the binding response.
    - ``installation.deleted`` — same as suspend but with
      :data:`INSTALLATION_DELETED_REASON`. Distinct from suspend so
      a future ``installation.unsuspend`` cannot accidentally revive
      a binding whose installation is actually gone.
    - ``installation.unsuspend`` — clear the suspend flag on rows
      whose ``last_sync_error`` matches
      :data:`INSTALLATION_SUSPENDED_REASON`. Non-suspend failures
      (real syncer errors) are preserved so an unsuspend that races
      with a separate failure does not paper over the second one.

    The caller (the webhook handler) owns the surrounding transaction;
    the processor only flushes through store-level updates and never
    opens its own ``session.begin()``.
    """

    def __init__(
        self,
        *,
        binding_store: DashboardGitHubTemplateBindingStore,
        project_store: ProjectStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._binding_store = binding_store
        self._project_store = project_store
        self._logger = logger

    async def process(self, payload: Mapping[str, Any]) -> None:
        """Dispatch on the installation action."""
        action = payload.get("action")
        installation = payload.get("installation", {})
        installation_id = _coerce_int(installation.get("id"))

        if installation_id is None or not isinstance(action, str):
            self._logger.warning(
                "installation payload missing id or action",
                installation_id=installation_id,
                action=action,
            )
            return

        if action == "created":
            # Binding-side state is still a no-op: an org-default
            # binding cannot have referenced the install before the
            # operator registered it through the API. Projects, by
            # contrast, may already exist with structured
            # ``github_owner``/``github_repo`` columns waiting for an
            # installation to come into scope (PRD #346 user story
            # 12), so backfill them from the payload's repository
            # block here.
            projects_updated = await self._backfill_projects_for_repos(
                installation_id=installation_id,
                installation=installation,
                repos=payload.get("repositories"),
            )
            self._logger.info(
                "Processed installation.created",
                github_installation_id=installation_id,
                projects_updated=projects_updated,
            )
            return

        if action == "suspend":
            ids = (
                await self._binding_store.mark_unreachable_by_installation_id(
                    github_installation_id=installation_id,
                    reason=INSTALLATION_SUSPENDED_REASON,
                )
            )
            self._logger.info(
                "Processed installation.suspend",
                github_installation_id=installation_id,
                bindings_updated=len(ids),
            )
            return

        if action == "deleted":
            ids = (
                await self._binding_store.mark_unreachable_by_installation_id(
                    github_installation_id=installation_id,
                    reason=INSTALLATION_DELETED_REASON,
                )
            )
            self._logger.info(
                "Processed installation.deleted",
                github_installation_id=installation_id,
                bindings_updated=len(ids),
            )
            return

        if action == "unsuspend":
            store = self._binding_store
            ids = await store.clear_failure_by_installation_id_and_reason(
                github_installation_id=installation_id,
                reason=INSTALLATION_SUSPENDED_REASON,
            )
            self._logger.info(
                "Processed installation.unsuspend",
                github_installation_id=installation_id,
                bindings_updated=len(ids),
            )
            return

        self._logger.info(
            "Ignoring unknown installation action",
            github_installation_id=installation_id,
            action=action,
        )

    async def process_installation_repositories(
        self, payload: Mapping[str, Any]
    ) -> None:
        """Dispatch on the ``installation_repositories`` action.

        ``installation_repositories.added`` fires when an operator
        scopes an existing app installation to new repos after the
        fact; ``installation_repositories.removed`` fires on the
        symmetric removal. Only ``added`` writes to projects today —
        a removal does not in itself invalidate the captured ids
        (the repo still exists, just outside this installation's
        scope), and clobbering the columns there would lose stable
        keys the rename handler still relies on.
        """
        action = payload.get("action")
        installation = payload.get("installation", {})
        installation_id = _coerce_int(installation.get("id"))

        if installation_id is None or not isinstance(action, str):
            self._logger.warning(
                "installation_repositories payload missing id or action",
                installation_id=installation_id,
                action=action,
            )
            return

        if action == "added":
            projects_updated = await self._backfill_projects_for_repos(
                installation_id=installation_id,
                installation=installation,
                repos=payload.get("repositories_added"),
            )
            self._logger.info(
                "Processed installation_repositories.added",
                github_installation_id=installation_id,
                projects_updated=projects_updated,
            )
            return

        self._logger.info(
            "Ignoring installation_repositories action",
            github_installation_id=installation_id,
            action=action,
        )

    async def _backfill_projects_for_repos(
        self,
        *,
        installation_id: int,
        installation: Mapping[str, Any],
        repos: object,
    ) -> int:
        """Apply the three github_*_id columns to matching project rows.

        ``installation.account`` carries the installation's owner
        (``login`` + ``id``); the ``repositories`` (or
        ``repositories_added``) list carries one entry per repo with
        ``id`` and either ``name`` or ``full_name``. The payload is
        the only source needed — no follow-up GitHub API round-trip —
        because all three numeric ids land in the same delivery.

        Returns the count of project rows updated across every repo
        in the payload so the caller can log a single
        ``projects_updated=N`` summary.
        """
        account = installation.get("account") or {}
        owner = account.get("login")
        owner_id = _coerce_int(account.get("id"))
        if (
            not isinstance(owner, str)
            or owner_id is None
            or not isinstance(repos, list)
        ):
            return 0

        total = 0
        for repo in repos:
            if not isinstance(repo, Mapping):
                continue
            repo_id = _coerce_int(repo.get("id"))
            repo_name = repo.get("name") or _repo_name_from_full(
                repo.get("full_name"), owner=owner
            )
            if repo_id is None or not isinstance(repo_name, str):
                continue
            updated = await self._project_store.apply_installation_scope(
                installation_id=installation_id,
                owner=owner,
                owner_id=owner_id,
                repo=repo_name,
                repo_id=repo_id,
            )
            total += len(updated)
        return total


def _repo_name_from_full(full_name: object, *, owner: str) -> str | None:
    """Extract the repo segment from ``"owner/repo"`` payload strings.

    Some installation payloads carry only ``full_name`` rather than a
    bare ``name`` (and a few historical deliveries flip the structure
    entirely). Falling back to a manual split keeps the backfill
    robust against either shape without forcing the caller into a
    branch.
    """
    if not isinstance(full_name, str):
        return None
    prefix = f"{owner}/"
    if not full_name.lower().startswith(prefix.lower()):
        return None
    return full_name[len(prefix) :] or None


def _coerce_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None
