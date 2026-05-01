"""Service that records installation reachability on bindings."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import structlog

from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
)

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
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._binding_store = binding_store
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
            # No DB write — the binding cannot know about the install
            # before the operator registers it through the API. Log so
            # the delivery is visible in audit trails.
            self._logger.info(
                "Processed installation.created (no-op)",
                github_installation_id=installation_id,
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


def _coerce_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None
