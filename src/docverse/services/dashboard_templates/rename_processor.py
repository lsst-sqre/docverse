"""Service that rewrites display names on rename / transfer webhooks."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import structlog

from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
)

__all__ = ["RenameEventProcessor"]


class RenameEventProcessor:
    """Translate rename / transfer webhooks into display-name rewrites.

    GitHub rename and transfer events carry stable numeric IDs in the
    payload alongside the new display strings. The processor rewrites
    name columns on every binding and content row matching the stable
    ID, with a name-keyed fallback for rows whose first sync did not
    capture the ID (un-synced bindings).

    No syncs are enqueued — the synced bytes are unchanged by a name
    flip, only the strings that operators read and that the dedup-key
    on the content row uses for the next ETag short-circuit. The
    caller (the webhook handler) owns the surrounding transaction; the
    processor only flushes through store-level updates and never opens
    its own ``session.begin()``.
    """

    def __init__(
        self,
        *,
        binding_store: DashboardGitHubTemplateBindingStore,
        template_store: DashboardGitHubTemplateStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._binding_store = binding_store
        self._template_store = template_store
        self._logger = logger

    async def process_repository_renamed(
        self, payload: Mapping[str, Any]
    ) -> None:
        """Rewrite ``github_repo`` on rows for a renamed repository.

        Primary path: every binding and content row whose stable
        ``github_repo_id`` matches ``repository.id``. Fallback path:
        un-synced bindings matching ``(github_owner, old_name)`` —
        their numeric ID was never captured so the primary lookup
        cannot reach them.
        """
        repo = payload.get("repository", {})
        repo_id = _coerce_int(repo.get("id"))
        new_repo = repo.get("name")
        owner_block = repo.get("owner", {})
        owner = owner_block.get("login") or owner_block.get("name")
        old_repo = (
            payload.get("changes", {})
            .get("repository", {})
            .get("name", {})
            .get("from")
        )

        if not (isinstance(new_repo, str) and isinstance(old_repo, str)):
            self._logger.warning(
                "repository.renamed payload missing names",
                old_repo=old_repo,
                new_repo=new_repo,
            )
            return

        binding_ids: list[int] = []
        template_ids: list[int] = []
        unsynced_ids: list[int] = []

        if repo_id is not None:
            binding_ids = await self._binding_store.rename_repo_by_repo_id(
                github_repo_id=repo_id, new_repo=new_repo
            )
            template_ids = await self._template_store.rename_repo_by_repo_id(
                github_repo_id=repo_id, new_repo=new_repo
            )

        if isinstance(owner, str) and old_repo != new_repo:
            unsynced_ids = (
                await self._binding_store.rename_repo_for_unsynced_by_old_name(
                    github_owner=owner,
                    old_repo=old_repo,
                    new_repo=new_repo,
                )
            )

        self._logger.info(
            "Processed repository.renamed",
            github_repo_id=repo_id,
            github_owner=owner,
            old_repo=old_repo,
            new_repo=new_repo,
            bindings_updated=len(binding_ids),
            templates_updated=len(template_ids),
            bindings_updated_unsynced=len(unsynced_ids),
        )

    async def process_repository_transferred(
        self, payload: Mapping[str, Any]
    ) -> None:
        """Rewrite owner + repo strings + ``github_owner_id`` on transfer.

        Only the ID-keyed primary path is used: a transfer changes the
        owner namespace, so a name-only fallback could collide with a
        same-name binding under either the old or the new owner. The
        binding has to have been synced (``github_repo_id`` populated)
        for the transfer to land on it.
        """
        repo = payload.get("repository", {})
        repo_id = _coerce_int(repo.get("id"))
        new_repo = repo.get("name")
        owner_block = repo.get("owner", {})
        new_owner = owner_block.get("login") or owner_block.get("name")
        new_owner_id = _coerce_int(owner_block.get("id"))

        if repo_id is None or not isinstance(new_owner, str):
            self._logger.warning(
                "repository.transferred payload missing repo_id or owner",
                repo_id=repo_id,
                new_owner=new_owner,
            )
            return
        if new_owner_id is None or not isinstance(new_repo, str):
            self._logger.warning(
                "repository.transferred payload missing owner_id or new_repo",
                new_owner_id=new_owner_id,
                new_repo=new_repo,
            )
            return

        binding_ids = await self._binding_store.transfer_repo_by_repo_id(
            github_repo_id=repo_id,
            new_owner=new_owner,
            new_owner_id=new_owner_id,
            new_repo=new_repo,
        )
        template_ids = await self._template_store.transfer_repo_by_repo_id(
            github_repo_id=repo_id,
            new_owner=new_owner,
            new_owner_id=new_owner_id,
            new_repo=new_repo,
        )

        self._logger.info(
            "Processed repository.transferred",
            github_repo_id=repo_id,
            new_owner=new_owner,
            new_owner_id=new_owner_id,
            new_repo=new_repo,
            bindings_updated=len(binding_ids),
            templates_updated=len(template_ids),
        )

    async def process_organization_renamed(
        self, payload: Mapping[str, Any]
    ) -> None:
        """Rewrite ``github_owner`` on rows for a renamed organization.

        Primary path: every binding and content row whose
        ``github_owner_id`` matches ``organization.id``. Fallback:
        un-synced bindings matching the old login.
        """
        org_block = payload.get("organization", {})
        org_id = _coerce_int(org_block.get("id"))
        new_login = org_block.get("login")
        old_login = payload.get("changes", {}).get("login", {}).get("from")

        if not (isinstance(new_login, str) and isinstance(old_login, str)):
            self._logger.warning(
                "organization.renamed payload missing login fields",
                old_login=old_login,
                new_login=new_login,
            )
            return

        binding_ids: list[int] = []
        template_ids: list[int] = []
        unsynced_ids: list[int] = []

        if org_id is not None:
            binding_ids = await self._binding_store.rename_owner_by_owner_id(
                github_owner_id=org_id, new_owner=new_login
            )
            template_ids = await self._template_store.rename_owner_by_owner_id(
                github_owner_id=org_id, new_owner=new_login
            )

        if old_login != new_login:
            store = self._binding_store
            unsynced_ids = await store.rename_owner_for_unsynced_by_old_login(
                old_login=old_login, new_owner=new_login
            )

        self._logger.info(
            "Processed organization.renamed",
            github_owner_id=org_id,
            old_login=old_login,
            new_login=new_login,
            bindings_updated=len(binding_ids),
            templates_updated=len(template_ids),
            bindings_updated_unsynced=len(unsynced_ids),
        )


def _coerce_int(value: object) -> int | None:
    """Return ``value`` as ``int`` when it is a non-bool int, else ``None``.

    Mirrors the strict guard in :class:`PushEventProcessor`: GitHub
    sends numeric IDs as JSON ints, but a malformed payload that sent
    ``"id": true`` would otherwise leak ``1`` through as a real id
    because ``isinstance(True, int)`` is ``True`` in Python.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None
