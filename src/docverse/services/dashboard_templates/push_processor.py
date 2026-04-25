"""Service that turns a GitHub push event into ``dashboard_sync`` jobs."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import httpx
import structlog

from docverse.domain.queue import QueueJob
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.github import (
    GitHubAppClient,
    extract_changed_paths_from_push,
    fetch_changed_paths_from_compare,
)

from .enqueue import DashboardSyncEnqueuer

__all__ = ["PushEventProcessor"]


class PushEventProcessor:
    """Translate a GitHub ``push`` event into ``dashboard_sync`` enqueues.

    The processor is the bridge between the webhook handler and the
    sync worker. It does three things:

    1. Look up every binding pinned to ``(owner, repo, ref)`` of the
       push, using the ``idx_dashboard_github_template_bindings_repo_ref``
       composite index.
    2. Compute the effective changed-path set. Cheap path:
       :func:`extract_changed_paths_from_push` reads paths out of the
       payload's ``commits`` array. If the payload signals truncation
       (``size > len(commits)`` or ``len(commits) >= 20``), fall back to
       ``GET /repos/{owner}/{repo}/compare/{before}...{after}`` —
       authoritative when the payload is incomplete.
    3. Filter the bindings: keep only those whose ``root_path`` was
       actually touched. ``root_path == "/"`` (or empty after normalize)
       is a "whole repo" binding; any change matches it. For nested
       roots, a path matches when it starts with ``"{root}/"``.

    Surviving bindings each get a single ``dashboard_sync`` job
    enqueued via :class:`DashboardSyncEnqueuer`. The caller (the webhook
    handler) owns the surrounding transaction; the processor only
    flushes through the enqueuer's ``QueueJob`` writes and never opens
    its own ``session.begin()``.
    """

    def __init__(
        self,
        *,
        binding_store: DashboardGitHubTemplateBindingStore,
        enqueuer: DashboardSyncEnqueuer,
        app_client: GitHubAppClient,
        http_client: httpx.AsyncClient,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._binding_store = binding_store
        self._enqueuer = enqueuer
        self._app_client = app_client
        self._http_client = http_client
        self._logger = logger

    async def process(self, payload: Mapping[str, Any]) -> list[QueueJob]:
        """Enqueue ``dashboard_sync`` for every affected binding.

        Returns the list of enqueued jobs (empty when no bindings
        match the push's ``(owner, repo, ref)`` or when none of the
        matching bindings' ``root_path`` overlap the changed-path set).
        """
        repo = payload.get("repository") or {}
        owner_block = repo.get("owner") or {}
        owner = (
            owner_block.get("login")
            or owner_block.get("name")
            or _split_full_name(repo.get("full_name"))
        )
        repo_name = repo.get("name") or _repo_from_full_name(
            repo.get("full_name")
        )
        ref = payload.get("ref")
        if not (owner and repo_name and ref):
            self._logger.warning(
                "Push payload missing owner/repo/ref",
                owner=owner,
                repo=repo_name,
                ref=ref,
            )
            return []

        bindings = await self._binding_store.list_by_repo_ref(
            github_owner=owner, github_repo=repo_name, github_ref=ref
        )
        if not bindings:
            self._logger.debug(
                "No bindings match push event",
                github_owner=owner,
                github_repo=repo_name,
                github_ref=ref,
            )
            return []

        changed_paths = await self._resolve_changed_paths(
            payload, owner=owner, repo=repo_name
        )

        affected = [
            b
            for b in bindings
            if _root_path_matches(b.root_path, changed_paths)
        ]
        if not affected:
            self._logger.info(
                "Push event affected no binding root_paths",
                github_owner=owner,
                github_repo=repo_name,
                github_ref=ref,
                binding_count=len(bindings),
            )
            return []

        jobs: list[QueueJob] = []
        for binding in affected:
            job = await self._enqueuer.enqueue(binding.id)
            jobs.append(job)
        self._logger.info(
            "Enqueued dashboard_sync jobs from push event",
            github_owner=owner,
            github_repo=repo_name,
            github_ref=ref,
            enqueued=len(jobs),
            considered=len(bindings),
        )
        return jobs

    async def _resolve_changed_paths(
        self, payload: Mapping[str, Any], *, owner: str, repo: str
    ) -> list[str]:
        paths = extract_changed_paths_from_push(payload)
        if paths is not None:
            return paths

        before = payload.get("before")
        after = payload.get("after")
        if not (isinstance(before, str) and isinstance(after, str)):
            self._logger.warning(
                "Truncated push payload has no before/after SHAs",
                github_owner=owner,
                github_repo=repo,
            )
            return []

        auth = await self._app_client.get_installation_auth(
            owner=owner, repo=repo
        )
        return await fetch_changed_paths_from_compare(
            self._http_client,
            auth=auth,
            owner=owner,
            repo=repo,
            before=before,
            after=after,
        )


def _split_full_name(full_name: object) -> str | None:
    if isinstance(full_name, str) and "/" in full_name:
        return full_name.split("/", 1)[0]
    return None


def _repo_from_full_name(full_name: object) -> str | None:
    if isinstance(full_name, str) and "/" in full_name:
        return full_name.split("/", 1)[1]
    return None


def _normalize_root(root_path: str) -> str:
    return root_path.strip("/")


def _root_path_matches(root_path: str, changed_paths: Iterable[str]) -> bool:
    """Return ``True`` if any changed path is inside ``root_path``.

    ``root_path = "/"`` (or empty after stripping slashes) is the
    "whole repo" binding — any non-empty changed-path set matches.
    For a nested root like ``"templates/blue"``, a path matches when
    it starts with ``"templates/blue/"``.
    """
    normalized = _normalize_root(root_path)
    if not normalized:
        return any(True for _ in changed_paths)
    prefix = f"{normalized}/"
    return any(p.startswith(prefix) or p == normalized for p in changed_paths)
