"""Fetch a GitHub dashboard template and upsert it into storage."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from enum import StrEnum

import gidgethub
import httpx
import jwt.exceptions
import structlog

from docverse.domain.dashboard_github_template import (
    DashboardGitHubTemplateBinding,
)
from docverse.exceptions import NotFoundError
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
    GitHubTemplateFileInput,
    GitHubTemplateKey,
)
from docverse.storage.dashboard_templates.template_source import (
    parse_template_toml,
)
from docverse.storage.github import GitHubAppClient, GitHubTreeFetcher

__all__ = [
    "DashboardSyncResult",
    "DashboardSyncStatus",
    "DashboardTemplateSyncer",
]


_TEMPLATE_TOML_PATH = "template.toml"


class DashboardSyncStatus(StrEnum):
    """Outcome category of a sync attempt.

    ``succeeded`` covers both first-ever syncs and unchanged re-syncs;
    use :attr:`DashboardSyncResult.changed` to distinguish them.
    """

    succeeded = "succeeded"
    failed = "failed"


@dataclass(frozen=True)
class DashboardSyncResult:
    """Outcome of a :meth:`DashboardTemplateSyncer.sync` call.

    ``changed`` is ``True`` only on a successful sync that actually
    moved the bytes (first-ever sync or ETag-mismatch rewrite); it is
    ``False`` for ETag short-circuits and for any failed sync. Failed
    syncs leave ``github_template_id`` pointing at the previous
    template row when one was recorded by an earlier successful sync,
    and ``None`` when the binding has never synced successfully.
    """

    binding: DashboardGitHubTemplateBinding
    github_template_id: int | None
    changed: bool
    status: DashboardSyncStatus
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        """``True`` when the sync completed without errors."""
        return self.status is DashboardSyncStatus.succeeded


class DashboardTemplateSyncer:
    """Fetch one binding's GitHub tree and upsert it into storage.

    The syncer owns the fetch + validate + upsert steps. It does not
    acquire advisory locks or fan out dashboard rebuilds — the worker
    wraps the call to :meth:`sync` with ``LockKey.for_dashboard_template``
    and invokes :class:`DashboardRebuildFanout` on success.

    Failure semantics: a sync that cannot complete (GitHub error,
    missing or invalid ``template.toml``) flips the binding to
    ``last_sync_status="failed"`` with a non-empty ``last_sync_error``
    and leaves ``github_template_id`` pointing at the previously-synced
    template row. Dashboards keep rendering from the last-good bytes.
    The method *returns* a failed result rather than raising so the
    caller's surrounding ``session.begin()`` block commits the failure
    state instead of rolling it back.
    """

    def __init__(
        self,
        *,
        binding_store: DashboardGitHubTemplateBindingStore,
        template_store: DashboardGitHubTemplateStore,
        app_client: GitHubAppClient,
        http_client: httpx.AsyncClient,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._binding_store = binding_store
        self._template_store = template_store
        self._app_client = app_client
        self._http_client = http_client
        self._logger = logger

    async def sync(self, binding_id: int) -> DashboardSyncResult:
        """Fetch the template tree and upsert it into storage.

        Raises
        ------
        NotFoundError
            If ``binding_id`` does not exist.
        """
        binding = await self._binding_store.get_by_id(binding_id)
        if binding is None:
            msg = f"Dashboard template binding {binding_id} not found"
            raise NotFoundError(msg)

        logger = self._logger.bind(
            binding_id=binding.id,
            github_owner=binding.github_owner,
            github_repo=binding.github_repo,
            github_ref=binding.github_ref,
            root_path=binding.root_path,
        )

        try:
            auth = await self._app_client.get_installation_auth(
                owner=binding.github_owner, repo=binding.github_repo
            )
        except (
            httpx.HTTPError,
            gidgethub.GitHubException,
            jwt.exceptions.InvalidKeyError,
        ) as exc:
            return await self._record_failure(
                binding=binding,
                logger=logger,
                message=f"GitHub App authentication failed: {exc}",
            )

        fetcher = GitHubTreeFetcher(
            http_client=self._http_client, auth=auth, logger=logger
        )
        try:
            fetched = await fetcher.fetch(
                owner=binding.github_owner,
                repo=binding.github_repo,
                ref=binding.github_ref,
                root_path=binding.root_path,
            )
        except httpx.HTTPError as exc:
            return await self._record_failure(
                binding=binding,
                logger=logger,
                message=f"GitHub tree fetch failed: {exc}",
            )

        template_toml: bytes | None = None
        other_files: list[GitHubTemplateFileInput] = []
        for file in fetched.files:
            if file.path == _TEMPLATE_TOML_PATH:
                template_toml = file.data
                continue
            other_files.append(
                GitHubTemplateFileInput(
                    relative_path=file.path,
                    is_text=_is_text(file.data),
                    data=file.data,
                )
            )

        if template_toml is None:
            return await self._record_failure(
                binding=binding,
                logger=logger,
                message=(
                    "template.toml not found under root_path "
                    f"{binding.root_path!r}"
                ),
            )

        try:
            parse_template_toml(template_toml)
        except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
            return await self._record_failure(
                binding=binding,
                logger=logger,
                message=f"Invalid template.toml: {exc}",
            )

        # ETag caching: fall back to the tree SHA when GitHub omits the
        # ETag header, so the compare-and-upsert step still short-
        # circuits unchanged re-syncs.
        etag = fetched.etag or f"tree:{fetched.tree_sha}"
        upsert = await self._template_store.upsert(
            key=GitHubTemplateKey(
                github_owner=binding.github_owner,
                github_repo=binding.github_repo,
                github_ref=binding.github_ref,
                root_path=binding.root_path,
            ),
            commit_sha=fetched.commit_sha,
            etag=etag,
            template_toml=template_toml,
            files=other_files,
            github_owner_id=fetched.owner_id,
            github_repo_id=fetched.repo_id,
        )

        updated = await self._binding_store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="succeeded",
            last_sync_error=None,
            github_template_id=upsert.template.id,
            github_owner_id=fetched.owner_id,
            github_repo_id=fetched.repo_id,
            github_installation_id=auth.installation_id,
        )
        if updated is None:
            msg = (
                f"Binding {binding.id} disappeared mid-sync while recording "
                "success"
            )
            raise RuntimeError(msg)

        logger.info(
            "Dashboard template synced",
            github_template_id=upsert.template.id,
            changed=upsert.changed,
        )
        return DashboardSyncResult(
            binding=updated,
            github_template_id=upsert.template.id,
            changed=upsert.changed,
            status=DashboardSyncStatus.succeeded,
        )

    async def _record_failure(
        self,
        *,
        binding: DashboardGitHubTemplateBinding,
        logger: structlog.stdlib.BoundLogger,
        message: str,
    ) -> DashboardSyncResult:
        """Mark the binding failed without clearing ``github_template_id``."""
        logger.warning("Dashboard template sync failed", reason=message)
        updated = await self._binding_store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="failed",
            last_sync_error=message,
        )
        if updated is None:
            msg = (
                f"Binding {binding.id} disappeared mid-sync while recording "
                "failure"
            )
            raise RuntimeError(msg)
        return DashboardSyncResult(
            binding=updated,
            github_template_id=updated.github_template_id,
            changed=False,
            status=DashboardSyncStatus.failed,
            error=message,
        )


def _is_text(data: bytes) -> bool:
    """Return ``True`` when ``data`` decodes as UTF-8 text.

    Used as a best-effort classifier for the ``is_text`` column on
    ``dashboard_github_template_files``. Treating anything that
    round-trips through UTF-8 as text covers Jinja templates, CSS, JS,
    TOML, and SVG assets; binary images (PNG, WOFF2) fail the decode
    and land as ``is_text=False``.
    """
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True
