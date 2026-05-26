"""Service that soft-deletes draft editions on a GitHub ``delete`` event."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import structlog

from docverse.client.models.dashboard_template import normalize_github_ref
from docverse.storage.edition_store import EditionStore
from docverse.storage.project_store import ProjectStore

__all__ = ["RefDeletedResult", "RefDeletedWebhookProcessor"]


@dataclass(frozen=True)
class RefDeletedResult:
    """Outcome of a :meth:`RefDeletedWebhookProcessor.process` call.

    ``deleted_edition_ids`` is the list of edition ids the processor
    soft-deleted on this delivery, in the order the per-project sweep
    visited them. Empty when nothing matched: a malformed payload, a
    non-branch/tag ``ref_type``, a repo with no bound project, or a
    ref no draft edition tracks. The webhook handler does not act on
    the list — it is returned for tests and any future caller (the
    daily audit reaches the same outcome through the lifecycle
    evaluator, not this processor).
    """

    deleted_edition_ids: list[int]


_BRANCH_OR_TAG = frozenset({"branch", "tag"})


class RefDeletedWebhookProcessor:
    """Translate a GitHub ``delete`` event into edition soft-deletes.

    The webhook fast path: a deleted branch or tag immediately retires
    every draft edition that tracked it. Sibling rules (release editions
    pinned to a tag, ``lifecycle_exempt`` drafts, soft-deleted rows) are
    filtered out server-side by
    :meth:`EditionStore.list_draft_editions_by_git_ref`, mirroring the
    daily ``git_ref_audit``'s candidate set so the two paths cannot
    disagree on what a missing ref means.

    This processor does **not** mint a :class:`LifecycleEvaluator`: the
    webhook delivers exactly one ref, so the predicate collapses to a
    one-ref filter that the evaluator's per-project context layer would
    only add ceremony to. The audit, which evaluates against a whole
    repository's ref set, is the right place for the evaluator's
    pure-function shape.

    The caller (the webhook handler) owns the surrounding transaction;
    the processor only flushes through store-level updates and never
    opens its own ``session.begin()``.
    """

    def __init__(
        self,
        *,
        project_store: ProjectStore,
        edition_store: EditionStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._project_store = project_store
        self._edition_store = edition_store
        self._logger = logger

    async def process(self, payload: Mapping[str, Any]) -> RefDeletedResult:
        """Soft-delete every draft edition tracking the deleted ref.

        The processor follows the same defensive-payload-parsing
        discipline as the existing push and rename processors: a
        missing or wrong-shape field logs a warning and returns an
        empty result so the handler can still answer 200 to GitHub.
        """
        ref_type = payload.get("ref_type")
        ref = payload.get("ref")
        repo = payload.get("repository") or {}
        owner_block = repo.get("owner") if isinstance(repo, Mapping) else None
        if not isinstance(owner_block, Mapping):
            owner_block = {}
        fallback_owner, fallback_repo = _split_full_name_tuple(
            repo.get("full_name") if isinstance(repo, Mapping) else None
        ) or (None, None)
        owner = (
            owner_block.get("login")
            or owner_block.get("name")
            or fallback_owner
        )
        repo_name = (
            repo.get("name") if isinstance(repo, Mapping) else None
        ) or fallback_repo
        repo_id = (
            _coerce_int(repo.get("id")) if isinstance(repo, Mapping) else None
        )

        if not isinstance(ref_type, str) or ref_type not in _BRANCH_OR_TAG:
            self._logger.info(
                "Ignoring delete event for non-branch/tag ref_type",
                ref_type=ref_type,
                github_owner=owner,
                github_repo=repo_name,
            )
            return RefDeletedResult(deleted_edition_ids=[])

        if not (isinstance(ref, str) and owner and repo_name):
            self._logger.warning(
                "Delete payload missing ref/owner/repo",
                ref=ref,
                owner=owner,
                repo=repo_name,
                ref_type=ref_type,
            )
            return RefDeletedResult(deleted_edition_ids=[])

        normalized_ref = normalize_github_ref(ref)

        projects = await self._project_store.list_by_github_repo(
            repo_id=repo_id, owner=owner, repo=repo_name
        )
        if not projects:
            self._logger.info(
                "No projects match delete event",
                github_owner=owner,
                github_repo=repo_name,
                github_repo_id=repo_id,
                github_ref=normalized_ref,
                ref_type=ref_type,
            )
            return RefDeletedResult(deleted_edition_ids=[])

        deleted_ids: list[int] = []
        for project in projects:
            editions = (
                await self._edition_store.list_draft_editions_by_git_ref(
                    project_id=project.id, git_ref=normalized_ref
                )
            )
            for edition in editions:
                deleted = await self._edition_store.soft_delete(
                    project_id=project.id, slug=edition.slug
                )
                if not deleted:
                    continue
                deleted_ids.append(edition.id)
                self._logger.info(
                    "Soft-deleted edition for deleted ref",
                    project_id=project.id,
                    project_slug=project.slug,
                    edition_id=edition.id,
                    edition_slug=edition.slug,
                    github_ref=normalized_ref,
                    ref_type=ref_type,
                    trigger="webhook",
                )

        self._logger.info(
            "Processed delete webhook",
            github_owner=owner,
            github_repo=repo_name,
            github_repo_id=repo_id,
            github_ref=normalized_ref,
            ref_type=ref_type,
            projects_matched=len(projects),
            editions_deleted=len(deleted_ids),
        )
        return RefDeletedResult(deleted_edition_ids=deleted_ids)


def _split_full_name_tuple(full_name: object) -> tuple[str, str] | None:
    if isinstance(full_name, str) and "/" in full_name:
        owner, repo = full_name.split("/", 1)
        return owner, repo
    return None


def _coerce_int(value: object) -> int | None:
    """Return ``value`` as ``int`` when it is a non-bool int, else ``None``.

    Mirrors :func:`docverse.services.dashboard_templates.push_processor
    ._coerce_int`: GitHub webhooks send numeric IDs as JSON ints, but the
    ``isinstance(True, int)`` quirk would otherwise leak a truth value
    through as the repo id.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None
