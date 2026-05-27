"""Service that soft-deletes draft editions on a GitHub ``delete`` event."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import structlog

from docverse.client.models.dashboard_template import normalize_github_ref
from docverse.domain.project import Project
from docverse.services.edition_publishing import EditionPublishingService
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

__all__ = [
    "AffectedProject",
    "RefDeletedResult",
    "RefDeletedWebhookProcessor",
]


@dataclass(frozen=True)
class AffectedProject:
    """An (org_slug, project_slug) pair the handler should rebuild.

    Paired because :func:`try_enqueue_dashboard_build_by_slug` needs
    both, and the webhook's repo-keyed lookup
    (:meth:`ProjectStore.list_by_github_repo`) does not filter by org —
    matched projects can in principle span orgs, so the handler cannot
    derive ``org_slug`` from a single ambient value the way the daily
    audit's per-org worker can.
    """

    org_slug: str
    project_slug: str


@dataclass(frozen=True)
class RefDeletedResult:
    """Outcome of a :meth:`RefDeletedWebhookProcessor.process` call.

    ``deleted_edition_ids`` is the list of edition ids the processor
    soft-deleted on this delivery, in the order the per-project sweep
    visited them. ``affected_projects`` is the list of (org_slug,
    project_slug) pairs that had at least one edition soft-deleted,
    each pair appearing once even when multiple editions on the same
    project matched the deleted ref. The webhook handler uses this
    list to enqueue one ``dashboard_build`` per affected project
    post-commit, mirroring the daily ``git_ref_audit``'s
    ``projects_with_deletes`` shape. Empty on a non-match path
    (malformed payload, non-branch/tag ``ref_type``, repo with no
    bound project, ref no draft edition tracks).
    """

    deleted_edition_ids: list[int]
    affected_projects: list[AffectedProject]


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
        org_store: OrganizationStore,
        publishing_service: EditionPublishingService,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._project_store = project_store
        self._edition_store = edition_store
        self._org_store = org_store
        self._publishing_service = publishing_service
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
            return RefDeletedResult(
                deleted_edition_ids=[], affected_projects=[]
            )

        if not (isinstance(ref, str) and owner and repo_name):
            self._logger.warning(
                "Delete payload missing ref/owner/repo",
                ref=ref,
                owner=owner,
                repo=repo_name,
                ref_type=ref_type,
            )
            return RefDeletedResult(
                deleted_edition_ids=[], affected_projects=[]
            )

        normalized_ref = normalize_github_ref(ref)

        # GitHub ``delete`` payloads reliably populate
        # ``repository.id``, so any project that has resolved its
        # ``github_repo_id`` is reachable via the store's repo-id path.
        # The name fallback in
        # :meth:`ProjectStore.list_by_github_repo` is restricted to
        # ``github_repo_id IS NULL`` rows by design (a coincidentally-
        # same-named row at a different numeric id must not match) —
        # the store docstring explains the restriction; this note
        # surfaces the invariant at the call site.
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
            return RefDeletedResult(
                deleted_edition_ids=[], affected_projects=[]
            )

        deleted_ids: list[int] = []
        affected_projects: list[AffectedProject] = []
        # Cache org_id -> org_slug across iterations: in the typical
        # case every matched project sits in one org and one lookup
        # serves the whole sweep; a multi-org match (rare — same repo
        # backing project slugs in distinct orgs) still pays at most
        # one lookup per unique org_id.
        org_slug_cache: dict[int, str] = {}
        for project in projects:
            project_deleted_ids = await self._sweep_project(
                project=project,
                normalized_ref=normalized_ref,
                ref_type=ref_type,
            )
            if not project_deleted_ids:
                continue
            deleted_ids.extend(project_deleted_ids)
            org_slug = await self._resolve_org_slug(
                org_id=project.org_id, cache=org_slug_cache
            )
            if org_slug is None:
                continue
            affected_projects.append(
                AffectedProject(org_slug=org_slug, project_slug=project.slug)
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
        return RefDeletedResult(
            deleted_edition_ids=deleted_ids,
            affected_projects=affected_projects,
        )

    async def _sweep_project(
        self,
        *,
        project: Project,
        normalized_ref: str,
        ref_type: str,
    ) -> list[int]:
        """Soft-delete + unpublish every draft tracking the deleted ref.

        Returns the ordered ids of the editions soft-deleted on this
        project (empty when nothing matched or every match was already
        soft-deleted by an in-flight delivery). ``unpublish`` runs
        inside the handler's open transaction; see
        :meth:`process`'s key-decision comment for why this matches the
        daily audit worker rather than the ``delete_edition`` handler.
        """
        editions = await self._edition_store.list_draft_editions_by_git_ref(
            project_id=project.id, git_ref=normalized_ref
        )
        deleted: list[int] = []
        for edition in editions:
            was_deleted = await self._edition_store.soft_delete(
                project_id=project.id, slug=edition.slug
            )
            if not was_deleted:
                continue
            deleted.append(edition.id)
            await self._publishing_service.unpublish(
                org_id=project.org_id,
                project_slug=project.slug,
                edition_slug=edition.slug,
            )
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
        return deleted

    async def _resolve_org_slug(
        self, *, org_id: int, cache: dict[int, str]
    ) -> str | None:
        """Return the org's slug, caching across the per-call sweep."""
        cached = cache.get(org_id)
        if cached is not None:
            return cached
        org = await self._org_store.get_by_id(org_id)
        if org is None:
            return None
        cache[org_id] = org.slug
        return org.slug


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
