"""Service translating a ``repository.edited`` webhook into convergence."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import structlog

from docverse_server.services.default_branch import (
    DefaultBranchOutcome,
    DefaultBranchService,
    DefaultBranchTrigger,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore

__all__ = [
    "DefaultBranchEventProcessor",
    "DefaultBranchEventResult",
    "DefaultBranchProjectResult",
]


@dataclass(frozen=True, slots=True)
class DefaultBranchProjectResult:
    """What a delivery did to one project backed by the repository.

    Carries the slugs alongside the outcome because the handler's
    post-commit work — the ``edition_lifecycle`` event and the
    ``dashboard_build`` enqueue — is keyed on them, and the repo-keyed
    project lookup can span organizations.
    """

    org_slug: str
    project_slug: str
    outcome: DefaultBranchOutcome


@dataclass(frozen=True, slots=True)
class DefaultBranchEventResult:
    """Outcome of one :meth:`DefaultBranchEventProcessor.process` call.

    ``projects`` holds one entry per project the delivery was applied
    to, in the order the repo lookup returned them; empty when the
    delivery changed no default branch, was malformed, or names a
    repository no project is bound to.
    """

    projects: tuple[DefaultBranchProjectResult, ...] = ()


class DefaultBranchEventProcessor:
    """Apply a ``repository.edited`` default-branch change to its projects.

    GitHub sends ``repository.edited`` for any settings edit —
    description, homepage, topics — and names what moved in
    ``changes``. Only an edit whose ``changes`` carries
    ``default_branch`` concerns Docverse: the new branch is
    ``repository.default_branch`` and the old one
    ``changes.default_branch.from``, which is the evidence
    :class:`~docverse_server.services.default_branch.DefaultBranchService`
    needs to rewrite a ``__main`` still tracking it.

    Payload parsing is defensive, as in the other processors: a missing
    or wrong-shape field logs and returns an empty result, so the
    handler still answers 200 and GitHub does not redeliver a payload
    that will never parse. The caller (the webhook handler) owns the
    transaction.
    """

    def __init__(
        self,
        *,
        project_store: ProjectStore,
        org_store: OrganizationStore,
        default_branch_service: DefaultBranchService,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._project_store = project_store
        self._org_store = org_store
        self._service = default_branch_service
        self._logger = logger

    async def process(
        self, payload: Mapping[str, Any]
    ) -> DefaultBranchEventResult:
        """Converge every project backed by the edited repository.

        Projects are found by ``repository.id``, with the
        ``(owner, name)`` fallback for projects whose numeric id is
        still unresolved — the same lookup the ``delete`` processor
        uses (:meth:`ProjectStore.list_by_github_repo`).
        """
        changes = payload.get("changes")
        change = (
            changes.get("default_branch")
            if isinstance(changes, Mapping)
            else None
        )
        repo = payload.get("repository")
        if not isinstance(repo, Mapping):
            repo = {}
        owner, repo_name, repo_id = _repository_coordinates(repo)
        logger = self._logger.bind(
            github_owner=owner, github_repo=repo_name, github_repo_id=repo_id
        )
        if not isinstance(change, Mapping):
            logger.info(
                "Ignoring repository.edited without a default branch change",
                changed_fields=(
                    sorted(changes) if isinstance(changes, Mapping) else None
                ),
            )
            return DefaultBranchEventResult()

        old_default_branch = change.get("from")
        new_default_branch = repo.get("default_branch")
        if not (
            isinstance(new_default_branch, str)
            and new_default_branch
            and owner
            and repo_name
        ):
            logger.warning(
                "repository.edited payload missing default branch or repo",
                new_default_branch=new_default_branch,
            )
            return DefaultBranchEventResult()
        if not isinstance(old_default_branch, str):
            old_default_branch = None

        projects = await self._project_store.list_by_github_repo(
            repo_id=repo_id, owner=owner, repo=repo_name
        )
        if not projects:
            logger.info(
                "No projects match repository.edited",
                old_default_branch=old_default_branch,
                new_default_branch=new_default_branch,
            )
            return DefaultBranchEventResult()

        results: list[DefaultBranchProjectResult] = []
        org_slugs: dict[int, str] = {}
        for project in projects:
            outcome = await self._service.apply(
                project=project,
                default_branch=new_default_branch,
                trigger=DefaultBranchTrigger.webhook,
                old_default_branch=old_default_branch,
            )
            org_slug = org_slugs.get(project.org_id)
            if org_slug is None:
                org = await self._org_store.get_by_id(project.org_id)
                if org is None:
                    continue
                org_slug = org_slugs[project.org_id] = org.slug
            results.append(
                DefaultBranchProjectResult(
                    org_slug=org_slug,
                    project_slug=project.slug,
                    outcome=outcome,
                )
            )

        logger.info(
            "Processed repository.edited default branch change",
            old_default_branch=old_default_branch,
            new_default_branch=new_default_branch,
            projects_matched=len(projects),
            main_rewrites=sum(r.outcome.main_rewritten for r in results),
        )
        return DefaultBranchEventResult(projects=tuple(results))


def _repository_coordinates(
    repo: Mapping[str, Any],
) -> tuple[str | None, str | None, int | None]:
    """Read ``(owner, name, id)`` from a payload's ``repository`` block.

    The owner comes from ``owner.login`` (``owner.name`` on older
    payload shapes), falling back to ``full_name``; the id is accepted
    only as a genuine ``int``, since ``isinstance(True, int)`` would
    otherwise let a boolean through as a repository id.
    """
    full_name = repo.get("full_name")
    fallback_owner = fallback_name = None
    if isinstance(full_name, str) and "/" in full_name:
        fallback_owner, fallback_name = full_name.split("/", 1)
    owner_block = repo.get("owner")
    if not isinstance(owner_block, Mapping):
        owner_block = {}
    owner = owner_block.get("login") or owner_block.get("name")
    name = repo.get("name")
    repo_id = repo.get("id")
    return (
        owner if isinstance(owner, str) else fallback_owner,
        name if isinstance(name, str) else fallback_name,
        repo_id
        if isinstance(repo_id, int) and not isinstance(repo_id, bool)
        else None,
    )
