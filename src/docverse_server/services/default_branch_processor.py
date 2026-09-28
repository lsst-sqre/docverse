"""Service translating a ``repository.edited`` webhook into convergence."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import structlog

from docverse_server.domain.project import Project
from docverse_server.services.default_branch import (
    DefaultBranchOutcome,
    DefaultBranchService,
    DefaultBranchTrigger,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore

__all__ = [
    "DefaultBranchEventProcessor",
    "DefaultBranchProjectResult",
    "DefaultBranchTarget",
]


@dataclass(frozen=True, slots=True)
class DefaultBranchTarget:
    """One project a ``repository.edited`` delivery converges, and on what.

    Returned by :meth:`DefaultBranchEventProcessor.resolve_targets` and
    handed back, one at a time, to
    :meth:`DefaultBranchEventProcessor.converge`. Carries the org slug
    alongside the project because the repo-keyed project lookup can
    span organizations and the handler's post-commit work is keyed on
    both slugs.
    """

    org_slug: str
    """Slug of the organization that owns :attr:`project`."""

    project: Project
    """The project bound to the edited repository."""

    default_branch: str
    """The repository's new default branch, ``repository.default_branch``."""

    old_default_branch: str | None
    """The branch it replaced, ``changes.default_branch.from``, if given."""


@dataclass(frozen=True, slots=True)
class DefaultBranchProjectResult:
    """What a delivery did to one project backed by the repository.

    Carries the slugs alongside the outcome because the handler's
    post-commit work — the ``edition_lifecycle`` event and the
    ``dashboard_build`` enqueue — is keyed on them.
    """

    org_slug: str
    project_slug: str
    outcome: DefaultBranchOutcome


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
    or wrong-shape field logs and returns no targets, so the handler
    still answers 200 and GitHub does not redeliver a payload that will
    never parse.

    The work is split in two so the caller (the webhook handler) can
    own one transaction per project, as the daily ``git_ref_audit``
    does: :meth:`resolve_targets` reads which projects the delivery
    reaches, inside a short read transaction, and :meth:`converge`
    applies the rule to one of them inside that project's own. The
    rule holds that project's ``__main`` ``EDITION_UPDATE`` lock while
    it writes, so a delivery-wide transaction would wait on each later
    project's lock while holding every earlier project's uncommitted
    rows.
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

    async def resolve_targets(
        self, payload: Mapping[str, Any]
    ) -> list[DefaultBranchTarget]:
        """Return every project a default-branch change converges.

        Projects are found by ``repository.id``, with the
        ``(owner, name)`` fallback for projects whose numeric id is
        still unresolved — the same lookup the ``delete`` processor
        uses (:meth:`ProjectStore.list_by_github_repo`). Read-only: the
        caller wraps it in a transaction that needs no commit.

        Returns
        -------
        list of DefaultBranchTarget
            One per project bound to the repository, in the order the
            repository lookup returned them. Empty, after an info or
            warning log saying why, when the edit changed something
            other than the default branch, the payload is malformed, or
            no project is bound to the repository.
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
            return []

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
            return []
        if not isinstance(old_default_branch, str):
            old_default_branch = None

        projects = await self._project_store.list_by_github_repo(
            repo_id=repo_id, owner=owner, repo=repo_name
        )
        targets: list[DefaultBranchTarget] = []
        org_slugs: dict[int, str] = {}
        for project in projects:
            org_slug = org_slugs.get(project.org_id)
            if org_slug is None:
                org = await self._org_store.get_by_id(project.org_id)
                if org is None:
                    continue
                org_slug = org_slugs[project.org_id] = org.slug
            targets.append(
                DefaultBranchTarget(
                    org_slug=org_slug,
                    project=project,
                    default_branch=new_default_branch,
                    old_default_branch=old_default_branch,
                )
            )
        if not targets:
            logger.info(
                "No projects match repository.edited",
                old_default_branch=old_default_branch,
                new_default_branch=new_default_branch,
            )
            return []

        logger.info(
            "Matched projects for repository.edited default branch change",
            old_default_branch=old_default_branch,
            new_default_branch=new_default_branch,
            projects_matched=len(targets),
        )
        return targets

    async def converge(
        self, target: DefaultBranchTarget
    ) -> DefaultBranchProjectResult:
        """Apply the delivery's default branch to one project.

        Runs :meth:`DefaultBranchService.apply` with the webhook's old
        default branch as its evidence that ``__main``'s ref is gone.
        The caller owns the transaction — one per target — and, once it
        commits, the post-commit work the result calls for.
        """
        outcome = await self._service.apply(
            project=target.project,
            default_branch=target.default_branch,
            trigger=DefaultBranchTrigger.webhook,
            old_default_branch=target.old_default_branch,
        )
        return DefaultBranchProjectResult(
            org_slug=target.org_slug,
            project_slug=target.project.slug,
            outcome=outcome,
        )


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
