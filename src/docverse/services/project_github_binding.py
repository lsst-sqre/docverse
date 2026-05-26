"""Resolve a project's GitHub binding into an auth-ready record.

Two PRD #346 callers (the daily ``git_ref_audit`` worker) and one
PRD #332 caller (the proactive ``sync_project`` pre-fetch) share the
same pre-flight: take a ``project_id``, find its GitHub coordinates,
and decide whether to use installation auth, anonymous auth, or skip
the project entirely. Centralising that decision here keeps the
auth-source ladder ("installation > anonymous > skip") consistent
across the two paths.
"""

from __future__ import annotations

from dataclasses import dataclass

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.storage.github import GitHubAppClient, InstallationAuth
from docverse.storage.project_store import ProjectStore

__all__ = [
    "ProjectGitHubBindingResolver",
    "ResolvedProjectGitHubBinding",
]


@dataclass(frozen=True, slots=True)
class ResolvedProjectGitHubBinding:
    """A project's GitHub binding plus an auth-minting handle.

    ``owner`` / ``repo`` are the operator-supplied source-of-truth
    strings from ``projects.github_owner`` / ``github_repo``.
    ``installation_id`` is the captured GitHub App installation id;
    ``None`` signals anonymous mode (story 13: a public repo with no
    App installation can still benefit from the daily audit by hitting
    the public API). ``auth`` is the minted :class:`InstallationAuth`
    when an installation is available, ``None`` otherwise — the
    caller passes it through to :class:`GitHubRefSetFetcher` as-is.
    """

    owner: str
    repo: str
    installation_id: int | None
    auth: InstallationAuth | None


class ProjectGitHubBindingResolver:
    """Resolve ``project_id`` to a :class:`ResolvedProjectGitHubBinding`.

    Encapsulates the "use installation auth if available, fall back to
    anonymous, return ``None`` if the project has no GitHub binding"
    decision so the periodic ``git_ref_audit`` worker (PRD #346) and
    the proactive ``sync_project`` pre-fetch (PRD #332) do not each
    re-derive it.

    Resolution shapes:

    * Project missing (or soft-deleted) → ``None``. The audit's
      project-id snapshot can race with a soft-delete; a typed
      ``None`` keeps the per-org loop going without raising.
    * ``github_owner`` / ``github_repo`` NULL (non-GitHub
      ``source_url``) → ``None``. ``ref_deleted`` does not apply.
    * Binding present, ``github_installation_id`` NULL →
      :class:`ResolvedProjectGitHubBinding` with ``auth=None``. The
      fetcher uses the anonymous public path. A later installation
      webhook backfills the id and the next tick authenticates.
    * Binding present, ``github_installation_id`` populated →
      :class:`ResolvedProjectGitHubBinding` with a freshly-minted
      :class:`InstallationAuth`. Token exchange happens on every
      resolve call: installation tokens expire in roughly one hour
      and tend to be consumed by a fan-out of fetches immediately
      after, so caching them across calls would buy little and
      complicate the eviction story.

    The binding-column read runs inside a short read transaction
    owned by this resolver; the installation-token exchange (a
    GitHub network round-trip) runs **after** that transaction has
    closed so the database connection is not held idle-in-transaction
    across the network call. The audit's per-project fan-out would
    otherwise compound the idle-in-transaction window per project.
    """

    def __init__(
        self,
        *,
        session: AsyncSession,
        project_store: ProjectStore,
        app_client: GitHubAppClient,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._project_store = project_store
        self._app_client = app_client
        self._logger = logger

    async def resolve(
        self, project_id: int
    ) -> ResolvedProjectGitHubBinding | None:
        """Resolve a project to its GitHub binding + auth record.

        Returns ``None`` for non-GitHub or missing projects;
        otherwise returns a :class:`ResolvedProjectGitHubBinding`. The
        anonymous-vs-installation auth choice is reflected on the
        returned record's ``auth`` field, not on the return type, so
        callers walk one ladder rather than two.

        The binding lookup is wrapped in this resolver's own short
        read transaction; the caller must therefore **not** wrap the
        call in ``session.begin()``. The token exchange runs after
        the read transaction has closed so the DB connection is not
        idle-in-transaction across the GitHub round-trip.
        """
        async with self._session.begin():
            project = await self._project_store.get_by_id(project_id)
        if project is None:
            return None
        owner = project.github_owner
        repo = project.github_repo
        if owner is None or repo is None:
            return None

        installation_id = project.github_installation_id
        if installation_id is None:
            return ResolvedProjectGitHubBinding(
                owner=owner,
                repo=repo,
                installation_id=None,
                auth=None,
            )

        token = await self._app_client.exchange_installation_token(
            installation_id
        )
        auth = InstallationAuth(
            token=token,
            installation_id=installation_id,
        )
        return ResolvedProjectGitHubBinding(
            owner=owner,
            repo=repo,
            installation_id=installation_id,
            auth=auth,
        )
