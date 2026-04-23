"""GitHub App client wrapper over ``safir.github.GitHubAppClientFactory``."""

from __future__ import annotations

import httpx
import structlog
from gidgethub.apps import get_installation_access_token
from gidgethub.httpx import GitHubAPI
from safir.github import GitHubAppClientFactory

__all__ = ["GitHubAppClient", "GitHubAppNotConfiguredError"]


_GITHUB_API_BASE = "https://api.github.com"


class GitHubAppNotConfiguredError(Exception):
    """The GitHub App feature is not configured in ``Config``.

    Raised when ``Factory.create_github_app_client()`` is called but any
    of ``github_app_id``, ``github_app_private_key``, or
    ``github_webhook_secret`` is unset. Callers at HTTP boundaries
    translate this to a feature-disabled response (503 for admin
    endpoints, 404 for the webhook endpoint).
    """


class GitHubAppClient:
    """Installation-scoped access to the GitHub REST API.

    Thin wrapper over :class:`safir.github.GitHubAppClientFactory`.
    Exposes installation-token exchange and a ready-to-use
    :class:`httpx.AsyncClient` pre-authenticated as a given installation,
    for downstream helpers (tree fetcher, compare API calls) that need
    raw REST access, including response headers such as ``ETag``.
    """

    def __init__(
        self,
        *,
        factory: GitHubAppClientFactory,
        http_client: httpx.AsyncClient,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._factory = factory
        self._http_client = http_client
        self._logger = logger

    async def get_installation_id(self, owner: str, repo: str) -> int:
        """Resolve the installation ID for a repository.

        Uses the app JWT to call
        ``GET /repos/{owner}/{repo}/installation``. Returns the
        ``installation.id`` value, which is stable across GitHub repo
        renames / transfers and is therefore the preferred internal key
        for later lookups.
        """
        jwt = self._factory.get_app_jwt()
        anon = GitHubAPI(self._http_client, self._factory.app_name)
        data = await anon.getitem(
            "/repos/{owner}/{repo}/installation",
            url_vars={"owner": owner, "repo": repo},
            jwt=jwt,
        )
        return int(data["id"])

    async def exchange_installation_token(self, installation_id: int) -> str:
        """Exchange the app JWT for a short-lived installation token.

        Delegates to :func:`gidgethub.apps.get_installation_access_token`
        so token minting stays in one tested code path. Returns only the
        token string — callers that need the expiry should call
        gidgethub directly.
        """
        anon = GitHubAPI(self._http_client, self._factory.app_name)
        token_info = await get_installation_access_token(
            anon,
            installation_id=str(installation_id),
            app_id=str(self._factory.app_id),
            private_key=self._factory.app_key,
        )
        return str(token_info["token"])

    async def create_installation_http_client(
        self, *, owner: str, repo: str
    ) -> httpx.AsyncClient:
        """Return a new ``httpx.AsyncClient`` scoped to an installation.

        The returned client has ``base_url`` set to the GitHub API and
        an ``Authorization: Bearer <token>`` header pre-configured.
        The caller owns the client and MUST close it (typically via
        ``async with``).

        Parameters
        ----------
        owner
            The repository owner (org or user login).
        repo
            The repository name.
        """
        installation_id = await self.get_installation_id(owner, repo)
        token = await self.exchange_installation_token(installation_id)
        return httpx.AsyncClient(
            base_url=_GITHUB_API_BASE,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
