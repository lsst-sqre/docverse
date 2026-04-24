"""GitHub App client wrapper over ``safir.github.GitHubAppClientFactory``."""

from __future__ import annotations

from dataclasses import dataclass

import httpx
import structlog
from gidgethub.apps import get_installation_access_token
from gidgethub.httpx import GitHubAPI
from safir.github import GitHubAppClientFactory

__all__ = [
    "GITHUB_API_BASE_URL",
    "GitHubAppClient",
    "GitHubAppNotConfiguredError",
    "InstallationAuth",
]


GITHUB_API_BASE_URL = "https://api.github.com"


@dataclass(frozen=True, slots=True)
class InstallationAuth:
    """Per-installation GitHub auth applied to the shared HTTP client.

    Callers (tree fetcher, compare API helper) build absolute URLs from
    ``base_url`` and attach ``Authorization: Bearer {token}`` on each
    request so the process-wide ``httpx.AsyncClient`` defaults stay
    untouched.
    """

    token: str
    base_url: str = GITHUB_API_BASE_URL


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
    Exposes installation-token exchange and a small
    :class:`InstallationAuth` record that downstream helpers (tree
    fetcher, compare API calls) attach to the shared
    ``httpx.AsyncClient`` on every request — the wrapper never mints
    its own client, so process-wide default headers stay untouched.
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

    async def get_installation_auth(
        self, *, owner: str, repo: str
    ) -> InstallationAuth:
        """Return the per-installation auth record for ``owner/repo``.

        Resolves the installation ID, exchanges it for a short-lived
        installation token, and returns an :class:`InstallationAuth` the
        caller attaches to the shared ``httpx.AsyncClient`` on each
        request. The wrapper does not construct an
        :class:`httpx.AsyncClient` of its own — the process-wide client
        from the ``main.py`` lifespan is reused so its default headers
        (used by Gafaelfawr/Repertoire/CDN calls) cannot leak the
        installation token.

        Parameters
        ----------
        owner
            The repository owner (org or user login).
        repo
            The repository name.
        """
        installation_id = await self.get_installation_id(owner, repo)
        token = await self.exchange_installation_token(installation_id)
        return InstallationAuth(token=token)
