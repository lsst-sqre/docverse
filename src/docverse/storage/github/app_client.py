"""GitHub App client wrapper over ``safir.github.GitHubAppClientFactory``."""

from __future__ import annotations

import http
from dataclasses import dataclass
from typing import Any, Literal, override

import gidgethub
import httpx
import structlog
from gidgethub.apps import get_installation_access_token
from gidgethub.httpx import GitHubAPI
from safir.github import GitHubAppClientFactory
from safir.slack.sentry import SentryEventInfo

from docverse.exceptions import DocverseSlackException

__all__ = [
    "GITHUB_API_BASE_URL",
    "GitHubAppClient",
    "GitHubAppNotConfiguredError",
    "GitHubAppNotInstalledError",
    "InstallationAuth",
    "MissingGitHubAppSecret",
    "RepositoryMetadata",
]


GITHUB_API_BASE_URL = "https://api.github.com"

#: GitHub App name surfaced in Sentry's ``github_app`` context so a
#: triager looking at a misconfigured-tenant event can find the App in
#: the GitHub admin UI without grepping config. Matches the
#: ``Factory.__init__`` default for ``github_app_name``.
_GITHUB_APP_NAME = "lsst-sqre/docverse"

#: The three secret names that gate the GitHub App feature. Carried on
#: :class:`GitHubAppNotConfiguredError` as a structured field so Sentry
#: triagers can route the event to the kind of operator who can fix
#: each ("missing app id" vs "stale private key" vs "rotated webhook
#: secret") without unpacking the rendered message.
MissingGitHubAppSecret = Literal["app_id", "private_key", "webhook_secret"]


@dataclass(frozen=True, slots=True)
class RepositoryMetadata:
    """Numeric ids GitHub assigns to a repository and its owner.

    Returned by :meth:`GitHubAppClient.resolve_repository_metadata` so
    the ``project_github_resolve`` worker captures all three columns
    (``installation_id``, ``owner_id``, ``repo_id``) in a single deep
    call rather than re-deriving the same shape at each call site.
    The three fields together survive a GitHub-side rename or
    transfer; the operator-visible ``owner``/``repo`` strings do not.
    """

    installation_id: int
    owner_id: int
    repo_id: int


@dataclass(frozen=True, slots=True)
class InstallationAuth:
    """Per-installation GitHub auth applied to the shared HTTP client.

    Callers (tree fetcher, compare API helper) build absolute URLs from
    ``base_url`` and attach ``Authorization: Bearer {token}`` on each
    request so the process-wide ``httpx.AsyncClient`` defaults stay
    untouched.

    ``installation_id`` is GitHub's stable identifier for the
    repository's app installation. The syncer captures it onto the
    binding so future push events can reference the installation
    directly without an extra ``/repos/{owner}/{repo}/installation``
    round-trip.
    """

    token: str
    installation_id: int
    base_url: str = GITHUB_API_BASE_URL


class GitHubAppNotConfiguredError(DocverseSlackException):
    """The GitHub App feature is not configured in ``Config``.

    Raised when ``Factory.create_github_app_client()`` is called but any
    of ``github_app_id``, ``github_app_private_key``, or
    ``github_webhook_secret`` is unset, or when the startup-time
    credential validator has recorded the credentials as failing.
    Callers at HTTP boundaries translate this to a feature-disabled
    response (503 for admin endpoints, 404 for the webhook endpoint).

    Carries an indicator of *which* GitHub App secret is the cause
    (``app_id`` / ``private_key`` / ``webhook_secret``) plus the
    API-facing org slug (when known by the caller) and the
    installation id (when known). ``to_sentry`` surfaces ``org_slug``
    and ``missing_secret`` as low-cardinality Sentry tags so an
    on-call operator can route the event to the kind of maintainer
    who can fix it; the non-secret ``installation_id`` and the static
    app name go into the ``github_app`` Sentry context.
    """

    def __init__(
        self,
        *,
        missing_secret: MissingGitHubAppSecret,
        org_slug: str | None = None,
        installation_id: int | None = None,
        message: str | None = None,
    ) -> None:
        if message is None:
            message = _format_github_app_not_configured_message(
                missing_secret=missing_secret,
                org_slug=org_slug,
            )
        super().__init__(message)
        self.missing_secret: MissingGitHubAppSecret = missing_secret
        self.org_slug = org_slug
        self.installation_id = installation_id

    @override
    def to_sentry(self) -> SentryEventInfo:
        info = super().to_sentry()
        info.tags["missing_secret"] = self.missing_secret
        if self.org_slug is not None:
            info.tags["org_slug"] = self.org_slug
        context: dict[str, Any] = {
            "installation_id": self.installation_id,
            "app_name": _GITHUB_APP_NAME,
        }
        info.contexts["github_app"] = context
        return info


def _format_github_app_not_configured_message(
    *,
    missing_secret: MissingGitHubAppSecret,
    org_slug: str | None,
) -> str:
    """Render a default message for :class:`GitHubAppNotConfiguredError`."""
    org_part = f" for org {org_slug!r}" if org_slug is not None else ""
    return (
        f"GitHub App is not configured{org_part}: missing {missing_secret!r}"
    )


class GitHubAppNotInstalledError(Exception):
    """No installation grants the GitHub App access to ``owner/repo``.

    Raised by :meth:`GitHubAppClient.get_installation_id` when
    ``GET /repos/{owner}/{repo}/installation`` returns 404. A GitHub App
    installation id is per-account, not per-repo, and that endpoint
    finds the account's installation via a repo it can reach — so a 404
    means *no* installation grants the App access to this repo: the App
    is not installed on the account, it is installed with "Only select
    repositories" and this repo is not selected, or the
    ``owner``/``repo`` is mistyped. The JWT was accepted (a bad key is a
    401), so this is an App-to-repo connection issue, not a credential
    one.

    Deliberately **not** a :class:`DocverseSlackException`: a
    not-installed repo is an expected, operator-recoverable state that
    the ``installation`` webhook backfills once the App is installed, so
    it must stay out of Slack and Sentry. Carries ``owner``/``repo`` so
    callers can log which repository was unreachable.
    """

    def __init__(self, *, owner: str, repo: str) -> None:
        super().__init__(f"GitHub App is not installed on {owner}/{repo}")
        self.owner = owner
        self.repo = repo


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

    async def validate(self) -> str | None:
        """Validate that the configured GitHub App credentials work.

        Two-step check:

        1. Mint an app JWT — surfaces ``jwt.exceptions.InvalidKeyError``
           and any other cryptography-stack errors immediately so a
           malformed PEM is caught before any network call.
        2. ``GET /app`` against the GitHub API with that JWT — confirms
           that GitHub itself accepts the credentials, catching wrong
           ``app_id`` values and keys that parse locally but do not match
           the App they're paired with.

        Raises any exception encountered during the two steps; callers
        are expected to log the failure and disable the feature for the
        lifetime of the process.

        Returns
        -------
        str or None
            The App's public ``html_url`` (e.g.
            ``https://github.com/apps/{slug}``) from the ``GET /app``
            response — the page an operator visits to install the App on
            a repository. ``None`` if GitHub omits the field. Callers
            surface this through the API so the UI can link operators to
            the install page.
        """
        jwt = self._factory.get_app_jwt()
        anon = GitHubAPI(self._http_client, self._factory.app_name)
        data = await anon.getitem("/app", jwt=jwt)
        html_url = data.get("html_url")
        return str(html_url) if html_url is not None else None

    async def get_installation_id(self, owner: str, repo: str) -> int:
        """Resolve the installation ID for a repository.

        Uses the app JWT to call
        ``GET /repos/{owner}/{repo}/installation``. Returns the
        ``installation.id`` value, which is stable across GitHub repo
        renames / transfers and is therefore the preferred internal key
        for later lookups.

        Raises
        ------
        GitHubAppNotInstalledError
            If GitHub returns 404 — no installation grants the App
            access to ``owner/repo`` (not installed on the account,
            installed without this repo selected, or a mistyped
            ``owner``/``repo``).
        """
        jwt = self._factory.get_app_jwt()
        anon = GitHubAPI(self._http_client, self._factory.app_name)
        try:
            data = await anon.getitem(
                "/repos/{owner}/{repo}/installation",
                url_vars={"owner": owner, "repo": repo},
                jwt=jwt,
            )
        except gidgethub.BadRequest as exc:
            if exc.status_code == http.HTTPStatus.NOT_FOUND:
                raise GitHubAppNotInstalledError(
                    owner=owner, repo=repo
                ) from exc
            raise
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

    async def resolve_repository_metadata(
        self, *, owner: str, repo: str
    ) -> RepositoryMetadata:
        """Resolve ``installation_id`` + ``owner_id`` + ``repo_id`` for a repo.

        Combines two GitHub REST calls so the
        :func:`docverse.worker.functions.project_github_resolve` worker
        captures all three opportunistic-id columns in one go:

        1. ``GET /repos/{owner}/{repo}/installation`` (with the app
           JWT) — yields the installation id.
        2. ``GET /repos/{owner}/{repo}`` (with the installation token)
           — yields the stable numeric repo id and owner id.

        Mirrors the pair of calls
        :class:`docverse.storage.github.GitHubTreeFetcher` already
        makes on every sync; reusing those endpoints keeps the rename-
        robust id capture path consistent across the codebase. The
        installation-token round-trip is required because the
        ``/repos/{owner}/{repo}`` endpoint refuses anonymous reads on
        private repos.
        """
        installation_id = await self.get_installation_id(owner, repo)
        token = await self.exchange_installation_token(installation_id)
        response = await self._http_client.get(
            f"{GITHUB_API_BASE_URL}/repos/{owner}/{repo}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
            },
        )
        response.raise_for_status()
        data = response.json()
        return RepositoryMetadata(
            installation_id=installation_id,
            owner_id=int(data["owner"]["id"]),
            repo_id=int(data["id"]),
        )

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
        return InstallationAuth(token=token, installation_id=installation_id)
