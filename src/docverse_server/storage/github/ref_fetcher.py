"""Fetch the live branch + tag ref set of a GitHub repository.

The PRD #346 ``git_ref_audit`` worker and the proactive sync-time
pre-fetch in PRD #332 both reach for the same shape: "give me the
current set of branches and tags for ``owner/repo`` so the lifecycle
``ref_deleted`` predicate can decide which draft editions have lost
their upstream ref". This module is that shape — a deep helper that
paginates :data:`GITHUB_API_BASE_URL`'s ``git/matching-refs/heads`` and
``/tags`` endpoints and returns the stripped bare-name ref set. It
mirrors :class:`GitHubTreeFetcher`'s no-mutation, per-request-auth
contract and adds pagination handling the tree fetcher does not need.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
import structlog

from .app_client import GITHUB_API_BASE_URL, InstallationAuth

__all__ = [
    "GitHubRefSetFetcher",
    "RepositoryNotAccessibleError",
    "RepositoryRefFetchError",
    "RepositoryRefSet",
]

# GitHub's documented per-page maximum for the matching-refs endpoints.
# 100 keeps round-trips at the floor: a repository with up to 100
# branches or 100 tags is served in a single response. Larger
# repositories paginate by following ``Link: rel="next"`` headers.
_PER_PAGE = 100


@dataclass(frozen=True, slots=True)
class RepositoryRefSet:
    """Live ref set of a GitHub repository, split by branch vs tag.

    ``branches`` and ``tags`` hold bare names without the
    ``refs/heads/`` or ``refs/tags/`` prefix — matching the shape
    Docverse stores in ``editions.tracking_params['git_ref']``, so the
    ``ref_deleted`` lifecycle predicate is a direct membership test
    against either set or against the derived :attr:`all` union.
    """

    branches: frozenset[str]
    tags: frozenset[str]

    @property
    def all(self) -> frozenset[str]:
        """Union of branch and tag names, deduplicated.

        ``editions.tracking_params['git_ref']`` does not record whether
        the tracked ref is a branch or a tag, so the evaluator branch
        in PRD #346 checks against the union. Returning a fresh
        frozenset on every access keeps the dataclass frozen-immutable
        without giving callers a shared mutable view.
        """
        return self.branches | self.tags


class RepositoryNotAccessibleError(Exception):
    """GitHub returned 404 for the repository's ref endpoints.

    On the authenticated path, this signals that the installation has
    lost (or was never granted) access to ``owner/repo``: the
    repository was deleted, transferred out of the installation's
    scope, or never selected when the App was installed with "Only
    select repositories". On the anonymous path it additionally covers
    private repositories that simply do not respond publicly.

    Deliberately **not** a :class:`DocverseSlackException`: the daily
    ``git_ref_audit`` is expected to encounter this for projects whose
    repo has been deleted or whose App installation is pending, so
    surfacing each occurrence to Sentry would page operators on every
    audit tick. Callers log the per-project skip and continue.
    """

    def __init__(self, *, owner: str, repo: str) -> None:
        super().__init__(f"GitHub repository {owner}/{repo} is not accessible")
        self.owner = owner
        self.repo = repo


class RepositoryRefFetchError(Exception):
    """A GitHub ref fetch failed with a non-404 error.

    Covers transport errors (:class:`httpx.HTTPError`), rate-limit
    responses (403 / 429), 5xx, and any other unexpected status that
    the fetcher cannot translate into a refset. The audit worker
    catches this per-project, logs it with the owner / repo, and
    continues to the next project so a single rate-limited
    installation does not block the audit for every other project.
    Like :class:`RepositoryNotAccessibleError` this is deliberately a
    plain ``Exception`` so individual transient failures do not
    page Sentry on every tick; the caller decides whether to capture
    the wrapped error.
    """

    def __init__(
        self,
        *,
        owner: str,
        repo: str,
        message: str | None = None,
    ) -> None:
        if message is None:
            message = f"Failed to fetch GitHub refs for {owner}/{repo}"
        super().__init__(message)
        self.owner = owner
        self.repo = repo


class GitHubRefSetFetcher:
    """Fetch the live branch + tag ref set of a GitHub repository.

    Calls ``GET /repos/{owner}/{repo}/git/matching-refs/heads`` and
    ``/tags`` against the GitHub REST API, paginates each endpoint
    internally via ``Link: rel="next"`` headers, and returns a
    :class:`RepositoryRefSet` of bare ref names (no ``refs/heads/`` or
    ``refs/tags/`` prefix). The fetcher is the single interface both
    the ``git_ref_audit`` worker (PRD #346) and the per-project
    pre-fetch in ``sync_project`` (PRD #332) call — they share the
    same auth resolution, the same pagination handling, and the same
    typed errors so the two paths cannot drift apart.

    Mirrors :class:`GitHubTreeFetcher`'s contract on the shared
    ``httpx.AsyncClient`` from the application lifespan: the
    ``Authorization`` header is attached per request when ``auth`` is
    supplied, and the client's defaults stay untouched. With
    ``auth=None`` the fetcher hits the public ``api.github.com``,
    which returns 200 for public repos and 404 for private /
    non-existent ones — both surface as
    :class:`RepositoryNotAccessibleError` so callers handle the
    "skip this project" path uniformly.
    """

    def __init__(self, *, http_client: httpx.AsyncClient) -> None:
        self._http_client = http_client

    async def fetch(
        self,
        *,
        owner: str,
        repo: str,
        auth: InstallationAuth | None = None,
        logger: structlog.stdlib.BoundLogger,
    ) -> RepositoryRefSet:
        """Return the live branches and tags for ``owner/repo``.

        Both endpoints paginate independently; an empty repo (no
        branches, no tags) returns
        ``RepositoryRefSet(frozenset(), frozenset())`` without
        raising.

        Parameters
        ----------
        owner, repo
            GitHub coordinates as registered on the project binding
            (case-preserved; the matching-refs endpoint accepts either
            case).
        auth
            ``InstallationAuth`` for authenticated calls, or ``None``
            for the anonymous public path.
        logger
            structlog bound logger inherited from the caller's
            per-project context. Bound here for "GitHub refs"
            structured-log breadcrumbs.

        Raises
        ------
        RepositoryNotAccessibleError
            Either endpoint returned 404 — repo deleted, transferred
            out of the installation's scope, private and queried
            anonymously, or never matched by a "Only select
            repositories" install.
        RepositoryRefFetchError
            Any other failure (network error, 5xx, rate-limit 403/429,
            malformed JSON).
        """
        branches = await self._fetch_refs(
            owner=owner,
            repo=repo,
            ref_kind="heads",
            auth=auth,
            logger=logger,
        )
        tags = await self._fetch_refs(
            owner=owner,
            repo=repo,
            ref_kind="tags",
            auth=auth,
            logger=logger,
        )
        return RepositoryRefSet(
            branches=frozenset(branches),
            tags=frozenset(tags),
        )

    async def _fetch_refs(
        self,
        *,
        owner: str,
        repo: str,
        ref_kind: str,
        auth: InstallationAuth | None,
        logger: structlog.stdlib.BoundLogger,
    ) -> list[str]:
        """Page through ``git/matching-refs/{heads,tags}`` for one kind.

        ``ref_kind`` is ``"heads"`` or ``"tags"``. The matching-refs
        endpoint takes a prefix; passing ``heads`` returns every
        ``refs/heads/*`` ref and passing ``tags`` returns every
        ``refs/tags/*`` ref. The fetcher strips that prefix from each
        returned ``ref`` so the caller sees bare names.

        Pagination state lives entirely in the response's
        ``Link: rel="next"`` header. After the first call the next
        URL already encodes ``per_page`` plus the GitHub cursor, so
        ``params`` must drop to ``None`` to avoid httpx re-appending
        ``per_page=100`` and breaking the cursor.
        """
        base_url = auth.base_url if auth is not None else GITHUB_API_BASE_URL
        url: str | None = (
            f"{base_url}/repos/{owner}/{repo}/git/matching-refs/{ref_kind}"
        )
        params: dict[str, str] | None = {"per_page": str(_PER_PAGE)}
        prefix = f"refs/{ref_kind}/"

        names: list[str] = []
        while url is not None:
            try:
                response = await self._http_client.get(
                    url,
                    headers=self._headers(auth),
                    params=params,
                )
            except httpx.HTTPError as exc:
                logger.warning(
                    "GitHub ref fetch network error",
                    owner=owner,
                    repo=repo,
                    ref_kind=ref_kind,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                raise RepositoryRefFetchError(
                    owner=owner,
                    repo=repo,
                    message=(
                        f"Network error fetching GitHub refs for "
                        f"{owner}/{repo}: {exc}"
                    ),
                ) from exc

            if response.status_code == httpx.codes.NOT_FOUND:
                raise RepositoryNotAccessibleError(owner=owner, repo=repo)
            if response.status_code != httpx.codes.OK:
                logger.warning(
                    "GitHub ref fetch returned non-2xx",
                    owner=owner,
                    repo=repo,
                    ref_kind=ref_kind,
                    status_code=response.status_code,
                )
                raise RepositoryRefFetchError(
                    owner=owner,
                    repo=repo,
                    message=(
                        f"GitHub refs for {owner}/{repo} returned status "
                        f"{response.status_code}"
                    ),
                )

            try:
                payload: Any = response.json()
            except ValueError as exc:
                raise RepositoryRefFetchError(
                    owner=owner,
                    repo=repo,
                    message=(
                        f"Malformed JSON in GitHub refs response for "
                        f"{owner}/{repo}"
                    ),
                ) from exc

            if not isinstance(payload, list):
                raise RepositoryRefFetchError(
                    owner=owner,
                    repo=repo,
                    message=(
                        f"Unexpected GitHub refs payload shape for "
                        f"{owner}/{repo}: expected list"
                    ),
                )

            for entry in payload:
                ref_value = (
                    entry.get("ref") if isinstance(entry, dict) else None
                )
                if not isinstance(ref_value, str):
                    continue
                if not ref_value.startswith(prefix):
                    continue
                names.append(ref_value[len(prefix) :])

            next_link = response.links.get("next")
            url = next_link.get("url") if next_link else None
            # The Link header URL already encodes ``per_page`` plus the
            # GitHub cursor, so re-appending ``params`` would clobber
            # the cursor and pin the loop on page 1 forever.
            params = None

        return names

    @staticmethod
    def _headers(auth: InstallationAuth | None) -> dict[str, str]:
        headers = {"Accept": "application/vnd.github+json"}
        if auth is not None:
            headers["Authorization"] = f"Bearer {auth.token}"
        return headers
