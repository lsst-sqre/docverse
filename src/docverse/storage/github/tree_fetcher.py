"""Fetch a subtree of a GitHub repository at a given ref."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
import structlog

from .app_client import InstallationAuth

__all__ = ["FetchedTree", "FetchedTreeFile", "GitHubTreeFetcher"]


@dataclass(frozen=True, slots=True)
class FetchedTreeFile:
    """A single blob inside a ``FetchedTree``.

    ``path`` is relative to the fetcher's ``root_path`` argument. For
    ``root_path="/"`` that matches the repository-relative path; for
    ``root_path="templates/default"`` it strips the prefix so downstream
    storage keys are invariant across template layouts.
    """

    path: str
    blob_sha: str
    size: int
    data: bytes


@dataclass(frozen=True, slots=True)
class FetchedTree:
    """A GitHub tree scoped to ``root_path``, plus identity/cache metadata.

    ``etag`` is the value of the git-trees response's ``ETag`` header,
    captured so the syncer can use it as a content cache key: after a
    full fetch, comparing the new ETag against the previously-stored
    one lets the syncer skip the no-op upsert + dashboard rebuild
    fan-out when nothing under ``root_path`` actually changed. The
    fetcher itself does not perform a conditional ``If-None-Match``
    request — issuing the conditional GET to skip the body fetch
    entirely is a possible future optimisation tracked alongside
    the dashboard sync worker (#237).

    ``commit_sha`` is the resolved commit the ref pointed at when the
    tree was read, independent of whether ``ref`` was a branch, tag, or
    raw SHA.
    """

    owner: str
    repo: str
    ref: str
    root_path: str
    commit_sha: str
    tree_sha: str
    etag: str | None
    files: tuple[FetchedTreeFile, ...]


def _normalize_root(root_path: str) -> str:
    """Strip leading/trailing slashes; ``"/"`` and ``""`` both mean root."""
    return root_path.strip("/")


class GitHubTreeFetcher:
    """Fetch a subtree of a repo using the GitHub REST API.

    Uses the shared :class:`httpx.AsyncClient` from the application
    lifespan and attaches an ``Authorization: Bearer {token}`` header
    per request from a caller-supplied :class:`InstallationAuth`. The
    client's default headers and ``base_url`` are deliberately not
    mutated so the same client can serve Gafaelfawr / Repertoire / CDN
    calls without leaking installation tokens or re-rooting their URLs.
    Reading raw HTTP responses (rather than going through gidgethub)
    keeps the ``ETag`` header and raw blob bytes accessible.
    """

    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient,
        auth: InstallationAuth,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._http_client = http_client
        self._auth = auth
        self._logger = logger

    async def fetch(
        self, *, owner: str, repo: str, ref: str, root_path: str
    ) -> FetchedTree:
        """Fetch the tree under ``root_path`` at ``ref`` for ``owner/repo``.

        Steps: resolve the ref to a commit (giving us ``commit_sha`` +
        the top-level tree SHA), list the recursive tree, keep only
        blobs inside ``root_path``, then fetch each blob's bytes.
        """
        commit_sha, tree_sha = await self._resolve_ref(owner, repo, ref)
        tree_entries, etag = await self._list_tree(owner, repo, tree_sha)

        normalized_root = _normalize_root(root_path)
        prefix = f"{normalized_root}/" if normalized_root else ""

        files: list[FetchedTreeFile] = []
        for entry in tree_entries:
            if entry.get("type") != "blob":
                continue
            path = entry["path"]
            if prefix and not path.startswith(prefix):
                continue
            rel_path = path[len(prefix) :] if prefix else path
            blob_sha = entry["sha"]
            data = await self._fetch_blob(owner, repo, blob_sha)
            files.append(
                FetchedTreeFile(
                    path=rel_path,
                    blob_sha=blob_sha,
                    size=int(entry.get("size", len(data))),
                    data=data,
                )
            )

        return FetchedTree(
            owner=owner,
            repo=repo,
            ref=ref,
            root_path=normalized_root,
            commit_sha=commit_sha,
            tree_sha=tree_sha,
            etag=etag,
            files=tuple(files),
        )

    def _auth_headers(self, accept: str) -> dict[str, str]:
        return {
            "Accept": accept,
            "Authorization": f"Bearer {self._auth.token}",
        }

    async def _resolve_ref(
        self, owner: str, repo: str, ref: str
    ) -> tuple[str, str]:
        """Resolve a ref (branch/tag/SHA) to (commit_sha, tree_sha)."""
        response = await self._http_client.get(
            f"{self._auth.base_url}/repos/{owner}/{repo}/commits/{ref}",
            headers=self._auth_headers("application/vnd.github+json"),
        )
        response.raise_for_status()
        data = response.json()
        return str(data["sha"]), str(data["commit"]["tree"]["sha"])

    async def _list_tree(
        self, owner: str, repo: str, tree_sha: str
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Return the recursive tree entries and the response ETag."""
        response = await self._http_client.get(
            f"{self._auth.base_url}/repos/{owner}/{repo}/git/trees/{tree_sha}",
            params={"recursive": "1"},
            headers=self._auth_headers("application/vnd.github+json"),
        )
        response.raise_for_status()
        data = response.json()
        entries: list[dict[str, Any]] = list(data.get("tree") or [])
        etag = response.headers.get("etag")
        return entries, etag

    async def _fetch_blob(self, owner: str, repo: str, blob_sha: str) -> bytes:
        """Fetch a blob's raw bytes.

        Uses ``Accept: application/vnd.github.raw`` so the response body
        is the file bytes verbatim — no base64 round-trip, which matters
        for binary assets (images, fonts) referenced by templates.
        """
        response = await self._http_client.get(
            f"{self._auth.base_url}/repos/{owner}/{repo}/git/blobs/{blob_sha}",
            headers=self._auth_headers("application/vnd.github.raw"),
        )
        response.raise_for_status()
        return response.content
