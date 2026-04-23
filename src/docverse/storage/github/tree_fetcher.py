"""Fetch a subtree of a GitHub repository at a given ref."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
import structlog

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

    ``etag`` is captured from the git-trees response so the syncer can
    short-circuit a subsequent unchanged fetch via ``If-None-Match``.
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

    Uses a raw :class:`httpx.AsyncClient` (pre-authenticated as an
    installation) rather than gidgethub so callers see response headers
    — specifically ``ETag`` on the tree response — which we need to
    deduplicate unchanged syncs.
    """

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._client = client
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

    async def _resolve_ref(
        self, owner: str, repo: str, ref: str
    ) -> tuple[str, str]:
        """Resolve a ref (branch/tag/SHA) to (commit_sha, tree_sha)."""
        response = await self._client.get(
            f"/repos/{owner}/{repo}/commits/{ref}",
            headers={"Accept": "application/vnd.github+json"},
        )
        response.raise_for_status()
        data = response.json()
        return str(data["sha"]), str(data["commit"]["tree"]["sha"])

    async def _list_tree(
        self, owner: str, repo: str, tree_sha: str
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Return the recursive tree entries and the response ETag."""
        response = await self._client.get(
            f"/repos/{owner}/{repo}/git/trees/{tree_sha}",
            params={"recursive": "1"},
            headers={"Accept": "application/vnd.github+json"},
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
        response = await self._client.get(
            f"/repos/{owner}/{repo}/git/blobs/{blob_sha}",
            headers={"Accept": "application/vnd.github.raw"},
        )
        response.raise_for_status()
        return response.content
