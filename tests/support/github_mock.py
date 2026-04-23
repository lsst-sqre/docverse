"""Helpers to seed GitHub REST API responses on a respx router.

Used by the ``mock_github`` fixture in ``tests/conftest.py`` to stand in
for the live GitHub API when exercising the sync worker, webhook
handler, and any unit test that drives the GitHub-facing storage
helpers (``GitHubAppClient``, ``GitHubTreeFetcher``, ``changed_paths``).
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import cached_property

import httpx
import respx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

__all__ = ["DEFAULT_APP_ID", "DEFAULT_APP_NAME", "GitHubMock", "make_rsa_pem"]


DEFAULT_APP_ID = 12345
DEFAULT_APP_NAME = "lsst-sqre/docverse"
_GITHUB_API = "https://api.github.com"


def make_rsa_pem() -> str:
    """Generate a fresh PKCS#8 PEM RSA private key for test JWT signing.

    Session-scoped in the fixture so the 2048-bit keygen cost only pays
    once per ``nox -s test`` run.
    """
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pem.decode("utf-8")


def _blob_sha(data: bytes) -> str:
    r"""Git blob SHA-1 (matches ``git hash-object``).

    Blob header is ``blob <size>\0`` prepended to the bytes. Using the
    real scheme lets tests assert that a known file's SHA matches what a
    real repo would produce, if they care — and gives the mock
    deterministic SHAs without callers having to invent them.
    """
    header = f"blob {len(data)}\0".encode()
    return hashlib.sha1(header + data, usedforsecurity=False).hexdigest()


@dataclass(slots=True)
class GitHubMock:
    """respx seeder for the GitHub REST endpoints the GitHub App touches.

    One instance per test; each seed_* method registers the handful of
    routes that together let a given helper (tree fetcher, webhook
    processor, app client) execute its flow without hitting the
    network.

    The fixture wires this onto the same autouse ``mock_discovery``
    router so both the Repertoire-discovery mocks and the GitHub mocks
    live in one respx context — tests that need both compose cleanly
    without juggling two context managers.
    """

    router: respx.Router
    private_key_pem: str
    app_id: int = DEFAULT_APP_ID
    app_name: str = DEFAULT_APP_NAME
    default_installation_id: int = 99
    default_token: str = "ghs_test_installation_token"  # noqa: S105
    _installation_ids: dict[tuple[str, str], int] = field(default_factory=dict)

    @cached_property
    def installation_token(self) -> str:
        """Installation token returned by the mocked exchange endpoint."""
        return self.default_token

    def seed_installation(
        self,
        owner: str,
        repo: str,
        *,
        installation_id: int | None = None,
        token: str | None = None,
    ) -> int:
        """Wire (repo → installation id) lookup and (id → token) exchange.

        Both endpoints are needed for
        ``GitHubAppClient.create_installation_http_client`` to succeed;
        seed them together so callers only write one line. Returns the
        installation id for downstream assertions.
        """
        iid = installation_id or self.default_installation_id
        bearer_token = token or self.default_token
        self._installation_ids[(owner, repo)] = iid

        self.router.get(
            f"{_GITHUB_API}/repos/{owner}/{repo}/installation"
        ).mock(
            return_value=httpx.Response(
                200, json={"id": iid, "account": {"login": owner}}
            )
        )
        self.router.post(
            f"{_GITHUB_API}/app/installations/{iid}/access_tokens"
        ).mock(
            return_value=httpx.Response(
                201,
                json={
                    "token": bearer_token,
                    "expires_at": "2099-01-01T00:00:00Z",
                },
            )
        )
        return iid

    def seed_tree(
        self,
        owner: str,
        repo: str,
        ref: str,
        *,
        files: Mapping[str, bytes],
        commit_sha: str | None = None,
        tree_sha: str | None = None,
        etag: str = 'W/"test-etag"',
        extra_tree_entries: list[dict[str, object]] | None = None,
    ) -> tuple[str, str]:
        """Register the commit, tree, and blob endpoints for a fetch.

        ``files`` maps *repo-absolute* paths to blob bytes. Callers
        passing a nested ``root_path`` to the fetcher must pre-prefix
        accordingly — this matches how real repositories are keyed and
        keeps the helper honest about the structure under test.

        ``extra_tree_entries`` lets a test add non-blob entries (``tree``
        subdirs) or blobs outside ``root_path`` to verify the fetcher's
        filtering.

        Returns ``(commit_sha, tree_sha)`` for cross-asserting.
        """
        csha = commit_sha or f"commit-{owner}-{repo}-{ref}"
        tsha = tree_sha or f"tree-{owner}-{repo}-{ref}"

        self.router.get(
            f"{_GITHUB_API}/repos/{owner}/{repo}/commits/{ref}"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "sha": csha,
                    "commit": {"tree": {"sha": tsha}},
                },
            )
        )

        tree_entries: list[dict[str, object]] = []
        for path, data in files.items():
            blob_sha = _blob_sha(data)
            tree_entries.append(
                {
                    "path": path,
                    "mode": "100644",
                    "type": "blob",
                    "sha": blob_sha,
                    "size": len(data),
                }
            )
            self.router.get(
                f"{_GITHUB_API}/repos/{owner}/{repo}/git/blobs/{blob_sha}"
            ).mock(return_value=httpx.Response(200, content=data))

        if extra_tree_entries:
            tree_entries.extend(extra_tree_entries)

        self.router.get(
            f"{_GITHUB_API}/repos/{owner}/{repo}/git/trees/{tsha}",
            params={"recursive": "1"},
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "sha": tsha,
                    "tree": tree_entries,
                    "truncated": False,
                },
                headers={"ETag": etag},
            )
        )

        return csha, tsha

    def seed_compare(
        self,
        owner: str,
        repo: str,
        *,
        before: str,
        after: str,
        changed_paths: list[str],
        renamed: Mapping[str, str] | None = None,
    ) -> None:
        """Register a compare-API response listing ``changed_paths``.

        ``renamed`` maps new filename → previous filename so callers can
        exercise the rename branch of ``fetch_changed_paths_from_compare``
        without flattening the rename into two independent entries.
        """
        renamed = renamed or {}
        files: list[dict[str, object]] = []
        for path in changed_paths:
            entry: dict[str, object] = {
                "filename": path,
                "status": "renamed" if path in renamed else "modified",
            }
            if path in renamed:
                entry["previous_filename"] = renamed[path]
            files.append(entry)

        self.router.get(
            f"{_GITHUB_API}/repos/{owner}/{repo}/compare/{before}...{after}"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "status": "ahead",
                    "ahead_by": len(changed_paths),
                    "behind_by": 0,
                    "total_commits": 1,
                    "files": files,
                },
            )
        )
