"""Tests for the changed-paths helpers."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from docverse.storage.github import (
    InstallationAuth,
    extract_changed_paths_from_push,
    fetch_changed_paths_from_compare,
)
from tests.support.github_mock import GitHubMock


def _commit(
    *,
    added: list[str] | None = None,
    modified: list[str] | None = None,
    removed: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "added": added or [],
        "modified": modified or [],
        "removed": removed or [],
    }


def test_extract_changed_paths_flat_list() -> None:
    """In-payload commits yield the union of added/modified/removed paths."""
    payload = {
        "before": "aaaa",
        "after": "bbbb",
        "size": 2,
        "commits": [
            _commit(added=["a.txt"], modified=["b.txt"]),
            _commit(modified=["b.txt"], removed=["c.txt"]),
        ],
    }
    result = extract_changed_paths_from_push(payload)
    assert result == ["a.txt", "b.txt", "c.txt"]


def test_extract_changed_paths_truncated_by_size_returns_none() -> None:
    """If size > len(commits), payload is truncated; return None."""
    payload = {
        "before": "aaaa",
        "after": "bbbb",
        "size": 5,
        "commits": [_commit(added=["a.txt"])],
    }
    assert extract_changed_paths_from_push(payload) is None


def test_extract_changed_paths_twenty_commits_returns_none() -> None:
    """20 commits is GitHub's ceiling; treat as possibly truncated."""
    payload = {
        "commits": [_commit(added=[f"f{i}.txt"]) for i in range(20)],
        "size": 20,
    }
    assert extract_changed_paths_from_push(payload) is None


def test_extract_changed_paths_missing_commits_returns_none() -> None:
    """Payloads without a ``commits`` array (e.g. tag-delete) fall back."""
    payload = {"before": "aaaa", "after": "bbbb"}
    assert extract_changed_paths_from_push(payload) is None


def test_extract_changed_paths_empty_commits_returns_empty_list() -> None:
    """A push with zero non-merge commits yields zero changed paths."""
    payload = {"commits": [], "size": 0}
    assert extract_changed_paths_from_push(payload) == []


@pytest.mark.asyncio
async def test_fetch_changed_paths_from_compare(
    mock_github: GitHubMock,
) -> None:
    """Compare-API response parses into a deduped, sorted list of paths."""
    mock_github.seed_compare(
        "acme",
        "repo",
        before="aaaa",
        after="bbbb",
        changed_paths=["templates/a.html", "templates/b.html", "docs/c.md"],
    )
    auth = mock_github.installation_auth("acme", "repo")

    async with httpx.AsyncClient() as http_client:
        paths = await fetch_changed_paths_from_compare(
            http_client,
            auth=auth,
            owner="acme",
            repo="repo",
            before="aaaa",
            after="bbbb",
        )

    assert paths == ["docs/c.md", "templates/a.html", "templates/b.html"]


@pytest.mark.asyncio
async def test_fetch_changed_paths_from_compare_includes_rename_source(
    mock_github: GitHubMock,
) -> None:
    """Renames contribute both the new and the previous path to the set."""
    mock_github.seed_compare(
        "acme",
        "repo",
        before="aaaa",
        after="bbbb",
        changed_paths=["templates/new.html"],
        renamed={"templates/new.html": "templates/old.html"},
    )
    auth = mock_github.installation_auth("acme", "repo")

    async with httpx.AsyncClient() as http_client:
        paths = await fetch_changed_paths_from_compare(
            http_client,
            auth=auth,
            owner="acme",
            repo="repo",
            before="aaaa",
            after="bbbb",
        )

    assert paths == ["templates/new.html", "templates/old.html"]


@pytest.mark.asyncio
async def test_fetch_changed_paths_from_compare_with_null_files(
    mock_github: GitHubMock,
) -> None:
    """``{"files": null}`` from GitHub coerces to an empty list, not a crash.

    Pins the ``data.get("files") or []`` defence in
    :func:`fetch_changed_paths_from_compare` against an upstream
    refactor that might switch to ``data["files"] or []`` (KeyError) or
    ``data.get("files", [])`` (TypeError on ``None``).
    """
    mock_github.router.get(
        "https://api.github.com/repos/acme/repo/compare/aaaa...bbbb"
    ).mock(return_value=httpx.Response(200, json={"files": None}))
    auth = InstallationAuth(token="ghs_test")

    async with httpx.AsyncClient() as http_client:
        paths = await fetch_changed_paths_from_compare(
            http_client,
            auth=auth,
            owner="acme",
            repo="repo",
            before="aaaa",
            after="bbbb",
        )

    assert paths == []


@pytest.mark.asyncio
async def test_fetch_changed_paths_from_compare_attaches_authorization(
    mock_github: GitHubMock,
) -> None:
    """Compare call carries ``Authorization: Bearer <token>`` per request.

    Mirrors the no-mutation contract on the tree fetcher: the helper
    must not rely on the shared client's defaults for either base URL
    or auth, since the same client also serves Gafaelfawr / Repertoire
    / CDN traffic.
    """
    mock_github.seed_compare(
        "acme",
        "repo",
        before="aaaa",
        after="bbbb",
        changed_paths=["templates/a.html"],
    )
    auth = InstallationAuth(token="ghs_compare_attach")

    async with httpx.AsyncClient() as http_client:
        assert "authorization" not in http_client.headers
        await fetch_changed_paths_from_compare(
            http_client,
            auth=auth,
            owner="acme",
            repo="repo",
            before="aaaa",
            after="bbbb",
        )
        assert "authorization" not in http_client.headers

    sent = [
        call.request
        for call in mock_github.router.calls
        if call.request.url.path == "/repos/acme/repo/compare/aaaa...bbbb"
    ]
    assert sent, "expected the compare endpoint to be called"
    assert sent[-1].headers["authorization"] == "Bearer ghs_compare_attach"


@pytest.mark.parametrize("status", [404, 429, 500], ids=["404", "429", "500"])
@pytest.mark.asyncio
async def test_fetch_changed_paths_from_compare_raises_on_non_2xx(
    mock_github: GitHubMock,
    status: int,
) -> None:
    """Non-2xx GitHub responses surface as ``httpx.HTTPStatusError``.

    Pins today's bare ``raise_for_status`` behaviour so when the sync
    worker (#237) layers a richer typed-exception taxonomy on top, the
    regression fence catches an accidental swallow.
    """
    mock_github.router.get(
        "https://api.github.com/repos/acme/repo/compare/aaaa...bbbb"
    ).mock(return_value=httpx.Response(status))
    auth = InstallationAuth(token="ghs_test")

    async with httpx.AsyncClient() as http_client:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_changed_paths_from_compare(
                http_client,
                auth=auth,
                owner="acme",
                repo="repo",
                before="aaaa",
                after="bbbb",
            )
