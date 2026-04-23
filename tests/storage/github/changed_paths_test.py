"""Tests for the changed-paths helpers."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from docverse.storage.github import (
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

    async with httpx.AsyncClient(base_url="https://api.github.com") as client:
        paths = await fetch_changed_paths_from_compare(
            client, owner="acme", repo="repo", before="aaaa", after="bbbb"
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

    async with httpx.AsyncClient(base_url="https://api.github.com") as client:
        paths = await fetch_changed_paths_from_compare(
            client, owner="acme", repo="repo", before="aaaa", after="bbbb"
        )

    assert paths == ["templates/new.html", "templates/old.html"]
