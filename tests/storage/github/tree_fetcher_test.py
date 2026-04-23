"""Tests for GitHubTreeFetcher end-to-end against the mock_github fixture."""

from __future__ import annotations

import httpx
import pytest
import structlog
from safir.github import GitHubAppClientFactory

from docverse.storage.github import GitHubAppClient, GitHubTreeFetcher
from tests.support.github_mock import DEFAULT_APP_NAME, GitHubMock


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


@pytest.mark.asyncio
async def test_tree_fetcher_scoped_to_root_path(
    mock_github: GitHubMock,
) -> None:
    """Only blobs under ``root_path`` are returned; paths are re-rooted."""
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={
            "templates/default/template.toml": b"[dashboard]\n",
            "templates/default/assets/logo.svg": b"<svg/>",
            "templates/other/template.toml": b"[dashboard]\n",
            "README.md": b"# readme\n",
        },
        etag='W/"abc"',
    )

    async with httpx.AsyncClient(base_url="https://api.github.com") as client:
        fetcher = GitHubTreeFetcher(client=client, logger=_logger())
        tree = await fetcher.fetch(
            owner="acme",
            repo="templates",
            ref="main",
            root_path="templates/default",
        )

    returned_paths = {f.path for f in tree.files}
    assert returned_paths == {"template.toml", "assets/logo.svg"}


@pytest.mark.asyncio
async def test_tree_fetcher_captures_etag_and_commit_sha(
    mock_github: GitHubMock,
) -> None:
    """ETag and commit SHA surface on ``FetchedTree`` for dedup/cache."""
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={"template.toml": b"[dashboard]\n"},
        commit_sha="deadbeef",
        tree_sha="treecafe",
        etag='W/"tree-etag-1"',
    )

    async with httpx.AsyncClient(base_url="https://api.github.com") as client:
        fetcher = GitHubTreeFetcher(client=client, logger=_logger())
        tree = await fetcher.fetch(
            owner="acme", repo="templates", ref="main", root_path="/"
        )

    assert tree.commit_sha == "deadbeef"
    assert tree.tree_sha == "treecafe"
    assert tree.etag == 'W/"tree-etag-1"'


@pytest.mark.asyncio
async def test_tree_fetcher_blob_bytes_are_verbatim(
    mock_github: GitHubMock,
) -> None:
    """Binary blob bytes round-trip unchanged (no base64 smear)."""
    logo_bytes = b"\x89PNG\r\n\x1a\n\x00\x01\x02\x03fake-png"
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={"assets/logo.png": logo_bytes},
    )

    async with httpx.AsyncClient(base_url="https://api.github.com") as client:
        fetcher = GitHubTreeFetcher(client=client, logger=_logger())
        tree = await fetcher.fetch(
            owner="acme", repo="templates", ref="main", root_path="/"
        )

    assert len(tree.files) == 1
    assert tree.files[0].data == logo_bytes
    assert tree.files[0].size == len(logo_bytes)


@pytest.mark.asyncio
async def test_tree_fetcher_empty_when_root_path_missing(
    mock_github: GitHubMock,
) -> None:
    """``root_path`` with no blobs returns ``files=()`` but keeps metadata."""
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={"README.md": b"# readme\n"},
    )

    async with httpx.AsyncClient(base_url="https://api.github.com") as client:
        fetcher = GitHubTreeFetcher(client=client, logger=_logger())
        tree = await fetcher.fetch(
            owner="acme",
            repo="templates",
            ref="main",
            root_path="does/not/exist",
        )

    assert tree.files == ()
    assert tree.commit_sha  # still resolved
    assert tree.root_path == "does/not/exist"


@pytest.mark.asyncio
async def test_tree_fetcher_ignores_non_blob_entries(
    mock_github: GitHubMock,
) -> None:
    """Subdirectory (``type: tree``) entries are not fetched as blobs."""
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={"template.toml": b"[dashboard]\n"},
        extra_tree_entries=[
            {
                "path": "assets",
                "mode": "040000",
                "type": "tree",
                "sha": "tree-subdir",
            }
        ],
    )

    async with httpx.AsyncClient(base_url="https://api.github.com") as client:
        fetcher = GitHubTreeFetcher(client=client, logger=_logger())
        tree = await fetcher.fetch(
            owner="acme", repo="templates", ref="main", root_path="/"
        )

    assert {f.path for f in tree.files} == {"template.toml"}


@pytest.mark.asyncio
async def test_mock_github_composes_with_app_client_end_to_end(
    mock_github: GitHubMock,
) -> None:
    """Full flow: app JWT → install-token exchange → install client → tree.

    Exercises both fixture helpers (``seed_installation`` +
    ``seed_tree``) on the same ``mock_discovery`` router in a single
    test, demonstrating the composition requirement from the task
    acceptance criteria.
    """
    mock_github.seed_installation("acme", "templates", installation_id=77)
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={"template.toml": b"[dashboard]\n"},
    )

    async with httpx.AsyncClient() as http_client:
        safir_factory = GitHubAppClientFactory(
            id=mock_github.app_id,
            key=mock_github.private_key_pem,
            name=DEFAULT_APP_NAME,
            http_client=http_client,
        )
        app_client = GitHubAppClient(
            factory=safir_factory,
            http_client=http_client,
            logger=_logger(),
        )
        installation_client = await app_client.create_installation_http_client(
            owner="acme", repo="templates"
        )
        try:
            fetcher = GitHubTreeFetcher(
                client=installation_client, logger=_logger()
            )
            tree = await fetcher.fetch(
                owner="acme",
                repo="templates",
                ref="main",
                root_path="/",
            )
        finally:
            await installation_client.aclose()

    assert {f.path for f in tree.files} == {"template.toml"}
