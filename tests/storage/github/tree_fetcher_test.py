"""Tests for GitHubTreeFetcher end-to-end against the mock_github fixture."""

from __future__ import annotations

import asyncio

import httpx
import pytest
import structlog
from safir.github import GitHubAppClientFactory

from docverse.storage.github import (
    GitHubAppClient,
    GitHubTreeFetcher,
    InstallationAuth,
)
from docverse.storage.github import tree_fetcher as tree_fetcher_module
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
    auth = mock_github.installation_auth("acme", "templates")

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubTreeFetcher(
            http_client=http_client, auth=auth, logger=_logger()
        )
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
    auth = mock_github.installation_auth("acme", "templates")

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubTreeFetcher(
            http_client=http_client, auth=auth, logger=_logger()
        )
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
    auth = mock_github.installation_auth("acme", "templates")

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubTreeFetcher(
            http_client=http_client, auth=auth, logger=_logger()
        )
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
    auth = mock_github.installation_auth("acme", "templates")

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubTreeFetcher(
            http_client=http_client, auth=auth, logger=_logger()
        )
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
    auth = mock_github.installation_auth("acme", "templates")

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubTreeFetcher(
            http_client=http_client, auth=auth, logger=_logger()
        )
        tree = await fetcher.fetch(
            owner="acme", repo="templates", ref="main", root_path="/"
        )

    assert {f.path for f in tree.files} == {"template.toml"}


@pytest.mark.asyncio
async def test_tree_fetcher_attaches_authorization_header(
    mock_github: GitHubMock,
) -> None:
    """Each GitHub call carries ``Authorization: Bearer <token>``.

    Pins the no-mutation contract: the fetcher attaches auth per
    request rather than mutating the shared client's defaults. Without
    this guarantee, the lifespan client could leak the installation
    token onto unrelated requests (Gafaelfawr/Repertoire/CDN).
    """
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={"template.toml": b"[dashboard]\n"},
    )
    auth = InstallationAuth(token="ghs_attach_test")

    async with httpx.AsyncClient() as http_client:
        # The shared client must not be configured with a base_url or an
        # Authorization header — those would defeat the per-request
        # attachment the fetcher under test is supposed to perform.
        assert "authorization" not in http_client.headers
        fetcher = GitHubTreeFetcher(
            http_client=http_client, auth=auth, logger=_logger()
        )
        await fetcher.fetch(
            owner="acme", repo="templates", ref="main", root_path="/"
        )
        # Defaults still untouched after the fetch returns.
        assert "authorization" not in http_client.headers

    sent_requests = [
        call.request
        for call in mock_github.router.calls
        if call.request.url.path.startswith("/repos/acme/templates")
    ]
    assert sent_requests, "expected at least one GitHub call"
    for request in sent_requests:
        assert request.headers["authorization"] == "Bearer ghs_attach_test"


@pytest.mark.parametrize("status", [404, 429, 500], ids=["404", "429", "500"])
@pytest.mark.asyncio
async def test_tree_fetcher_raises_on_non_2xx_ref_resolution(
    mock_github: GitHubMock,
    status: int,
) -> None:
    """Non-2xx responses on ref resolution raise ``httpx.HTTPStatusError``.

    Seeds the failing status on ``commits/{ref}`` (the first call in
    :meth:`GitHubTreeFetcher.fetch`) — sufficient to pin the
    ``raise_for_status`` behaviour without permuting every sub-call.
    The sync worker (#237) will layer typed exceptions on top; this
    test fences against an accidental swallow at the storage layer.
    """
    mock_github.router.get(
        "https://api.github.com/repos/acme/templates/commits/main"
    ).mock(return_value=httpx.Response(status))
    auth = InstallationAuth(token="ghs_test")

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubTreeFetcher(
            http_client=http_client, auth=auth, logger=_logger()
        )
        with pytest.raises(httpx.HTTPStatusError):
            await fetcher.fetch(
                owner="acme", repo="templates", ref="main", root_path="/"
            )


@pytest.mark.asyncio
async def test_tree_fetcher_runs_blob_fetches_concurrently(
    mock_github: GitHubMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Blob fetches overlap in-flight, bounded by the module concurrency.

    Monkeypatches ``_fetch_blob`` to record the peak in-flight count.
    With the default constant (12) and eight kept files, peak must be
    strictly greater than 1 — a serial loop would pin it to 1.
    """
    assert tree_fetcher_module._BLOB_FETCH_CONCURRENCY == 12
    file_count = 8
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={
            f"file-{i}.txt": f"data-{i}".encode() for i in range(file_count)
        },
    )
    auth = mock_github.installation_auth("acme", "templates")

    in_flight = 0
    peak = 0

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubTreeFetcher(
            http_client=http_client, auth=auth, logger=_logger()
        )
        original = fetcher._fetch_blob

        async def spy(owner: str, repo: str, blob_sha: str) -> bytes:
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            try:
                await asyncio.sleep(0.01)
                return await original(owner, repo, blob_sha)
            finally:
                in_flight -= 1

        monkeypatch.setattr(fetcher, "_fetch_blob", spy)
        tree = await fetcher.fetch(
            owner="acme", repo="templates", ref="main", root_path="/"
        )

    assert len(tree.files) == file_count
    assert peak > 1


@pytest.mark.asyncio
async def test_tree_fetcher_preserves_tree_entry_order(
    mock_github: GitHubMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``FetchedTree.files`` order matches filtered tree-entry order.

    Makes the first-scheduled blob the slowest to complete so completion
    order is inverted from entry order; slot pre-allocation is what
    keeps ``files[0]`` pointing at the first entry.
    """
    expected_paths = [f"file-{i}.txt" for i in range(6)]
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={
            path: f"data-{i}".encode() for i, path in enumerate(expected_paths)
        },
    )
    auth = mock_github.installation_auth("acme", "templates")

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubTreeFetcher(
            http_client=http_client, auth=auth, logger=_logger()
        )
        original = fetcher._fetch_blob
        call_count = 0

        async def invert_completion_order(
            owner: str, repo: str, blob_sha: str
        ) -> bytes:
            nonlocal call_count
            call_count += 1
            # The first task scheduled waits longest, so completion
            # order is the reverse of entry order.
            delay = max(0.0, 0.02 - 0.003 * (call_count - 1))
            await asyncio.sleep(delay)
            return await original(owner, repo, blob_sha)

        monkeypatch.setattr(fetcher, "_fetch_blob", invert_completion_order)
        tree = await fetcher.fetch(
            owner="acme", repo="templates", ref="main", root_path="/"
        )

    assert [f.path for f in tree.files] == expected_paths


@pytest.mark.asyncio
async def test_mock_github_composes_with_app_client_end_to_end(
    mock_github: GitHubMock,
) -> None:
    """Full flow: app JWT → install-token exchange → InstallationAuth → tree.

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
        auth = await app_client.get_installation_auth(
            owner="acme", repo="templates"
        )
        fetcher = GitHubTreeFetcher(
            http_client=http_client, auth=auth, logger=_logger()
        )
        tree = await fetcher.fetch(
            owner="acme",
            repo="templates",
            ref="main",
            root_path="/",
        )

    assert {f.path for f in tree.files} == {"template.toml"}
