"""Tests for GitHubRefSetFetcher against the mock_github fixture."""

from __future__ import annotations

import httpx
import pytest
import respx
import structlog

from docverse.storage.github import (
    GITHUB_API_BASE_URL,
    GitHubRefSetFetcher,
    InstallationAuth,
    RepositoryNotAccessibleError,
    RepositoryRefFetchError,
    RepositoryRefSet,
)
from tests.support.github_mock import GitHubMock


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _heads_url(owner: str, repo: str) -> str:
    return (
        f"{GITHUB_API_BASE_URL}/repos/{owner}/{repo}/git/matching-refs/heads"
    )


def _tags_url(owner: str, repo: str) -> str:
    return f"{GITHUB_API_BASE_URL}/repos/{owner}/{repo}/git/matching-refs/tags"


def _ref_entry(ref: str) -> dict[str, object]:
    return {
        "ref": ref,
        "node_id": f"node-{ref}",
        "url": f"https://api.github.com/{ref}",
        "object": {"sha": "deadbeef", "type": "commit"},
    }


def _seed_refs(
    mock_discovery: respx.Router,
    *,
    owner: str = "acme",
    repo: str = "docs",
    branches: list[str] | None = None,
    tags: list[str] | None = None,
) -> None:
    """Seed a single-page matching-refs response for heads and tags."""
    branches = branches if branches is not None else []
    tags = tags if tags is not None else []
    mock_discovery.get(_heads_url(owner, repo)).mock(
        return_value=httpx.Response(
            200,
            json=[_ref_entry(f"refs/heads/{name}") for name in branches],
        )
    )
    mock_discovery.get(_tags_url(owner, repo)).mock(
        return_value=httpx.Response(
            200,
            json=[_ref_entry(f"refs/tags/{name}") for name in tags],
        )
    )


@pytest.mark.asyncio
async def test_fetch_returns_branches_and_tags(
    mock_github: GitHubMock,
) -> None:
    """Single-page fetch returns the branches and tags as bare names."""
    _seed_refs(
        mock_github.router,
        branches=["main", "develop"],
        tags=["v1.0.0", "v1.1.0"],
    )

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubRefSetFetcher(http_client=http_client)
        refs = await fetcher.fetch(
            owner="acme",
            repo="docs",
            auth=InstallationAuth(token="ghs_test", installation_id=42),
            logger=_logger(),
        )

    assert refs.branches == frozenset({"main", "develop"})
    assert refs.tags == frozenset({"v1.0.0", "v1.1.0"})


@pytest.mark.asyncio
async def test_all_property_returns_union_of_branches_and_tags() -> None:
    """``RepositoryRefSet.all`` exposes the deduped union of refs."""
    refs = RepositoryRefSet(
        branches=frozenset({"main", "develop"}),
        tags=frozenset({"v1.0.0", "main"}),
    )
    assert refs.all == frozenset({"main", "develop", "v1.0.0"})


@pytest.mark.asyncio
async def test_empty_repo_returns_empty_set(
    mock_github: GitHubMock,
) -> None:
    """An empty repo (no branches, no tags) returns an empty refset."""
    _seed_refs(mock_github.router, branches=[], tags=[])

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubRefSetFetcher(http_client=http_client)
        refs = await fetcher.fetch(
            owner="acme",
            repo="docs",
            auth=InstallationAuth(token="ghs_test", installation_id=42),
            logger=_logger(),
        )

    assert refs.branches == frozenset()
    assert refs.tags == frozenset()
    assert refs.all == frozenset()


@pytest.mark.asyncio
async def test_ref_names_with_slashes_round_trip(
    mock_github: GitHubMock,
) -> None:
    """Branch / tag names containing ``/`` survive the prefix-strip.

    The ``ref_deleted`` predicate matches on the bare ref name from
    ``editions.tracking_params['git_ref']``, which can contain slashes
    (``tickets/DM-12345``, ``release/1.0``). Stripping ``refs/heads/``
    or ``refs/tags/`` must preserve every remaining slash so the
    membership test in the evaluator is exact.
    """
    _seed_refs(
        mock_github.router,
        branches=["tickets/DM-12345", "release/1.0", "main"],
        tags=["v1.0.0", "releases/2024/01"],
    )

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubRefSetFetcher(http_client=http_client)
        refs = await fetcher.fetch(
            owner="acme",
            repo="docs",
            auth=InstallationAuth(token="ghs_test", installation_id=42),
            logger=_logger(),
        )

    assert refs.branches == frozenset(
        {"tickets/DM-12345", "release/1.0", "main"}
    )
    assert refs.tags == frozenset({"v1.0.0", "releases/2024/01"})


@pytest.mark.asyncio
async def test_paginates_branches_across_multiple_pages(
    mock_github: GitHubMock,
) -> None:
    """Branches paginate across ``Link: rel="next"`` pages and merge.

    Two pages are seeded for the heads endpoint; the second is reached
    only by following the ``Link: <...>; rel="next"`` header. Asserts
    that the fetcher follows the link, merges the responses, and does
    not duplicate ref names that span page boundaries.
    """
    next_url = _heads_url("acme", "docs") + "?page=2&per_page=100"
    heads_route = mock_github.router.get(_heads_url("acme", "docs"))
    heads_route.side_effect = [
        httpx.Response(
            200,
            json=[
                _ref_entry("refs/heads/main"),
                _ref_entry("refs/heads/develop"),
            ],
            headers={"Link": f'<{next_url}>; rel="next"'},
        ),
        httpx.Response(
            200,
            json=[
                _ref_entry("refs/heads/feature-a"),
                _ref_entry("refs/heads/feature-b"),
            ],
        ),
    ]
    mock_github.router.get(_tags_url("acme", "docs")).mock(
        return_value=httpx.Response(200, json=[])
    )

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubRefSetFetcher(http_client=http_client)
        refs = await fetcher.fetch(
            owner="acme",
            repo="docs",
            auth=InstallationAuth(token="ghs_test", installation_id=42),
            logger=_logger(),
        )

    assert refs.branches == frozenset(
        {"main", "develop", "feature-a", "feature-b"}
    )
    assert refs.tags == frozenset()


@pytest.mark.asyncio
async def test_paginates_tags_across_multiple_pages(
    mock_github: GitHubMock,
) -> None:
    """Tags paginate across ``Link: rel="next"`` pages and merge."""
    next_url = _tags_url("acme", "docs") + "?page=2&per_page=100"
    mock_github.router.get(_heads_url("acme", "docs")).mock(
        return_value=httpx.Response(200, json=[])
    )
    tags_route = mock_github.router.get(_tags_url("acme", "docs"))
    tags_route.side_effect = [
        httpx.Response(
            200,
            json=[
                _ref_entry("refs/tags/v1.0.0"),
                _ref_entry("refs/tags/v1.1.0"),
            ],
            headers={"Link": f'<{next_url}>; rel="next"'},
        ),
        httpx.Response(
            200,
            json=[_ref_entry("refs/tags/v2.0.0")],
        ),
    ]

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubRefSetFetcher(http_client=http_client)
        refs = await fetcher.fetch(
            owner="acme",
            repo="docs",
            auth=InstallationAuth(token="ghs_test", installation_id=42),
            logger=_logger(),
        )

    assert refs.tags == frozenset({"v1.0.0", "v1.1.0", "v2.0.0"})


@pytest.mark.asyncio
async def test_authenticated_404_raises_repository_not_accessible(
    mock_github: GitHubMock,
) -> None:
    """An authenticated 404 surfaces as ``RepositoryNotAccessibleError``."""
    mock_github.router.get(_heads_url("acme", "private")).mock(
        return_value=httpx.Response(404, json={"message": "Not Found"})
    )
    mock_github.router.get(_tags_url("acme", "private")).mock(
        return_value=httpx.Response(200, json=[])
    )

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubRefSetFetcher(http_client=http_client)
        with pytest.raises(RepositoryNotAccessibleError) as exc_info:
            await fetcher.fetch(
                owner="acme",
                repo="private",
                auth=InstallationAuth(token="ghs_test", installation_id=42),
                logger=_logger(),
            )

    assert exc_info.value.owner == "acme"
    assert exc_info.value.repo == "private"


@pytest.mark.asyncio
async def test_anonymous_404_raises_repository_not_accessible(
    mock_github: GitHubMock,
) -> None:
    """An anonymous 404 (private repo, missing repo) also raises typed."""
    mock_github.router.get(_heads_url("acme", "private")).mock(
        return_value=httpx.Response(404, json={"message": "Not Found"})
    )
    mock_github.router.get(_tags_url("acme", "private")).mock(
        return_value=httpx.Response(200, json=[])
    )

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubRefSetFetcher(http_client=http_client)
        with pytest.raises(RepositoryNotAccessibleError):
            await fetcher.fetch(
                owner="acme",
                repo="private",
                logger=_logger(),
            )


@pytest.mark.asyncio
async def test_5xx_raises_repository_ref_fetch_error(
    mock_github: GitHubMock,
) -> None:
    """A 5xx response on either endpoint raises ``RepositoryRefFetchError``."""
    mock_github.router.get(_heads_url("acme", "docs")).mock(
        return_value=httpx.Response(500, json={"message": "Server Error"})
    )
    mock_github.router.get(_tags_url("acme", "docs")).mock(
        return_value=httpx.Response(200, json=[])
    )

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubRefSetFetcher(http_client=http_client)
        with pytest.raises(RepositoryRefFetchError) as exc_info:
            await fetcher.fetch(
                owner="acme",
                repo="docs",
                auth=InstallationAuth(token="ghs_test", installation_id=42),
                logger=_logger(),
            )

    assert exc_info.value.owner == "acme"
    assert exc_info.value.repo == "docs"


@pytest.mark.asyncio
async def test_rate_limit_raises_repository_ref_fetch_error(
    mock_github: GitHubMock,
) -> None:
    """A 403 with rate-limit signal still surfaces as ref-fetch failure.

    Rate-limit responses come back as 403 or 429 with structured body.
    The fetcher does not introspect the body — any non-200, non-404
    is a typed ref-fetch failure, leaving rate-limit-vs-other
    classification to the caller via the surfaced status if needed.
    """
    mock_github.router.get(_heads_url("acme", "docs")).mock(
        return_value=httpx.Response(
            403,
            json={"message": "API rate limit exceeded"},
        )
    )
    mock_github.router.get(_tags_url("acme", "docs")).mock(
        return_value=httpx.Response(200, json=[])
    )

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubRefSetFetcher(http_client=http_client)
        with pytest.raises(RepositoryRefFetchError):
            await fetcher.fetch(
                owner="acme",
                repo="docs",
                auth=InstallationAuth(token="ghs_test", installation_id=42),
                logger=_logger(),
            )


@pytest.mark.asyncio
async def test_network_error_raises_repository_ref_fetch_error(
    mock_github: GitHubMock,
) -> None:
    """An ``httpx.HTTPError`` from the network surfaces as typed."""
    mock_github.router.get(_heads_url("acme", "docs")).mock(
        side_effect=httpx.ConnectError("connection refused")
    )

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubRefSetFetcher(http_client=http_client)
        with pytest.raises(RepositoryRefFetchError):
            await fetcher.fetch(
                owner="acme",
                repo="docs",
                auth=InstallationAuth(token="ghs_test", installation_id=42),
                logger=_logger(),
            )


@pytest.mark.asyncio
async def test_authenticated_path_sends_authorization_header(
    mock_github: GitHubMock,
) -> None:
    """Each authenticated request carries ``Authorization: Bearer <token>``.

    Pins the no-mutation contract: the fetcher attaches auth per
    request rather than mutating the shared client's defaults, so the
    lifespan client cannot leak the installation token onto unrelated
    requests (Gafaelfawr / Repertoire / CDN).
    """
    _seed_refs(mock_github.router, branches=["main"], tags=[])

    async with httpx.AsyncClient() as http_client:
        assert "authorization" not in http_client.headers
        fetcher = GitHubRefSetFetcher(http_client=http_client)
        await fetcher.fetch(
            owner="acme",
            repo="docs",
            auth=InstallationAuth(token="ghs_attach_test", installation_id=99),
            logger=_logger(),
        )
        assert "authorization" not in http_client.headers

    sent_requests = [
        call.request
        for call in mock_github.router.calls
        if call.request.url.path.startswith(
            "/repos/acme/docs/git/matching-refs/"
        )
    ]
    assert sent_requests
    for request in sent_requests:
        assert request.headers["authorization"] == "Bearer ghs_attach_test"


@pytest.mark.asyncio
async def test_anonymous_path_omits_authorization_header(
    mock_github: GitHubMock,
) -> None:
    """``auth=None`` issues unauthenticated requests against api.github.com."""
    _seed_refs(mock_github.router, branches=["main"], tags=[])

    async with httpx.AsyncClient() as http_client:
        fetcher = GitHubRefSetFetcher(http_client=http_client)
        await fetcher.fetch(
            owner="acme",
            repo="docs",
            logger=_logger(),
        )

    sent_requests = [
        call.request
        for call in mock_github.router.calls
        if call.request.url.path.startswith(
            "/repos/acme/docs/git/matching-refs/"
        )
    ]
    assert sent_requests
    for request in sent_requests:
        assert "authorization" not in {k.lower() for k in request.headers}
