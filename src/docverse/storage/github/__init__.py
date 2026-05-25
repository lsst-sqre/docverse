"""GitHub App infrastructure shared by the sync worker and webhooks."""

from __future__ import annotations

from .app_client import (
    GITHUB_API_BASE_URL,
    GitHubAppClient,
    GitHubAppNotConfiguredError,
    InstallationAuth,
    RepositoryMetadata,
)
from .changed_paths import (
    extract_changed_paths_from_push,
    fetch_changed_paths_from_compare,
)
from .startup import GitHubAppValidationState, validate_github_app
from .tree_fetcher import FetchedTree, FetchedTreeFile, GitHubTreeFetcher
from .web_url import build_github_browse_url

__all__ = [
    "GITHUB_API_BASE_URL",
    "FetchedTree",
    "FetchedTreeFile",
    "GitHubAppClient",
    "GitHubAppNotConfiguredError",
    "GitHubAppValidationState",
    "GitHubTreeFetcher",
    "InstallationAuth",
    "RepositoryMetadata",
    "build_github_browse_url",
    "extract_changed_paths_from_push",
    "fetch_changed_paths_from_compare",
    "validate_github_app",
]
