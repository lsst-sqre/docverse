"""GitHub App infrastructure shared by the sync worker and webhooks."""

from __future__ import annotations

from .app_client import (
    GITHUB_API_BASE_URL,
    GitHubAppClient,
    GitHubAppNotConfiguredError,
    InstallationAuth,
)
from .changed_paths import (
    extract_changed_paths_from_push,
    fetch_changed_paths_from_compare,
)
from .tree_fetcher import FetchedTree, FetchedTreeFile, GitHubTreeFetcher

__all__ = [
    "GITHUB_API_BASE_URL",
    "FetchedTree",
    "FetchedTreeFile",
    "GitHubAppClient",
    "GitHubAppNotConfiguredError",
    "GitHubTreeFetcher",
    "InstallationAuth",
    "extract_changed_paths_from_push",
    "fetch_changed_paths_from_compare",
]
