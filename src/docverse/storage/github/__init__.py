"""GitHub App infrastructure shared by the sync worker and webhooks."""

from __future__ import annotations

from .app_client import GitHubAppClient, GitHubAppNotConfiguredError
from .changed_paths import (
    extract_changed_paths_from_push,
    fetch_changed_paths_from_compare,
)
from .tree_fetcher import FetchedTree, FetchedTreeFile, GitHubTreeFetcher

__all__ = [
    "FetchedTree",
    "FetchedTreeFile",
    "GitHubAppClient",
    "GitHubAppNotConfiguredError",
    "GitHubTreeFetcher",
    "extract_changed_paths_from_push",
    "fetch_changed_paths_from_compare",
]
