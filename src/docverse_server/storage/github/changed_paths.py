"""Extract the set of changed paths from a GitHub push event.

The webhook path (in-payload) and the API fallback (compare API) live
side-by-side so the webhook handler can try the cheap path first and
fall back to the API only when it can prove the payload is truncated.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import httpx

from .app_client import InstallationAuth

__all__ = [
    "extract_changed_paths_from_push",
    "fetch_changed_paths_from_compare",
]


_MAX_PUSH_COMMITS = 20
"""GitHub delivers at most 20 commits per push webhook payload.

If a push has more than 20 commits, the ``commits`` array is truncated
and the payload's ``size`` field tells us the true count. The ``>= 20``
boundary is a defensive upper bound; ``size > len(commits)`` is the
authoritative signal.
"""


def extract_changed_paths_from_push(
    payload: Mapping[str, Any],
) -> list[str] | None:
    """Return the sorted set of paths touched by a push payload.

    Returns ``None`` if the payload's ``commits`` array is (or may be)
    truncated. The caller MUST then fall back to
    :func:`fetch_changed_paths_from_compare` using ``payload["before"]``
    and ``payload["after"]`` — the compare API is authoritative for
    pushes that exceed the webhook size limits.

    GitHub truncates at 20 commits per push and 3000 files per commit.
    The 20-commit truncation is detectable via ``size > len(commits)``;
    the per-commit 3000-file truncation has no explicit signal, so
    touching that limit is rare enough that the compare fallback is
    wired by the caller when it has other reason to suspect truncation.
    """
    commits = payload.get("commits")
    if commits is None:
        return None
    size = payload.get("size")
    if isinstance(size, int) and size > len(commits):
        return None
    if len(commits) >= _MAX_PUSH_COMMITS:
        return None

    paths: set[str] = set()
    for commit in commits:
        if not isinstance(commit, Mapping):
            continue
        for key in ("added", "modified", "removed"):
            entries = commit.get(key)
            if not entries:
                continue
            for path in entries:
                if isinstance(path, str):
                    paths.add(path)
    return sorted(paths)


async def fetch_changed_paths_from_compare(
    client: httpx.AsyncClient,
    *,
    auth: InstallationAuth,
    owner: str,
    repo: str,
    before: str,
    after: str,
) -> list[str]:
    """Return the sorted set of paths changed between two commits.

    Calls ``GET /repos/{owner}/{repo}/compare/{before}...{after}`` on
    the shared ``client`` with ``auth.base_url`` + an
    ``Authorization: Bearer {auth.token}`` header attached per request,
    so the client's defaults stay untouched. Unions each entry's
    ``filename`` (new path) with any ``previous_filename`` (old path on
    renames). Both sides are needed so a rename whose new path is
    outside the binding's ``root_path`` but whose old path was inside
    still triggers a resync.
    """
    response = await client.get(
        f"{auth.base_url}/repos/{owner}/{repo}/compare/{before}...{after}",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {auth.token}",
        },
    )
    response.raise_for_status()
    data = response.json()
    files = data.get("files") or []

    paths: set[str] = set()
    for entry in files:
        if not isinstance(entry, Mapping):
            continue
        filename = entry.get("filename")
        if isinstance(filename, str):
            paths.add(filename)
        previous = entry.get("previous_filename")
        if isinstance(previous, str):
            paths.add(previous)
    return sorted(paths)
