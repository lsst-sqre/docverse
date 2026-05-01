"""Build GitHub browse-UI URLs from binding source coordinates."""

from __future__ import annotations

__all__ = ["build_github_browse_url"]


def build_github_browse_url(
    *,
    owner: str,
    repo: str,
    ref: str,
    root_path: str,
) -> str:
    """Return the GitHub browse-UI URL for a binding's source location.

    Yields ``https://github.com/{owner}/{repo}/tree/{ref}`` when
    ``root_path`` is the repo root, and appends the (slash-normalised)
    path segment otherwise. ``ref`` is passed through verbatim — GitHub
    accepts embedded slashes (``release/1.0``) directly in the
    ``/tree/...`` route without URL-encoding.
    """
    base = f"https://github.com/{owner}/{repo}/tree/{ref}"
    normalized_path = root_path.strip("/")
    if not normalized_path:
        return base
    return f"{base}/{normalized_path}"
