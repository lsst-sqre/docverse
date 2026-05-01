"""Tests for the GitHub browse-URL helper."""

from __future__ import annotations

import pytest

from docverse.storage.github import build_github_browse_url


def test_root_path_omits_path_segment() -> None:
    """``root_path == "/"`` produces a bare ``/tree/{ref}`` URL."""
    url = build_github_browse_url(
        owner="lsst-sqre",
        repo="docverse-templates",
        ref="main",
        root_path="/",
    )
    assert url == "https://github.com/lsst-sqre/docverse-templates/tree/main"


def test_subdirectory_root_path_appends_without_double_slash() -> None:
    """A ``/templates/default`` root path appends with one separator."""
    url = build_github_browse_url(
        owner="lsst-sqre",
        repo="docverse-templates",
        ref="main",
        root_path="/templates/default",
    )
    assert url == (
        "https://github.com/lsst-sqre/docverse-templates"
        "/tree/main/templates/default"
    )


def test_root_path_without_leading_slash() -> None:
    """A leading-slash-less root path is treated like the rooted form."""
    url = build_github_browse_url(
        owner="lsst-sqre",
        repo="docverse-templates",
        ref="main",
        root_path="templates/default",
    )
    assert url == (
        "https://github.com/lsst-sqre/docverse-templates"
        "/tree/main/templates/default"
    )


def test_root_path_with_trailing_slash_is_stripped() -> None:
    """Trailing slashes on the root path are normalized away."""
    url = build_github_browse_url(
        owner="lsst-sqre",
        repo="docverse-templates",
        ref="main",
        root_path="/templates/default/",
    )
    assert url == (
        "https://github.com/lsst-sqre/docverse-templates"
        "/tree/main/templates/default"
    )


def test_ref_with_slash_is_preserved() -> None:
    """A ref containing ``/`` (e.g. ``release/1.0``) appears verbatim.

    GitHub's browse UI renders ``/tree/release/1.0`` correctly without
    URL-encoding the embedded slash, so the helper passes the ref
    through unchanged. Refs with embedded slashes are common for tag
    namespaces and long-lived release branches.
    """
    url = build_github_browse_url(
        owner="lsst-sqre",
        repo="docverse-templates",
        ref="release/1.0",
        root_path="/",
    )
    assert url == (
        "https://github.com/lsst-sqre/docverse-templates/tree/release/1.0"
    )


def test_ref_with_slash_and_subdirectory_root() -> None:
    """A slash-bearing ref combined with a subdirectory root."""
    url = build_github_browse_url(
        owner="lsst-sqre",
        repo="docverse-templates",
        ref="release/1.0",
        root_path="/templates/default",
    )
    assert url == (
        "https://github.com/lsst-sqre/docverse-templates"
        "/tree/release/1.0/templates/default"
    )


@pytest.mark.parametrize("empty_root", ["", "/", "//"])
def test_various_empty_root_paths_omit_segment(empty_root: str) -> None:
    """Inputs that normalize to empty all yield the bare ``/tree/{ref}``."""
    url = build_github_browse_url(
        owner="lsst-sqre",
        repo="docverse-templates",
        ref="main",
        root_path=empty_root,
    )
    assert url == "https://github.com/lsst-sqre/docverse-templates/tree/main"
