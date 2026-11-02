"""Known-answer tests for docverse.excluded_paths."""

from __future__ import annotations

import pytest

import docverse
from docverse.excluded_paths import EXCLUDED_PATH_NAMES, is_excluded_path


def test_excluded_path_names_known_answer() -> None:
    """The list is pinned exactly; changing it is a deliberate API change."""
    expected = {
        ".doctrees",
        ".buildinfo",
        ".git",
        "__pycache__",
        ".pytest_cache",
        ".ipynb_checkpoints",
        ".DS_Store",
        "Thumbs.db",
        ".hg",
        ".svn",
    }
    assert expected == EXCLUDED_PATH_NAMES
    assert isinstance(EXCLUDED_PATH_NAMES, frozenset)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        # Top-level matches, file and directory alike
        (".doctrees", True),
        (".doctrees/environment.pickle", True),
        (".buildinfo", True),
        (".DS_Store", True),
        ("Thumbs.db", True),
        # Any segment matches, so nested sub-sites are covered
        ("a/b/.doctrees/x", True),
        ("subsite/.buildinfo", True),
        ("pkg/__pycache__/mod.cpython-312.pyc", True),
        ("notebooks/.ipynb_checkpoints/n-checkpoint.ipynb", True),
        ("v1/.git/HEAD", True),
        ("v1/.hg/store", True),
        ("v1/.svn/entries", True),
        ("tests/.pytest_cache/README.md", True),
        ("img/.DS_Store", True),
        # Ordinary site content is kept
        ("index.html", False),
        ("_sources/x.rst.txt", False),
        ("_static/basic.css", False),
        ("docs/.well-known/x", False),
        (".nojekyll", False),
        # Exact comparison: no globbing, no substring, case-sensitive
        ("foo.doctrees/x", False),
        (".doctrees-old/x", False),
        ("x.buildinfo", False),
        ("my__pycache__/x", False),
        (".ds_store", False),
        ("thumbs.db", False),
    ],
)
def test_is_excluded_path(path: str, expected: bool) -> None:  # noqa: FBT001
    assert is_excluded_path(path) is expected


@pytest.mark.parametrize(
    "path",
    [
        ".doctrees/x.pickle",
        "a/b/.doctrees/x",
        ".buildinfo",
        "_sources/index.rst.txt",
        "docs/.well-known/x",
        "index.html",
    ],
)
def test_is_excluded_path_dot_slash_prefix(path: str) -> None:
    """A ``./``-prefixed path answers the same as the bare relative path.

    Tarball members written by ``create_tarball`` carry the ``./`` prefix.
    """
    assert is_excluded_path(f"./{path}") is is_excluded_path(path)


def test_root_member_is_not_excluded() -> None:
    """The ``.`` root member of a client tarball is not itself excluded."""
    assert is_excluded_path(".") is False


def test_reexported_from_package() -> None:
    assert docverse.EXCLUDED_PATH_NAMES is EXCLUDED_PATH_NAMES
    assert docverse.is_excluded_path is is_excluded_path
