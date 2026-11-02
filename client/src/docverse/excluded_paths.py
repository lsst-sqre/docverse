"""Path names that Docverse never ingests into a build.

Documentation tools leave caches and version-control metadata next to the
rendered site: Sphinx's ``.doctrees/`` pickles and ``.buildinfo``, Python
bytecode caches, Jupyter checkpoints, VCS directories, and desktop
operating-system litter. None of it is part of the published site, and the
Sphinx pickles in particular can add tens of megabytes to every build.

This module is the single source of truth for that list. ``docverse
upload`` applies it when creating the tarball, and the Docverse server
applies the same predicate when it unpacks an upload or copies a build
from LSST the Docs, so every ingest path agrees on what a build contains.
The list is fixed in code: there is no per-deployment or per-organization
setting.
"""

from __future__ import annotations

__all__ = ["EXCLUDED_PATH_NAMES", "is_excluded_path"]

EXCLUDED_PATH_NAMES: frozenset[str] = frozenset(
    {
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
)
"""File and directory names excluded from every build.

A path is excluded when any one of its segments equals one of these names
exactly (see `is_excluded_path`).
"""


def is_excluded_path(relative_path: str) -> bool:
    """Report whether a build-relative path is excluded from ingest.

    Parameters
    ----------
    relative_path
        A ``/``-separated path relative to the build root, such as a
        tarball member name or an object-store key with the build prefix
        removed. A leading ``./`` (as carried by tarball members) gives the
        same answer as the bare relative path.

    Returns
    -------
    bool
        `True` when any segment of the path equals one of
        `EXCLUDED_PATH_NAMES`. Comparison is exact and case-sensitive: there
        is no globbing or substring matching, so ``foo.doctrees`` and
        ``.well-known`` are not excluded. Because any segment counts, caches
        inside nested sub-sites (``a/b/.doctrees/x``) are excluded too.
    """
    return not EXCLUDED_PATH_NAMES.isdisjoint(relative_path.split("/"))
