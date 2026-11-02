"""Tests for docverse._tar."""

from __future__ import annotations

import os
import re
import tarfile
import time
from pathlib import Path

import pytest

from docverse._tar import create_tarball


def test_create_tarball(tmp_path: Path) -> None:
    """create_tarball produces a .tar.gz with correct contents and hash."""
    source = tmp_path / "docs"
    source.mkdir()
    (source / "index.html").write_text("<h1>Hello</h1>")
    (source / "style.css").write_text("body {}")

    result = create_tarball(source)
    tarball_path, content_hash = result.path, result.content_hash
    try:
        assert tarball_path.exists()
        assert tarball_path.suffix == ".gz"
        assert tarball_path.stem.endswith(".tar")

        # Hash format: sha256:<64 hex chars>
        assert re.fullmatch(r"sha256:[a-f0-9]{64}", content_hash)

        # Archive contains the source files at root
        with tarfile.open(tarball_path, "r:gz") as tar:
            names = {m.name for m in tar.getmembers()}
            assert "./index.html" in names
            assert "./style.css" in names
    finally:
        tarball_path.unlink(missing_ok=True)


def test_create_tarball_hash_determinism(tmp_path: Path) -> None:
    """Two calls on the same directory produce the same hash."""
    source = tmp_path / "docs"
    source.mkdir()
    (source / "page.html").write_text("<p>content</p>")

    path1, hash1, *_ = create_tarball(source)
    path2, hash2, *_ = create_tarball(source)
    try:
        assert hash1 == hash2
    finally:
        path1.unlink(missing_ok=True)
        path2.unlink(missing_ok=True)


def test_create_tarball_hash_ignores_wall_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Identical content hashes alike however far apart it is archived.

    ``test_create_tarball_hash_determinism`` only catches a time-dependent
    digest when its two calls happen to straddle an integer second, so it
    fails intermittently rather than reliably. Move the clock explicitly.
    """
    source = tmp_path / "docs"
    source.mkdir()
    (source / "page.html").write_text("<p>content</p>")

    monkeypatch.setattr(time, "time", lambda: 1_000_000_000.0)
    path1, hash1, *_ = create_tarball(source)
    monkeypatch.setattr(time, "time", lambda: 2_000_000_000.0)
    path2, hash2, *_ = create_tarball(source)
    try:
        assert hash1 == hash2
    finally:
        path1.unlink(missing_ok=True)
        path2.unlink(missing_ok=True)


def test_create_tarball_gzip_header_has_no_timestamp(tmp_path: Path) -> None:
    """The gzip MTIME field is zeroed so the digest stays content-only."""
    source = tmp_path / "docs"
    source.mkdir()
    (source / "page.html").write_text("<p>content</p>")

    tarball_path = create_tarball(source).path
    try:
        header = tarball_path.read_bytes()[:10]
        assert header[:2] == b"\x1f\x8b"  # gzip magic
        # Bytes 4-7 are the little-endian MTIME field.
        assert header[4:8] == b"\x00\x00\x00\x00"
        # Bit 3 of FLG would mean an original filename is embedded.
        assert not header[3] & 0x08
    finally:
        tarball_path.unlink(missing_ok=True)


def _write_sphinx_tree(root: Path) -> None:
    """Write a small Sphinx-like output tree with build caches beside it."""
    root.mkdir()
    (root / "index.html").write_text("<h1>Hello</h1>")
    (root / "_static").mkdir()
    (root / "_static" / "basic.css").write_text("body {}")
    (root / ".doctrees").mkdir()
    (root / ".doctrees" / "environment.pickle").write_bytes(b"\x80\x04pickle")
    (root / ".doctrees" / "index.doctree").write_bytes(b"\x80\x04doctree")
    (root / ".buildinfo").write_text("# Sphinx build info version 1\n")


def test_create_tarball_excludes_cache_paths(tmp_path: Path) -> None:
    """Excluded directories and files never enter the tarball."""
    source = tmp_path / "docs"
    _write_sphinx_tree(source)

    result = create_tarball(source)
    try:
        with tarfile.open(result.path, "r:gz") as tar:
            names = {m.name for m in tar.getmembers()}
        assert names == {
            ".",
            "./index.html",
            "./_static",
            "./_static/basic.css",
        }
        # ``.doctrees/`` (one directory, not traversed) and ``.buildinfo``
        assert result.excluded_count == 2
        assert result.file_count == 2
    finally:
        result.path.unlink(missing_ok=True)


def test_create_tarball_excludes_nested_cache_paths(tmp_path: Path) -> None:
    """A cache inside a nested sub-site is excluded too."""
    source = tmp_path / "docs"
    source.mkdir()
    (source / "index.html").write_text("<h1>Hello</h1>")
    _write_sphinx_tree(source / "subsite")

    result = create_tarball(source)
    try:
        with tarfile.open(result.path, "r:gz") as tar:
            names = {m.name for m in tar.getmembers()}
        assert "./subsite/index.html" in names
        assert not any(".doctrees" in n or ".buildinfo" in n for n in names)
        assert result.excluded_count == 2
        assert result.file_count == 3
    finally:
        result.path.unlink(missing_ok=True)


def test_create_tarball_reports_no_excluded_paths(tmp_path: Path) -> None:
    """A tree without caches reports zero excluded paths."""
    source = tmp_path / "docs"
    source.mkdir()
    (source / "index.html").write_text("<h1>Hello</h1>")

    result = create_tarball(source)
    try:
        assert result.excluded_count == 0
        assert result.file_count == 1
    finally:
        result.path.unlink(missing_ok=True)


def test_create_tarball_only_excluded_paths(tmp_path: Path) -> None:
    """A tree holding only caches archives no files."""
    source = tmp_path / "docs"
    source.mkdir()
    (source / ".doctrees").mkdir()
    (source / ".doctrees" / "environment.pickle").write_bytes(b"pickle")

    result = create_tarball(source)
    try:
        assert result.file_count == 0
        assert result.excluded_count == 1
    finally:
        result.path.unlink(missing_ok=True)


def test_create_tarball_excluded_hash_determinism(tmp_path: Path) -> None:
    """Archiving a tree with caches twice yields the same digest."""
    source = tmp_path / "docs"
    _write_sphinx_tree(source)

    result1 = create_tarball(source)
    result2 = create_tarball(source)
    try:
        assert result1.content_hash == result2.content_hash
    finally:
        result1.path.unlink(missing_ok=True)
        result2.path.unlink(missing_ok=True)


def test_create_tarball_excluded_paths_do_not_affect_hash(
    tmp_path: Path,
) -> None:
    """A tree with caches hashes like the same tree without them."""
    with_caches = tmp_path / "with"
    _write_sphinx_tree(with_caches)
    without_caches = tmp_path / "without"
    without_caches.mkdir()
    (without_caches / "index.html").write_text("<h1>Hello</h1>")
    (without_caches / "_static").mkdir()
    (without_caches / "_static" / "basic.css").write_text("body {}")

    # Tar members carry mtimes, so pin every survivor's to the same value
    # (children before their parent directories).
    for root in (with_caches, without_caches):
        for rel in ("index.html", "_static/basic.css", "_static", "."):
            os.utime(root / rel, (1_000_000_000, 1_000_000_000))

    result_with = create_tarball(with_caches)
    result_without = create_tarball(without_caches)
    try:
        assert result_with.content_hash == result_without.content_hash
    finally:
        result_with.path.unlink(missing_ok=True)
        result_without.path.unlink(missing_ok=True)
