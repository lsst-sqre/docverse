"""Tests for docverse._tar."""

from __future__ import annotations

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

    tarball_path, content_hash = create_tarball(source)
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

    path1, hash1 = create_tarball(source)
    path2, hash2 = create_tarball(source)
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
    path1, hash1 = create_tarball(source)
    monkeypatch.setattr(time, "time", lambda: 2_000_000_000.0)
    path2, hash2 = create_tarball(source)
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

    tarball_path, _ = create_tarball(source)
    try:
        header = tarball_path.read_bytes()[:10]
        assert header[:2] == b"\x1f\x8b"  # gzip magic
        # Bytes 4-7 are the little-endian MTIME field.
        assert header[4:8] == b"\x00\x00\x00\x00"
        # Bit 3 of FLG would mean an original filename is embedded.
        assert not header[3] & 0x08
    finally:
        tarball_path.unlink(missing_ok=True)
