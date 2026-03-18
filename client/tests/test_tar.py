"""Tests for docverse.client._tar."""

from __future__ import annotations

import re
import tarfile
from pathlib import Path

from docverse.client._tar import create_tarball


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

    _, hash1 = create_tarball(source)
    path2, hash2 = create_tarball(source)
    try:
        assert hash1 == hash2
    finally:
        path2.unlink(missing_ok=True)
