"""Tarball creation utilities for build uploads."""

from __future__ import annotations

import hashlib
import io
import tarfile
import tempfile
from collections.abc import Buffer
from pathlib import Path

__all__ = ["create_tarball"]


class _HashingWriter(io.RawIOBase):
    """File wrapper that computes a SHA-256 hash while writing."""

    def __init__(self, fileobj: io.RawIOBase) -> None:
        self._fileobj = fileobj
        self._hasher = hashlib.sha256()

    def write(self, b: Buffer) -> int:
        data = bytes(b)
        n = self._fileobj.write(data)
        if n is not None:
            self._hasher.update(data[:n])
        return n

    @property
    def hex_digest(self) -> str:
        return self._hasher.hexdigest()

    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        return True


def create_tarball(source_dir: str | Path) -> tuple[Path, str]:
    """Create a gzipped tarball from a directory.

    Parameters
    ----------
    source_dir
        Path to the directory whose contents should be archived.

    Returns
    -------
    tuple
        ``(tarball_path, content_hash)`` where *content_hash* is formatted
        as ``sha256:<hex>``. The caller is responsible for deleting the
        temporary file.
    """
    source = Path(source_dir).resolve()
    tmp = tempfile.NamedTemporaryFile(  # noqa: SIM115
        suffix=".tar.gz", delete=False
    )
    try:
        raw = Path(tmp.name).open("wb", buffering=0)  # noqa: SIM115
        writer = _HashingWriter(raw)
        with tarfile.open(mode="w:gz", fileobj=writer) as tar:
            tar.add(str(source), arcname=".")
        raw.close()
    except BaseException:
        Path(tmp.name).unlink(missing_ok=True)
        raise

    return Path(tmp.name), f"sha256:{writer.hex_digest}"
