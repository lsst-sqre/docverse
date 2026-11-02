"""Tarball creation utilities for build uploads."""

from __future__ import annotations

import gzip
import hashlib
import io
import tarfile
import tempfile
from collections.abc import Buffer
from pathlib import Path
from typing import NamedTuple

from .excluded_paths import is_excluded_path

__all__ = ["Tarball", "create_tarball"]


class Tarball(NamedTuple):
    """A tarball written by `create_tarball`."""

    path: Path
    """Path to the temporary ``.tar.gz`` file. The caller deletes it."""

    content_hash: str
    """SHA-256 digest of the compressed bytes, as ``sha256:<hex>``."""

    excluded_count: int
    """Number of paths left out because `is_excluded_path` matched them.

    An excluded directory counts once: it is not traversed, so the files
    beneath it are neither archived nor counted.
    """

    file_count: int
    """Number of regular files archived (directories are not counted)."""


class _ExcludedPathFilter:
    """``tarfile`` member filter that drops excluded paths and counts.

    Returning `None` from a ``TarFile.add`` filter leaves the member out;
    for a directory it also stops ``tarfile`` from descending into it.
    """

    def __init__(self) -> None:
        self.excluded_count = 0
        self.file_count = 0

    def __call__(self, tarinfo: tarfile.TarInfo) -> tarfile.TarInfo | None:
        if is_excluded_path(tarinfo.name):
            self.excluded_count += 1
            return None
        if tarinfo.isfile():
            self.file_count += 1
        return tarinfo


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


def create_tarball(source_dir: str | Path) -> Tarball:
    """Create a gzipped tarball from a directory.

    Paths matched by `docverse.excluded_paths.is_excluded_path` (Sphinx's
    ``.doctrees/`` and ``.buildinfo``, VCS directories, Python caches, and
    so on) are left out of the archive, so they never reach Docverse and
    never affect the digest.

    Parameters
    ----------
    source_dir
        Path to the directory whose contents should be archived.

    Returns
    -------
    Tarball
        The temporary tarball's path, its ``sha256:<hex>`` content hash,
        the number of excluded paths, and the number of files archived.
        The caller is responsible for deleting the temporary file.

    Notes
    -----
    The digest covers the compressed bytes, so the gzip header has to be
    stable for identical content to hash identically. ``tarfile``'s own
    ``w:gz`` mode stamps the current time into that header, which made the
    digest change every second; the gzip stream is therefore built
    explicitly with ``mtime=0``.
    """
    source = Path(source_dir).resolve()
    member_filter = _ExcludedPathFilter()
    tmp = tempfile.NamedTemporaryFile(  # noqa: SIM115
        suffix=".tar.gz", delete=False
    )
    try:
        with Path(tmp.name).open("wb", buffering=0) as raw:
            writer = _HashingWriter(raw)
            with (
                gzip.GzipFile(
                    filename="", mode="wb", fileobj=writer, mtime=0
                ) as gz,
                tarfile.open(mode="w", fileobj=gz) as tar,
            ):
                tar.add(str(source), arcname=".", filter=member_filter)
    except BaseException:
        Path(tmp.name).unlink(missing_ok=True)
        raise

    return Tarball(
        path=Path(tmp.name),
        content_hash=f"sha256:{writer.hex_digest}",
        excluded_count=member_filter.excluded_count,
        file_count=member_filter.file_count,
    )
