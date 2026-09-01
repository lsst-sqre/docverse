"""The content-identity hash for build content.

A build's content identity is a ``sha256`` over a sorted, newline-
delimited table of ``relative_key<TAB>sha256(file_bytes)`` lines. It is
deliberately independent of *how* the content arrived: an LTD prefix
copied by keeper-sync and a tarball uploaded by the client hash to the
same value when they hold the same files, which is what lets a build
that already exists be recognized instead of re-uploaded (dual-upload
convergence).

That property only holds while every producer computes the hash the
same way, so the algorithm lives here as pure functions rather than in
any one producer. Key normalization is part of the algorithm and lives
here too: the client's tarballs are written with ``arcname="."`` and so
carry ``./``-prefixed member names, while the copier derives bare
relative keys by stripping an S3 prefix. Normalizing inside the hash
means neither producer can forget to.

The hash is persisted as ``builds.content_hash`` and compared across
deploys, so changing the line format, the separator, or the ordering
would silently partition old rows from new ones. Treat it as a wire
format: ``tests/domain/content_hash_test.py`` pins it with a
known-answer digest.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

__all__ = [
    "EMPTY_MANIFEST_HASH",
    "PLACEHOLDER_CONTENT_HASH",
    "hash_manifest_pairs",
]

EMPTY_MANIFEST_HASH = f"sha256:{hashlib.sha256(b'').hexdigest()}"
"""Manifest hash of content with no files in it.

Falls out of the algorithm — no entries means no lines fed to the
hasher — but is named because a caller cannot otherwise tell "hashed
nothing" apart from "hashed real content" by looking at the returned
value, and keeper-sync has to make exactly that distinction when
deciding whether its edition-prefix fallback actually recovered
anything (#516).
"""

PLACEHOLDER_CONTENT_HASH = f"sha256:{'0' * 64}"
"""Stand-in written to a build row whose content is not yet known.

Used on freshly-created pending rows — keeper-sync's placeholder builds,
and client uploads that supply no transport digest — and overwritten
with the real manifest hash once the content has been hashed. It matches
the ``sha256:<64 hex>`` shape the column and
:class:`~docverse.models.builds.BuildCreate` require, and cannot collide
with a real manifest hash: no input hashes to 64 zeros.
"""


def hash_manifest_pairs(entries: Iterable[tuple[str, str]]) -> str:
    r"""Hash a manifest of ``(relative_key, file_digest)`` pairs.

    Parameters
    ----------
    entries
        One ``(relative_key, digest)`` pair per file, in any order.
        ``relative_key`` is the file's path relative to the root of the
        build content, with or without a leading ``./``; ``digest`` is
        the bare hex ``sha256`` of that file's bytes. Any iterable is
        accepted, so a producer can hand over a generator over its own
        entries.

    Returns
    -------
    str
        ``sha256:<64 hex chars>`` over the sorted
        ``relative_key\tdigest\n`` lines, or :data:`EMPTY_MANIFEST_HASH`
        when ``entries`` is empty.

    Notes
    -----
    Sorting and ``./``-stripping both happen here, in that order, so a
    producer's fan-out order and tar layout cannot affect the result.
    File size is deliberately not part of a manifest line: it is
    derivable from the bytes already hashed, and including it would
    couple the hash to a second representation of the same fact.
    """
    normalized = sorted(
        (_normalize_relative_key(key), digest) for key, digest in entries
    )
    hasher = hashlib.sha256()
    for key, digest in normalized:
        hasher.update(f"{key}\t{digest}\n".encode())
    return f"sha256:{hasher.hexdigest()}"


def _normalize_relative_key(key: str) -> str:
    """Strip the leading ``./`` a ``arcname="."`` tarball member carries.

    Only one leading ``./`` is removed: deeper occurrences are real path
    segments the producer chose, and collapsing them would make two
    genuinely different trees hash alike.
    """
    return key.removeprefix("./")
