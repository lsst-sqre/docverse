"""Tests for the shared manifest content-hash algorithm.

These tests pin the wire format of the manifest hash itself, not any one
producer's use of it. Two independent producers — the keeper-sync copier
and the build-processing worker — must agree byte for byte, and a
persisted ``builds.content_hash`` is only comparable across deploys if
the algorithm never silently changes. So determinism, ordering,
normalization, the empty-input value, and a known-answer digest are all
asserted here rather than left implicit in a producer's test.
"""

from __future__ import annotations

import hashlib

import pytest

from docverse_server.domain.content_hash import (
    EMPTY_MANIFEST_HASH,
    PLACEHOLDER_CONTENT_HASH,
    hash_manifest_pairs,
)

# Two files with hand-computable digests, used by the known-answer test.
_INDEX_DIGEST = hashlib.sha256(b"hello").hexdigest()
_CSS_DIGEST = hashlib.sha256(b"body {}").hexdigest()


def test_empty_input_is_empty_manifest_hash() -> None:
    """No entries hashes to the documented empty-prefix sentinel."""
    assert hash_manifest_pairs([]) == EMPTY_MANIFEST_HASH


def test_empty_manifest_hash_is_sha256_of_no_bytes() -> None:
    expected = f"sha256:{hashlib.sha256(b'').hexdigest()}"

    assert expected == EMPTY_MANIFEST_HASH


def test_placeholder_is_sha256_of_64_zeros() -> None:
    """The placeholder must satisfy ``BuildCreate.content_hash``'s regex."""
    expected = f"sha256:{'0' * 64}"

    assert expected == PLACEHOLDER_CONTENT_HASH


def test_placeholder_is_not_a_real_manifest_hash() -> None:
    """The placeholder must never collide with hashed content."""
    assert PLACEHOLDER_CONTENT_HASH != EMPTY_MANIFEST_HASH


def test_hash_is_deterministic() -> None:
    entries = [("index.html", _INDEX_DIGEST), ("css/main.css", _CSS_DIGEST)]

    assert hash_manifest_pairs(entries) == hash_manifest_pairs(entries)


def test_hash_is_independent_of_input_order() -> None:
    """Producers fan out concurrently, so entry order is not stable."""
    forward = [("index.html", _INDEX_DIGEST), ("css/main.css", _CSS_DIGEST)]
    reversed_ = list(reversed(forward))

    assert hash_manifest_pairs(forward) == hash_manifest_pairs(reversed_)


def test_hash_accepts_any_iterable() -> None:
    """A generator hashes the same as the list it would produce."""
    entries = [("index.html", _INDEX_DIGEST), ("css/main.css", _CSS_DIGEST)]

    assert hash_manifest_pairs(iter(entries)) == hash_manifest_pairs(entries)


def test_leading_dot_slash_is_normalized_away() -> None:
    """The client's ``arcname="."`` tar layout yields ``./``-prefixed keys."""
    dotted = [("./index.html", _INDEX_DIGEST), ("./css/main.css", _CSS_DIGEST)]
    bare = [("index.html", _INDEX_DIGEST), ("css/main.css", _CSS_DIGEST)]

    assert hash_manifest_pairs(dotted) == hash_manifest_pairs(bare)


def test_normalization_happens_before_sorting() -> None:
    """Sorting the raw keys would order ``./b`` before ``a``."""
    dotted = [("./b.html", _CSS_DIGEST), ("a.html", _INDEX_DIGEST)]
    bare = [("b.html", _CSS_DIGEST), ("a.html", _INDEX_DIGEST)]

    assert hash_manifest_pairs(dotted) == hash_manifest_pairs(bare)


def test_only_one_leading_dot_slash_is_stripped() -> None:
    """A literal ``./`` directory inside the tree stays part of the key."""
    assert hash_manifest_pairs([("././a.html", _INDEX_DIGEST)]) != (
        hash_manifest_pairs([("a.html", _INDEX_DIGEST)])
    )


def test_hash_distinguishes_keys_from_digests() -> None:
    """Swapping a key and its digest must change the hash."""
    assert hash_manifest_pairs([("index.html", _INDEX_DIGEST)]) != (
        hash_manifest_pairs([(_INDEX_DIGEST, "index.html")])
    )


def test_known_answer_digest() -> None:
    """Pin the exact digest so the algorithm cannot change silently.

    ``builds.content_hash`` is persisted and compared across deploys, so
    a change to the line format, separator, or ordering would silently
    partition old rows from new ones instead of failing loudly.
    """
    entries = [("index.html", _INDEX_DIGEST), ("css/main.css", _CSS_DIGEST)]

    assert hash_manifest_pairs(entries) == (
        "sha256:b69b0dc8f484e769a6814e2af8771f866c70cde2bb5c7af3454156e221d6267c"
    )


@pytest.mark.parametrize(
    "entries",
    [
        [],
        [("index.html", _INDEX_DIGEST)],
        [("index.html", _INDEX_DIGEST), ("css/main.css", _CSS_DIGEST)],
    ],
)
def test_hash_has_prefixed_sha256_shape(
    entries: list[tuple[str, str]],
) -> None:
    """Every result satisfies the ``sha256:<64 hex>`` column contract."""
    value = hash_manifest_pairs(entries)

    prefix, _, hexdigest = value.partition(":")
    assert prefix == "sha256"
    assert len(hexdigest) == 64
    assert set(hexdigest) <= set("0123456789abcdef")
