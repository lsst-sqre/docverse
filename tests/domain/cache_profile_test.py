"""Tests for cache-profile derivation."""

from __future__ import annotations

import pytest

from docverse.models import EditionKind
from docverse_server.domain.cache_profile import (
    CACHE_PROFILE_LONG,
    CACHE_PROFILE_SHORT,
    compute_cache_profile,
)


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        (EditionKind.main, CACHE_PROFILE_LONG),
        (EditionKind.release, CACHE_PROFILE_LONG),
        (EditionKind.major, CACHE_PROFILE_LONG),
        (EditionKind.minor, CACHE_PROFILE_LONG),
        (EditionKind.draft, CACHE_PROFILE_SHORT),
        (EditionKind.alternate, CACHE_PROFILE_SHORT),
    ],
)
def test_compute_cache_profile(kind: EditionKind, expected: str) -> None:
    assert compute_cache_profile(kind) == expected


@pytest.mark.parametrize("kind", list(EditionKind))
def test_every_kind_maps_to_a_known_profile(kind: EditionKind) -> None:
    """A newly-added ``EditionKind`` must still get a valid profile."""
    assert compute_cache_profile(kind) in {
        CACHE_PROFILE_LONG,
        CACHE_PROFILE_SHORT,
    }
