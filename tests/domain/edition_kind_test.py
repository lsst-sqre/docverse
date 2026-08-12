"""Tests for the promote-only edition-kind transition policy."""

from __future__ import annotations

import pytest

from docverse.models import EditionKind
from docverse_server.domain.edition_kind import is_kind_promotion

__all__ = []


def test_draft_to_release_is_a_promotion() -> None:
    """The one healing transition the derivation paths may write."""
    assert is_kind_promotion(EditionKind.draft, EditionKind.release) is True


def test_unchanged_kind_is_not_a_promotion() -> None:
    """A freshly derived kind matching the row's kind writes nothing."""
    for kind in EditionKind:
        assert is_kind_promotion(kind, kind) is False


def test_release_is_never_demoted() -> None:
    """A manual PATCH to ``release`` survives every later derivation."""
    assert is_kind_promotion(EditionKind.release, EditionKind.draft) is False


@pytest.mark.parametrize(
    "current",
    [
        EditionKind.main,
        EditionKind.major,
        EditionKind.minor,
        EditionKind.alternate,
    ],
)
def test_structural_kinds_are_never_rewritten(current: EditionKind) -> None:
    """``main`` and the aggregate/alternate kinds are never a source."""
    for derived in EditionKind:
        assert is_kind_promotion(current, derived) is False
