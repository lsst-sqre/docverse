"""Tests for the edition pointer domain model and its key builder."""

from __future__ import annotations

import dataclasses

import pytest

from docverse_server.domain.edition_pointer import (
    EditionPointer,
    edition_pointer_key,
)


def test_key_joins_project_and_edition_slugs() -> None:
    """The CDN key is exactly what the Worker resolver looks up."""
    assert edition_pointer_key("myproject", "main") == "myproject/main"


def test_pointer_is_frozen() -> None:
    """A pointer is a read of the edge, never a thing to mutate."""
    pointer = EditionPointer(
        build_public_id="ABC123",
        r2_prefix="myproject/__builds/ABC123/",
        cache_profile="long",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        pointer.build_public_id = "DEF456"  # type: ignore[misc]
