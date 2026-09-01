"""Tests for docverse.models.builds."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from docverse.models import BuildCreate


def test_content_hash_is_optional() -> None:
    """``content_hash`` may be omitted entirely.

    The field is a deprecated transport digest: content identity is the
    server's to compute, so a client that has nothing to say about the
    bytes it uploaded must be able to stay silent rather than invent a
    value.
    """
    data = BuildCreate(git_ref="main")
    assert data.content_hash is None
    assert "content_hash" not in data.model_dump(exclude_none=True)


def test_content_hash_pattern_still_enforced() -> None:
    """A supplied digest must still match ``sha256:<64 hex>``.

    Optional widens what may be omitted, not what may be sent: an old
    client that sends a malformed digest is still rejected rather than
    silently writing junk into a column the convergence lookup reads.
    """
    with pytest.raises(ValidationError):
        BuildCreate(git_ref="main", content_hash="not-a-digest")


def test_content_hash_accepted_when_supplied() -> None:
    """A well-formed digest from an old client round-trips unchanged."""
    digest = "sha256:" + "a" * 64
    data = BuildCreate(git_ref="main", content_hash=digest)
    assert data.content_hash == digest


def test_content_hash_documented_as_deprecated() -> None:
    """The deprecation lives in the field description, not a warning.

    There is no runtime ``DeprecationWarning``, so the description is
    the only signal an integrator gets — in the OpenAPI schema and in
    ``help()`` alike.
    """
    description = BuildCreate.model_fields["content_hash"].description
    assert description is not None
    assert "deprecated" in description.lower()
