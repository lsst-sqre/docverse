"""Tests for dashboard-template binding client models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from docverse.client.models import DashboardTemplateBindingCreate
from docverse.client.models.dashboard_template import normalize_github_ref


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("main", "main"),
        ("refs/heads/main", "main"),
        ("refs/heads/release/1.0", "release/1.0"),
        ("refs/tags/v1.2.0", "v1.2.0"),
        ("refs/tags/release/1.0", "release/1.0"),
        # 40-char hex SHAs must pass through untouched.
        ("a" * 40, "a" * 40),
        # Non-tracked refs (e.g. pull/PR refs) pass through unchanged.
        ("refs/pull/123/head", "refs/pull/123/head"),
    ],
)
def test_normalize_github_ref_strips_known_prefixes(
    raw: str, expected: str
) -> None:
    assert normalize_github_ref(raw) == expected


def test_create_normalizes_refs_heads_prefix() -> None:
    """``refs/heads/main`` round-trips as the bare ``main`` ref."""
    binding = DashboardTemplateBindingCreate(
        github_owner="acme",
        github_repo="templates",
        github_ref="refs/heads/main",
    )
    assert binding.github_ref == "main"


def test_create_normalizes_refs_tags_prefix() -> None:
    """``refs/tags/v1.0`` round-trips as the bare ``v1.0`` tag."""
    binding = DashboardTemplateBindingCreate(
        github_owner="acme",
        github_repo="templates",
        github_ref="refs/tags/v1.0",
    )
    assert binding.github_ref == "v1.0"


def test_create_passes_bare_ref_through() -> None:
    """A bare ref (no prefix) is the canonical storage form."""
    binding = DashboardTemplateBindingCreate(
        github_owner="acme",
        github_repo="templates",
        github_ref="main",
    )
    assert binding.github_ref == "main"


def test_create_passes_sha_through() -> None:
    """A 40-char hex SHA must not be normalized."""
    sha = "0" * 40
    binding = DashboardTemplateBindingCreate(
        github_owner="acme",
        github_repo="templates",
        github_ref=sha,
    )
    assert binding.github_ref == sha


def test_create_rejects_empty_ref() -> None:
    """An empty string still fails the ``min_length=1`` constraint."""
    with pytest.raises(ValidationError):
        DashboardTemplateBindingCreate(
            github_owner="acme",
            github_repo="templates",
            github_ref="",
        )


def test_create_rejects_refs_heads_alone() -> None:
    """``refs/heads/`` with no branch name normalizes to ``""`` and fails."""
    with pytest.raises(ValidationError):
        DashboardTemplateBindingCreate(
            github_owner="acme",
            github_repo="templates",
            github_ref="refs/heads/",
        )


def test_create_rejects_refs_tags_alone() -> None:
    """``refs/tags/`` with no tag name normalizes to ``""`` and fails."""
    with pytest.raises(ValidationError):
        DashboardTemplateBindingCreate(
            github_owner="acme",
            github_repo="templates",
            github_ref="refs/tags/",
        )
