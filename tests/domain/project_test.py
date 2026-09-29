"""Tests for the project domain model."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from docverse_server.domain.project import FALLBACK_DEFAULT_BRANCH, Project


def _project(*, github_default_branch: str | None) -> Project:
    now = datetime(2026, 9, 25, tzinfo=UTC)
    return Project(
        id=1,
        public_id=1,
        slug="proj",
        title="Project",
        org_id=1,
        github_default_branch=github_default_branch,
        date_created=now,
        date_updated=now,
    )


@pytest.mark.parametrize(
    ("column", "expected"),
    [
        (None, FALLBACK_DEFAULT_BRANCH),
        ("main", "main"),
        ("master", "master"),
    ],
)
def test_effective_default_branch(column: str | None, expected: str) -> None:
    """The recorded default branch wins; ``main`` stands in until then."""
    project = _project(github_default_branch=column)

    assert project.effective_default_branch == expected


def test_fallback_default_branch_is_main() -> None:
    """Projects with no recorded default branch behave as before."""
    assert FALLBACK_DEFAULT_BRANCH == "main"
