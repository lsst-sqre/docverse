"""Tests for project client models, focusing on the GitHub binding."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from docverse.client.models import (
    ProjectCreate,
    ProjectGitHubBinding,
    ProjectGitHubBindingCreate,
    ProjectUpdate,
)
from docverse.client.models.projects import parse_github_url


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (
            "https://github.com/lsst/pipelines_lsst_io",
            ("lsst", "pipelines_lsst_io"),
        ),
        (
            "https://github.com/lsst/pipelines_lsst_io.git",
            ("lsst", "pipelines_lsst_io"),
        ),
        (
            "https://github.com/lsst/pipelines_lsst_io/tree/main/docs",
            ("lsst", "pipelines_lsst_io"),
        ),
        (
            "https://GITHUB.COM/lsst/pipelines_lsst_io",
            ("lsst", "pipelines_lsst_io"),
        ),
        ("https://gitlab.com/lsst/pipelines", None),
        ("https://example.com/lsst/pipelines", None),
        ("https://github.com/lsst", None),
        ("https://github.com/", None),
        ("not-a-url", None),
    ],
)
def test_parse_github_url(url: str, expected: tuple[str, str] | None) -> None:
    assert parse_github_url(url) == expected


def test_binding_create_accepts_valid_pair() -> None:
    binding = ProjectGitHubBindingCreate(owner="lsst-sqre", repo="docverse")
    assert binding.owner == "lsst-sqre"
    assert binding.repo == "docverse"


def test_binding_create_rejects_invalid_owner() -> None:
    with pytest.raises(ValidationError):
        ProjectGitHubBindingCreate(owner="-bad", repo="docverse")


def test_binding_create_rejects_invalid_repo() -> None:
    with pytest.raises(ValidationError):
        ProjectGitHubBindingCreate(owner="lsst", repo="bad repo")


def test_binding_create_rejects_owner_too_long() -> None:
    with pytest.raises(ValidationError):
        ProjectGitHubBindingCreate(owner="a" * 40, repo="docverse")


def test_binding_create_rejects_extra_fields() -> None:
    with pytest.raises(ValidationError):
        ProjectGitHubBindingCreate.model_validate(
            {"owner": "lsst", "repo": "docverse", "installation_id": 1}
        )


def test_binding_response_includes_installation_id() -> None:
    expected = 42
    binding = ProjectGitHubBinding(
        owner="lsst", repo="docverse", installation_id=expected
    )
    assert binding.installation_id == expected


def test_binding_response_installation_id_defaults_none() -> None:
    binding = ProjectGitHubBinding(owner="lsst", repo="docverse")
    assert binding.installation_id is None


def test_project_create_accepts_github_without_source_url() -> None:
    proj = ProjectCreate(
        slug="docs",
        title="Docs",
        github=ProjectGitHubBindingCreate(owner="lsst", repo="docverse"),
    )
    assert proj.github is not None
    assert proj.github.owner == "lsst"
    assert proj.source_url is None


def test_project_create_accepts_source_url_without_github() -> None:
    proj = ProjectCreate(
        slug="docs",
        title="Docs",
        source_url="https://github.com/lsst/docverse",
    )
    assert proj.source_url == "https://github.com/lsst/docverse"
    assert proj.github is None


def test_project_create_accepts_agreeing_pair() -> None:
    proj = ProjectCreate(
        slug="docs",
        title="Docs",
        source_url="https://github.com/lsst/docverse",
        github=ProjectGitHubBindingCreate(owner="lsst", repo="docverse"),
    )
    assert proj.github is not None


def test_project_create_rejects_disagreeing_pair() -> None:
    with pytest.raises(ValidationError):
        ProjectCreate(
            slug="docs",
            title="Docs",
            source_url="https://github.com/lsst/docverse",
            github=ProjectGitHubBindingCreate(owner="other", repo="repo"),
        )


def test_project_create_accepts_non_github_source_url_with_github() -> None:
    """A non-GitHub source URL plus a github sub-object skips the check."""
    proj = ProjectCreate(
        slug="docs",
        title="Docs",
        source_url="https://gitlab.com/lsst/mirror",
        github=ProjectGitHubBindingCreate(owner="lsst", repo="docverse"),
    )
    assert proj.github is not None
    assert proj.source_url == "https://gitlab.com/lsst/mirror"


def test_project_update_accepts_explicit_null_clears() -> None:
    update = ProjectUpdate.model_validate({"github": None, "source_url": None})
    assert update.github is None
    assert update.source_url is None
    assert "github" in update.model_fields_set
    assert "source_url" in update.model_fields_set


def test_project_update_rejects_disagreeing_pair() -> None:
    with pytest.raises(ValidationError):
        ProjectUpdate.model_validate(
            {
                "source_url": "https://github.com/lsst/docverse",
                "github": {"owner": "other", "repo": "repo"},
            }
        )
