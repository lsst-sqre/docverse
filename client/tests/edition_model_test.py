"""Tests for edition client models."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from docverse.client.models import (
    Edition,
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.client.models.queue_enums import PublishStatus


def _base_edition(**overrides: object) -> Edition:
    base: dict[str, object] = {
        "self_url": "https://example.com/editions/main",
        "project_url": "https://example.com/projects/p",
        "history_url": "https://example.com/editions/main/history",
        "rollback_url": "https://example.com/editions/main/rollback",
        "slug": "main",
        "title": "Main",
        "kind": EditionKind.main,
        "tracking_mode": TrackingMode.git_ref,
        "lifecycle_exempt": False,
        "date_created": datetime(2026, 1, 1, tzinfo=UTC),
        "date_updated": datetime(2026, 1, 2, tzinfo=UTC),
    }
    base.update(overrides)
    return Edition.model_validate(base)


def test_edition_publish_status_default_is_none() -> None:
    edition = _base_edition()
    assert edition.publish_status is None


def test_edition_with_publish_status() -> None:
    edition = _base_edition(publish_status=PublishStatus.published)
    assert edition.publish_status == PublishStatus.published


@pytest.mark.parametrize(
    "slug",
    ["main", "v1", "release-1", "dm-54112"],
)
def test_edition_create_accepts_lowercase_slug(slug: str) -> None:
    edition = EditionCreate(
        slug=slug,
        title="T",
        kind=EditionKind.draft,
        tracking_mode=TrackingMode.git_ref,
    )
    assert edition.slug == slug


@pytest.mark.parametrize(
    "slug",
    ["DM-54112", "DM-54794-relax-edition-slug", "Mixed-Case-1"],
)
def test_edition_create_accepts_uppercase_ticket_slug(slug: str) -> None:
    edition = EditionCreate(
        slug=slug,
        title="T",
        kind=EditionKind.draft,
        tracking_mode=TrackingMode.git_ref,
    )
    assert edition.slug == slug


@pytest.mark.parametrize(
    "slug",
    ["-leading", "trailing-", "with space", "with_underscore", "a"],
)
def test_edition_create_rejects_invalid_slug(slug: str) -> None:
    with pytest.raises(ValidationError):
        EditionCreate(
            slug=slug,
            title="T",
            kind=EditionKind.draft,
            tracking_mode=TrackingMode.git_ref,
        )


def test_uppercase_slug_is_edition_only() -> None:
    """Edition slug relaxation must not leak into project/org slugs."""
    with pytest.raises(ValidationError):
        ProjectCreate(
            slug="DM-54112",
            title="T",
            doc_repo="https://github.com/example/repo",
        )
    with pytest.raises(ValidationError):
        OrganizationCreate(
            slug="LSST",
            title="T",
            base_domain="lsst.io",
        )
