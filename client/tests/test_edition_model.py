"""Tests for edition client models."""

from __future__ import annotations

from datetime import UTC, datetime

from docverse.client.models import Edition, EditionKind, TrackingMode
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
