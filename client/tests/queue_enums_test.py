"""Tests for queue_enums client models."""

from __future__ import annotations

from docverse.client.models.queue_enums import JobKind, PublishStatus


def test_publish_status_members() -> None:
    assert PublishStatus.pending == "pending"
    assert PublishStatus.publishing == "publishing"
    assert PublishStatus.published == "published"
    assert PublishStatus.failed == "failed"


def test_job_kind_publish_edition() -> None:
    assert JobKind.publish_edition == "publish_edition"
