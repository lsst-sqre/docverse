"""Tests for queue_enums client models."""

from __future__ import annotations

from docverse.models.queue_enums import JobKind, PublishStatus


def test_publish_status_members() -> None:
    assert PublishStatus.pending == "pending"
    assert PublishStatus.publishing == "publishing"
    assert PublishStatus.published == "published"
    assert PublishStatus.failed == "failed"


def test_job_kind_publish_edition() -> None:
    assert JobKind.publish_edition == "publish_edition"


def test_job_kind_edition_reconcile() -> None:
    """The reconciliation loop's per-org job kind (PRD #612)."""
    assert JobKind.edition_reconcile == "edition_reconcile"
