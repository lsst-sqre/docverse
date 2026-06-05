"""Tests for queue client models, notably the typed progress payload."""

from __future__ import annotations

from datetime import UTC, datetime

from docverse.client.models import (
    BuildProcessingProgress,
    EditionUpdateRef,
    PublishJobRef,
    QueueJob,
)
from docverse.client.models.queue_enums import JobKind, JobStatus


def _build_processing_progress() -> dict[str, object]:
    return {
        "message": "Build processing complete",
        "object_count": 3,
        "total_size_bytes": 1024,
        "editions_updated": [{"slug": "main", "action": "created"}],
        "editions_skipped": [{"slug": "stale"}],
        "publish_jobs": [
            {
                "edition_slug": "main",
                "publish_queue_job_public_id": "0000-0000-0000-05",
            }
        ],
    }


def test_build_processing_progress_typed_fields() -> None:
    """A build payload parses into typed nested models, not raw dicts."""
    progress = BuildProcessingProgress.model_validate(
        _build_processing_progress()
    )

    assert progress.message == "Build processing complete"
    assert progress.object_count == 3  # noqa: PLR2004
    assert progress.total_size_bytes == 1024  # noqa: PLR2004

    assert progress.editions_updated is not None
    assert isinstance(progress.editions_updated[0], EditionUpdateRef)
    assert progress.editions_updated[0].slug == "main"
    assert progress.editions_updated[0].action == "created"

    assert progress.editions_skipped is not None
    assert progress.editions_skipped[0].slug == "stale"

    assert progress.publish_jobs is not None
    assert isinstance(progress.publish_jobs[0], PublishJobRef)
    assert progress.publish_jobs[0].edition_slug == "main"
    assert (
        progress.publish_jobs[0].publish_queue_job_public_id
        == "0000-0000-0000-05"
    )


def test_build_processing_progress_allows_extra_keys() -> None:
    """Unknown keys (e.g. stale_skipped) survive via ``extra='allow'``."""
    progress = BuildProcessingProgress.model_validate(
        {
            "message": "Stale build skipped",
            "stale_skipped": True,
            "latest_build_id": 42,
        }
    )

    dumped = progress.model_dump(exclude_none=True)
    assert dumped["stale_skipped"] is True
    assert dumped["latest_build_id"] == 42  # noqa: PLR2004


def _queue_job(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "self_url": "https://example.com/queue/jobs/0000-0000-0000-05",
        "id": "0000-0000-0000-05",
        "kind": JobKind.build_processing,
        "status": JobStatus.completed,
        "date_created": datetime(2026, 1, 1, tzinfo=UTC),
    }
    base.update(overrides)
    return base


def test_queue_job_progress_is_build_processing_model() -> None:
    """``QueueJob.progress`` validates a build payload into the typed model."""
    job = QueueJob.model_validate(
        _queue_job(progress=_build_processing_progress())
    )

    assert isinstance(job.progress, BuildProcessingProgress)
    assert job.progress.editions_updated is not None
    assert job.progress.editions_updated[0].slug == "main"


def test_queue_job_progress_preserves_non_build_payload() -> None:
    """A non-build kind's progress round-trips unchanged via ``extra``."""
    job = QueueJob.model_validate(
        _queue_job(
            kind=JobKind.keeper_sync_run_discovery,
            progress={
                "message": "Discovery complete",
                "in_scope_count": 5,
                "enqueued_count": 4,
            },
        )
    )

    assert job.progress is not None
    dumped = job.progress.model_dump(exclude_none=True)
    assert dumped["in_scope_count"] == 5  # noqa: PLR2004
    assert dumped["enqueued_count"] == 4  # noqa: PLR2004


def test_queue_job_progress_none() -> None:
    """A job with no progress yields ``None``."""
    job = QueueJob.model_validate(_queue_job())
    assert job.progress is None
