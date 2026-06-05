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


def test_edition_update_ref_has_typed_edition_url() -> None:
    """``edition_url`` is a declared field, not merely an ``extra`` key."""
    url = "https://docverse.example/api/orgs/o/projects/p/editions/main"
    ref = EditionUpdateRef(slug="main", action="created", edition_url=url)

    assert ref.edition_url == url
    assert "edition_url" in EditionUpdateRef.model_fields
    assert ref.model_dump(exclude_none=True)["edition_url"] == url


def test_publish_job_ref_has_typed_queue_job_url() -> None:
    """``queue_job_url`` is a declared field, not merely an ``extra`` key."""
    url = "https://docverse.example/api/queue/jobs/0000-0000-0000-05"
    ref = PublishJobRef(
        edition_slug="main",
        publish_queue_job_public_id="0000-0000-0000-05",
        queue_job_url=url,
    )

    assert ref.queue_job_url == url
    assert "queue_job_url" in PublishJobRef.model_fields
    assert ref.model_dump(exclude_none=True)["queue_job_url"] == url


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


def test_queue_job_subject_back_reference_fields_default_none() -> None:
    """The subject back-reference URL fields are declared and optional."""
    job = QueueJob.model_validate(_queue_job())

    assert job.build_url is None
    assert job.edition_url is None
    assert job.subject_url is None
    for name in ("build_url", "edition_url", "subject_url"):
        assert name in QueueJob.model_fields


def test_queue_job_subject_back_reference_fields_round_trip() -> None:
    """The back-reference URLs survive a validate round-trip."""
    build = "https://example.com/orgs/o/projects/p/builds/0000-0000-0000-05"
    edition = "https://example.com/orgs/o/projects/p/editions/main"
    job = QueueJob.model_validate(
        _queue_job(build_url=build, edition_url=edition, subject_url=edition)
    )

    assert job.build_url == build
    assert job.edition_url == edition
    assert job.subject_url == edition
