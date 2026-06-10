"""Foundation tests for the Docverse metrics event manager.

These tests do not touch the database: they build the event manager
straight from configuration (resolved to a ``MockEventManager`` by the
``METRICS_MOCK=true`` test environment) and exercise the swallow posture
that production relies on (``raise_on_error=False``).
"""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import pytest
import structlog
from safir.metrics import (
    KafkaEventManager,
    MockEventManager,
    MockEventPublisher,
)
from safir.metrics._event_manager import _State
from structlog.testing import capture_logs

from docverse.config import Configuration
from docverse.metrics import (
    BuildProcessedEvent,
    BuildUploadedEvent,
    DocverseEvents,
    EditionPublishedEvent,
    EditionPublishTrigger,
    MetricsEditionKind,
    build_event_manager,
)


@pytest.mark.asyncio
async def test_build_event_manager_registers_every_publisher() -> None:
    """The mock config builds a MockEventManager with all publishers."""
    config = Configuration()
    manager, events = await build_event_manager(config)

    assert isinstance(manager, MockEventManager)
    assert isinstance(events, DocverseEvents)
    # Every declared publisher is registered and recording.
    assert isinstance(events.build_uploaded, MockEventPublisher)
    assert isinstance(events.build_processed, MockEventPublisher)
    assert isinstance(events.dashboard_built, MockEventPublisher)
    assert isinstance(events.edition_published, MockEventPublisher)
    assert isinstance(events.project_lifecycle, MockEventPublisher)
    assert isinstance(events.edition_lifecycle, MockEventPublisher)
    assert isinstance(events.membership_changed, MockEventPublisher)
    assert isinstance(events.keeper_sync_run_completed, MockEventPublisher)
    assert isinstance(events.lifecycle_action, MockEventPublisher)

    await manager.aclose()


@pytest.mark.asyncio
async def test_publishers_record_payloads() -> None:
    """Publishing a payload through each publisher records it for asserts."""
    config = Configuration()
    manager, events = await build_event_manager(config)

    build_uploaded = events.build_uploaded
    build_processed = events.build_processed
    edition_published = events.edition_published
    assert isinstance(build_uploaded, MockEventPublisher)
    assert isinstance(build_processed, MockEventPublisher)
    assert isinstance(edition_published, MockEventPublisher)

    await build_uploaded.publish(
        BuildUploadedEvent(
            organization="org",
            project="proj",
            uploader="testuser",
            commit_sha=None,
            github_repository=None,
            github_run_id=None,
            github_actor=None,
            ci_platform=None,
        )
    )
    await build_processed.publish(
        BuildProcessedEvent(
            organization="org",
            project="proj",
            success=True,
            object_count=3,
            total_size_bytes=99,
            editions_updated=1,
            editions_skipped=0,
            stale_skipped=False,
            elapsed=timedelta(seconds=1),
        )
    )
    await edition_published.publish(
        EditionPublishedEvent(
            organization="org",
            project="proj",
            edition_kind=MetricsEditionKind.release,
            trigger=EditionPublishTrigger.build,
            elapsed=timedelta(seconds=2),
        )
    )

    build_uploaded.published.assert_published_all(
        [
            {
                "organization": "org",
                "project": "proj",
                "uploader": "testuser",
                "commit_sha": None,
                "github_repository": None,
                "github_run_id": None,
                "github_actor": None,
                "ci_platform": None,
            }
        ]
    )
    assert len(build_processed.published) == 1
    assert len(edition_published.published) == 1

    await manager.aclose()


@pytest.mark.asyncio
async def test_publish_failure_is_swallowed_when_not_raising() -> None:
    """With raise_on_error=False a failing publish is swallowed and logged.

    Production runs the Kafka-backed manager with ``raise_on_error=False``
    so a Kafka/schema-registry outage can never fail a request, build, or
    job. Docverse call sites therefore publish with no defensive
    try/except; this test pins that swallow behaviour at the Safir
    boundary the call sites depend on.
    """
    logger = structlog.get_logger("docverse.test")
    manager = KafkaEventManager(
        application="docverse",
        topic_prefix="lsst.square.metrics.events",
        kafka_broker=Mock(),
        kafka_admin_client=Mock(),
        schema_manager=AsyncMock(),
        raise_on_error=False,
        logger=logger,
    )
    # Drive the manager into a publishable state without a live Kafka.
    manager._state = _State.ready_to_publish

    failing_publisher = Mock()
    failing_publisher.publish = AsyncMock(
        side_effect=RuntimeError("kafka down")
    )
    failing_publisher.topic = "lsst.square.metrics.events.docverse"

    payload = BuildUploadedEvent(
        organization="org",
        project="proj",
        uploader="testuser",
        commit_sha=None,
        github_repository=None,
        github_run_id=None,
        github_actor=None,
        ci_platform=None,
    )

    # The failing publish is swallowed: ``manager.publish`` returns (its
    # abandonable wrapper turns the error into a no-op) instead of raising.
    with capture_logs() as captured:
        await manager.publish(payload, failing_publisher, None)

    assert any(record.get("log_level") == "error" for record in captured)
