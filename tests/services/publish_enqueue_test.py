"""Tests for the shared :func:`enqueue_publish_for_edition` helper.

The helper is exercised end-to-end through worker tests
(``tests/worker/build_processing_test.py`` and
``tests/worker/keeper_sync_project_test.py``); the unit tests here
focus on the two history-row paths the helper has to cover:

* the **normal** path used after edition tracking, where the
  ``EditionBuildHistory`` row already exists and the helper just sets
  it ``pending``;
* the **sync** path used after a keeper-sync build finalize, where no
  history row has been recorded yet and the helper must record one
  before flipping it ``pending``.
"""

from __future__ import annotations

import pytest
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.client.models.queue_enums import JobKind, PublishStatus
from docverse.config import Configuration
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.domain.base32id import (
    generate_base32_id,
    serialize_base32_id,
    validate_base32_id,
)
from docverse.domain.queue import JobStatus
from docverse.services.publish_enqueue import enqueue_publish_for_edition
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_backend import ArqQueueBackend
from docverse.storage.queue_job_store import QueueJobStore

_HASH = "sha256:" + "a" * 64
_config = Configuration()


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org_project_edition_build(
    db_session: AsyncSession,
) -> tuple[int, int, str, int, str, int, str]:
    """Insert an org/project/edition/build matrix the helper needs."""
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    project_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)

    org = await org_store.create(
        OrganizationCreate(
            slug="pe-org",
            title="PE Org",
            base_domain="pe-org.example.com",
        )
    )
    project = await project_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug="pe-proj",
            title="PE Project",
            source_url="https://example.com/example/pe",
        ),
    )
    edition = await edition_store.create(
        project_id=project.id,
        data=EditionCreate(
            slug="main",
            title="Main",
            kind=EditionKind.draft,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "main"},
        ),
    )
    build = await build_store.create(
        project_id=project.id,
        project_slug=project.slug,
        data=BuildCreate(git_ref="main", content_hash=_HASH),
        uploader="testuser",
    )
    await build_store.transition_status(
        build_id=build.id, new_status=BuildStatus.processing
    )
    await build_store.transition_status(
        build_id=build.id, new_status=BuildStatus.completed
    )
    return (
        org.id,
        project.id,
        project.slug,
        edition.id,
        edition.slug,
        build.id,
        serialize_base32_id(build.public_id),
    )


@pytest.mark.asyncio
async def test_enqueue_publish_for_edition_records_history_when_missing(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Sync path: helper records ``EditionBuildHistory`` row when absent.

    The keeper-sync flow finalizes the build directly via the build
    store, skipping ``EditionTrackingService.track_build`` (the path
    that records the history row in the normal client-upload flow).
    The helper must therefore detect the missing row and record one
    before setting it ``pending``.
    """
    async with db_session.begin():
        (
            org_id,
            project_id,
            project_slug,
            edition_id,
            edition_slug,
            build_id,
            build_public_id,
        ) = await _seed_org_project_edition_build(db_session)
        run_row = SqlKeeperSyncRun(
            public_id=validate_base32_id(generate_base32_id()),
            org_id=org_id,
            kind="backfill",
            status="in_progress",
        )
        db_session.add(run_row)
        await db_session.flush()
        run_id = run_row.id

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    queue_backend = ArqQueueBackend(
        arq_queue=mock_arq,
        default_queue_name=_config.arq_queue_name,
    )

    async for session in db_session_dependency():
        edition_store = EditionStore(session=session, logger=_logger())
        history_store = EditionBuildHistoryStore(
            session=session, logger=_logger()
        )
        queue_job_store = QueueJobStore(session=session, logger=_logger())

        result = await enqueue_publish_for_edition(
            session=session,
            edition_store=edition_store,
            history_store=history_store,
            queue_job_store=queue_job_store,
            queue_backend=queue_backend,
            org_id=org_id,
            project_id=project_id,
            project_slug=project_slug,
            edition_id=edition_id,
            edition_slug=edition_slug,
            build_id=build_id,
            build_public_id=build_public_id,
            keeper_sync_run_id=run_id,
        )

        async with session.begin():
            edition = await edition_store.get_by_slug(
                project_id=project_id, slug=edition_slug
            )
            assert edition is not None
            assert edition.publish_status == PublishStatus.pending

            history = await history_store.get_by_edition_and_build(
                edition_id=edition_id, build_id=build_id
            )
            assert history is not None
            assert history.publish_status == PublishStatus.pending

            child_qj = await queue_job_store.get(result.queue_job_id)
            assert child_qj is not None
            assert child_qj.kind == JobKind.publish_edition
            assert child_qj.status == JobStatus.queued
            assert child_qj.keeper_sync_run_id == run_id
            assert child_qj.backend_job_id == result.backend_job_id
            assert child_qj.edition_id == edition_id
            assert child_qj.build_id == build_id
            assert child_qj.org_id == org_id
            assert child_qj.project_id == project_id

    # The arq job was enqueued on the regular queue (not the sync queue).
    queues = mock_arq._job_metadata
    assert len(queues.get(_config.arq_queue_name, {})) == 1


@pytest.mark.asyncio
async def test_enqueue_publish_for_edition_reuses_existing_history(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Normal path: helper reuses an existing ``EditionBuildHistory`` row.

    The build_processing flow's ``EditionTrackingService.track_build``
    records the history row before the publish helper runs. The helper
    must NOT create a duplicate row; it must look up the existing one
    and flip it ``pending``.
    """
    async with db_session.begin():
        (
            org_id,
            project_id,
            project_slug,
            edition_id,
            edition_slug,
            build_id,
            build_public_id,
        ) = await _seed_org_project_edition_build(db_session)

        history_store = EditionBuildHistoryStore(
            session=db_session, logger=_logger()
        )
        pre_existing = await history_store.record(
            edition_id=edition_id, build_id=build_id
        )
        pre_existing_id = pre_existing.id

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    queue_backend = ArqQueueBackend(
        arq_queue=mock_arq,
        default_queue_name=_config.arq_queue_name,
    )

    async for session in db_session_dependency():
        edition_store = EditionStore(session=session, logger=_logger())
        history_store = EditionBuildHistoryStore(
            session=session, logger=_logger()
        )
        queue_job_store = QueueJobStore(session=session, logger=_logger())

        await enqueue_publish_for_edition(
            session=session,
            edition_store=edition_store,
            history_store=history_store,
            queue_job_store=queue_job_store,
            queue_backend=queue_backend,
            org_id=org_id,
            project_id=project_id,
            project_slug=project_slug,
            edition_id=edition_id,
            edition_slug=edition_slug,
            build_id=build_id,
            build_public_id=build_public_id,
        )

        async with session.begin():
            history = await history_store.get_by_edition_and_build(
                edition_id=edition_id, build_id=build_id
            )
            assert history is not None
            # Same row — the helper must not have inserted a duplicate.
            assert history.id == pre_existing_id
            assert history.publish_status == PublishStatus.pending

            entries = await history_store.list_by_edition(edition_id)
            assert len(entries) == 1
