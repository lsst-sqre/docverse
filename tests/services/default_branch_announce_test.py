"""Tests for ``announce_main_rewrite``.

The post-commit step every default-branch trigger shares (PRD #721):
a rewritten ``__main`` is announced the way a ``PATCH`` of it would be,
with one ``edition_lifecycle`` ``update`` event and one
``dashboard_build`` for the project. A call whose outcome left
``__main`` alone announces nothing.
"""

from __future__ import annotations

import pytest
import structlog
from safir.arq import MockArqQueue
from safir.metrics import MockEventPublisher
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import OrganizationCreate, ProjectCreate
from docverse.models.queue_enums import JobKind
from docverse_server.config import Configuration
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.factory import Factory
from docverse_server.metrics import (
    DocverseEvents,
    EditionLifecycleEvent,
    LifecycleAction,
    MetricsEditionKind,
)
from docverse_server.services.default_branch import DefaultBranchOutcome
from docverse_server.services.default_branch_announce import (
    announce_main_rewrite,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from tests.support.arq_testing import count_jobs_by_name

_config = Configuration()

_ORG_SLUG = "announce-org"
_PROJECT_SLUG = "announce-proj"

_REWRITTEN = DefaultBranchOutcome(
    column_changed=True, rewritten_from="master", repointed_build_id=7
)
"""An outcome whose ``__main`` moved from ``master`` onto the default."""


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


def _factory(db_session: AsyncSession, arq_queue: MockArqQueue) -> Factory:
    return Factory(
        session=db_session,
        logger=_logger(),
        arq_queue=arq_queue,
        default_queue_name=_config.arq_queue_name,
    )


async def _seed(db_session: AsyncSession) -> None:
    """Seed the org and project the announcement names by slug."""
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    project_store = ProjectStore(session=db_session, logger=logger)
    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug=_ORG_SLUG,
                title="Announce Org",
                base_domain=f"{_ORG_SLUG}.example.com",
            )
        )
        await project_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug=_PROJECT_SLUG,
                title="Announce Project",
                source_url=f"https://example.com/example/{_PROJECT_SLUG}",
            ),
        )
        await db_session.commit()


async def _announce(
    db_session: AsyncSession,
    arq_queue: MockArqQueue,
    *,
    events: DocverseEvents | None,
    outcome: DefaultBranchOutcome = _REWRITTEN,
) -> bool:
    return await announce_main_rewrite(
        factory=_factory(db_session, arq_queue),
        session=db_session,
        events=events,
        logger=_logger(),
        org_slug=_ORG_SLUG,
        project_slug=_PROJECT_SLUG,
        outcome=outcome,
    )


def _lifecycle_events(events: DocverseEvents) -> list[EditionLifecycleEvent]:
    publisher = events.edition_lifecycle
    assert isinstance(publisher, MockEventPublisher)
    return list(publisher.published)


async def _dashboard_rows(db_session: AsyncSession) -> int:
    async with db_session.begin():
        result = await db_session.execute(
            select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.dashboard_build.value
            )
        )
        return len(list(result.scalars().all()))


def _arq_queue() -> MockArqQueue:
    return MockArqQueue(default_queue_name=_config.arq_queue_name)


@pytest.mark.asyncio
async def test_announce_publishes_the_update_and_enqueues_a_dashboard(
    db_session: AsyncSession, mock_events: DocverseEvents
) -> None:
    """A rewrite gets one ``update`` event and one ``dashboard_build``."""
    await _seed(db_session)
    arq_queue = _arq_queue()

    enqueued = await _announce(db_session, arq_queue, events=mock_events)

    assert enqueued is True
    [event] = _lifecycle_events(mock_events)
    assert event.action is LifecycleAction.update
    assert event.edition_kind is MetricsEditionKind.main
    assert event.organization == _ORG_SLUG
    assert event.project == _PROJECT_SLUG
    assert await _dashboard_rows(db_session) == 1
    assert count_jobs_by_name(arq_queue, "dashboard_build") == 1


@pytest.mark.asyncio
async def test_announce_does_nothing_without_a_rewrite(
    db_session: AsyncSession, mock_events: DocverseEvents
) -> None:
    """An outcome that left ``__main`` alone announces nothing.

    Recording the column or retiring nothing is not a ``PATCH`` of
    ``__main``, so neither is announced as one.
    """
    await _seed(db_session)
    arq_queue = _arq_queue()

    enqueued = await _announce(
        db_session,
        arq_queue,
        events=mock_events,
        outcome=DefaultBranchOutcome(column_changed=True),
    )

    assert enqueued is False
    assert _lifecycle_events(mock_events) == []
    assert await _dashboard_rows(db_session) == 0
    assert count_jobs_by_name(arq_queue, "dashboard_build") == 0


@pytest.mark.asyncio
async def test_announce_without_events_still_enqueues_the_dashboard(
    db_session: AsyncSession,
) -> None:
    """A worker without a metrics publisher still rebuilds the dashboard."""
    await _seed(db_session)
    arq_queue = _arq_queue()

    enqueued = await _announce(db_session, arq_queue, events=None)

    assert enqueued is True
    assert await _dashboard_rows(db_session) == 1
    assert count_jobs_by_name(arq_queue, "dashboard_build") == 1


@pytest.mark.asyncio
async def test_announce_reports_a_deduplicated_dashboard_build(
    db_session: AsyncSession, mock_events: DocverseEvents
) -> None:
    """The return value is the enqueue's, not the rewrite's.

    With a ``dashboard_build`` already active for the project, the
    enqueue is skipped and the helper says so, so a caller counting its
    enqueued jobs does not count one it did not add. The event is still
    published: the rewrite happened either way.
    """
    await _seed(db_session)
    arq_queue = _arq_queue()
    assert await _announce(db_session, arq_queue, events=None) is True

    enqueued = await _announce(db_session, arq_queue, events=mock_events)

    assert enqueued is False
    assert len(_lifecycle_events(mock_events)) == 1
    assert await _dashboard_rows(db_session) == 1
    assert count_jobs_by_name(arq_queue, "dashboard_build") == 1
