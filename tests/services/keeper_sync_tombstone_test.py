"""Tests for :class:`KeeperSyncTombstoneService`."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.client.models import (
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.exceptions import NotFoundError
from docverse.services.keeper_sync_tombstone import KeeperSyncTombstoneService
from docverse.storage.edition_store import EditionStore
from docverse.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore


async def _seed_org(session: AsyncSession, *, slug: str = "ks-tomb") -> int:
    logger = structlog.get_logger("test")
    store = OrganizationStore(session=session, logger=logger)
    org = await store.create(
        OrganizationCreate(
            slug=slug,
            title="ks-tomb",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id


def _build_service(
    session: AsyncSession,
    *,
    logger: structlog.stdlib.BoundLogger | None = None,
) -> KeeperSyncTombstoneService:
    log = logger or structlog.get_logger("test")
    state_store = KeeperSyncStateStore(session=session, logger=log)
    return KeeperSyncTombstoneService(
        session=session, state_store=state_store, logger=log
    )


@pytest.mark.asyncio
async def test_record_writes_tombstone_on_existing_edition_row(
    db_session: AsyncSession,
) -> None:
    """Existing state row picks up tombstone fields on ``record``."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session)
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=42,
            ltd_slug="main",
            docverse_id=7,
        )

    service = KeeperSyncTombstoneService(
        session=db_session, state_store=state_store, logger=logger
    )
    async with db_session.begin():
        state = await service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=42,
            reason=TombstoneReason.lifecycle_delete,
            note="aged-out draft",
        )

    assert state.date_tombstoned is not None
    assert state.tombstone_reason == "lifecycle_delete"
    assert state.tombstone_note == "aged-out draft"
    assert state.docverse_id == 7


@pytest.mark.asyncio
async def test_record_creates_row_when_none_exists_for_preemptive(
    db_session: AsyncSession,
) -> None:
    """``lifecycle_preemptive`` writes a new row with ``docverse_id=NULL``."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-preemptive")

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        state = await service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=99,
            reason=TombstoneReason.lifecycle_preemptive,
        )

    assert state.id > 0
    assert state.docverse_id is None
    assert state.date_tombstoned is not None
    assert state.tombstone_reason == "lifecycle_preemptive"
    assert state.tombstone_note is None
    assert state.ltd_id == 99


@pytest.mark.asyncio
async def test_record_creates_project_row_with_slug_key(
    db_session: AsyncSession,
) -> None:
    """Project rows are slug-keyed; ``record`` honours the slug variant."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-project")

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        state = await service.record(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
            reason=TombstoneReason.manual_delete,
        )

    assert state.ltd_slug == "pipelines"
    assert state.tombstone_reason == "manual_delete"
    assert state.date_tombstoned is not None


@pytest.mark.asyncio
async def test_is_tombstoned_returns_true_only_when_stamped(
    db_session: AsyncSession,
) -> None:
    """``is_tombstoned`` is true iff ``date_tombstoned IS NOT NULL``."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-check")
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
        )

    service = KeeperSyncTombstoneService(
        session=db_session, state_store=state_store, logger=logger
    )
    async with db_session.begin():
        assert (
            await service.is_tombstoned(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=1,
            )
            is False
        )
        await service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            reason=TombstoneReason.manual_delete,
        )
        assert (
            await service.is_tombstoned(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=1,
            )
            is True
        )


@pytest.mark.asyncio
async def test_is_tombstoned_false_for_missing_row(
    db_session: AsyncSession,
) -> None:
    """Absence of a state row is "not tombstoned", not an error."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-missing")

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        assert (
            await service.is_tombstoned(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=12345,
            )
            is False
        )


@pytest.mark.asyncio
async def test_record_emits_structured_log(
    db_session: AsyncSession,
) -> None:
    """``record`` emits a log line with org, resource_type, ltd_id, reason."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-log")

    service = _build_service(db_session, logger=logger)
    with capture_logs() as captured:
        async with db_session.begin():
            await service.record(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=42,
                reason=TombstoneReason.lifecycle_preemptive,
            )

    events = [
        e for e in captured if e.get("event") == "Sync tombstone recorded"
    ]
    assert events, "expected a Sync tombstone recorded log line"
    event = events[-1]
    assert event["org_id"] == org_id
    assert event["resource_type"] == "edition"
    assert event["ltd_id"] == 42
    assert event["reason"] == "lifecycle_preemptive"


# ---------------------------------------------------------------------------
# list_for_org
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_for_org_returns_only_tombstoned_rows(
    db_session: AsyncSession,
) -> None:
    """Untombstoned rows do not appear in the listing."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-list-only-tomb")
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="alive",
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            ltd_slug="dead",
        )

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        await service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            reason=TombstoneReason.manual_delete,
        )
    async with db_session.begin():
        result = await service.list_for_org(
            org_id=org_id, cursor=None, limit=25
        )

    ltd_ids = sorted(
        entry.ltd_id
        for entry in result.page.entries
        if entry.ltd_id is not None
    )
    assert ltd_ids == [2]


@pytest.mark.asyncio
async def test_list_for_org_filters_by_resource_type(
    db_session: AsyncSession,
) -> None:
    """``resource_type=edition`` excludes tombstoned project rows."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-list-restype")

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        await service.record(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="proj-a",
            reason=TombstoneReason.manual_delete,
        )
        await service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=7,
            reason=TombstoneReason.lifecycle_delete,
        )

    async with db_session.begin():
        editions_only = await service.list_for_org(
            org_id=org_id,
            cursor=None,
            limit=25,
            resource_type=ResourceType.edition,
        )
        projects_only = await service.list_for_org(
            org_id=org_id,
            cursor=None,
            limit=25,
            resource_type=ResourceType.project,
        )

    assert [e.resource_type for e in editions_only.page.entries] == ["edition"]
    assert [e.resource_type for e in projects_only.page.entries] == ["project"]


@pytest.mark.asyncio
async def test_list_for_org_filters_by_reason(
    db_session: AsyncSession,
) -> None:
    """``tombstone_reason=lifecycle_delete`` excludes other reasons."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-list-reason")

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        await service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            reason=TombstoneReason.manual_delete,
        )
        await service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            reason=TombstoneReason.lifecycle_delete,
        )

    async with db_session.begin():
        lifecycle_only = await service.list_for_org(
            org_id=org_id,
            cursor=None,
            limit=25,
            tombstone_reason=TombstoneReason.lifecycle_delete,
        )
    ltd_ids = [entry.ltd_id for entry in lifecycle_only.page.entries]
    assert ltd_ids == [2]


@pytest.mark.asyncio
async def test_list_for_org_scopes_to_org(
    db_session: AsyncSession,
) -> None:
    """Tombstones on one org are invisible to another org's listing."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_a = await _seed_org(db_session, slug="ks-list-scope-a")
        org_b = await _seed_org(db_session, slug="ks-list-scope-b")

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        await service.record(
            org_id=org_a,
            resource_type=ResourceType.edition,
            ltd_id=99,
            reason=TombstoneReason.manual_delete,
        )

    async with db_session.begin():
        result = await service.list_for_org(
            org_id=org_b, cursor=None, limit=25
        )
    assert result.page.entries == []
    assert result.page.count == 0


@pytest.mark.asyncio
async def test_list_for_org_includes_display_path_for_edition(
    db_session: AsyncSession,
) -> None:
    """Edition rows derive ``<project_slug>/<edition_slug>``."""
    logger = structlog.get_logger("test")
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)

    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="ks-disp-ed",
                title="ks-disp-ed",
                base_domain="ks-disp-ed.example.com",
            )
        )
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="my-proj",
                title="My Proj",
                source_url="https://example.com/x/y",
            ),
        )
        edition = await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug="v1",
                title="Version 1",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        await state_store.upsert(
            org_id=org.id,
            resource_type=ResourceType.edition,
            ltd_id=4242,
            ltd_slug="v1",
            docverse_id=edition.id,
        )

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        await service.record(
            org_id=org.id,
            resource_type=ResourceType.edition,
            ltd_id=4242,
            reason=TombstoneReason.lifecycle_delete,
        )

    async with db_session.begin():
        result = await service.list_for_org(
            org_id=org.id, cursor=None, limit=25
        )

    assert len(result.page.entries) == 1
    entry = result.page.entries[0]
    assert result.display_path_by_state_id[entry.id] == "my-proj/v1"


@pytest.mark.asyncio
async def test_list_for_org_display_path_falls_back_for_preemptive(
    db_session: AsyncSession,
) -> None:
    """Preemptive rows have no Docverse row; fall back to ltd_slug."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-disp-preempt")

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        await service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=900,
            ltd_slug="never-imported",
            reason=TombstoneReason.lifecycle_preemptive,
        )

    async with db_session.begin():
        result = await service.list_for_org(
            org_id=org_id, cursor=None, limit=25
        )

    entry = result.page.entries[0]
    assert result.display_path_by_state_id[entry.id] == "never-imported"


# ---------------------------------------------------------------------------
# clear (with revive-on-clear)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clear_un_tombstones_state_row(
    db_session: AsyncSession,
) -> None:
    """``clear`` nulls ``date_tombstoned`` / reason / note on the state row."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-clear-basic")

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        recorded = await service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=51,
            reason=TombstoneReason.manual_delete,
            note="recover me",
        )

    async with db_session.begin():
        cleared = await service.clear(state_id=recorded.id, org_id=org_id)

    assert cleared.state.date_tombstoned is None
    assert cleared.state.tombstone_reason is None
    assert cleared.state.tombstone_note is None
    assert cleared.revived_docverse_row is False  # no docverse row linked


@pytest.mark.asyncio
async def test_clear_revives_soft_deleted_edition(
    db_session: AsyncSession,
) -> None:
    """A soft-deleted edition is revived in the same transaction."""
    logger = structlog.get_logger("test")
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)

    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="ks-clear-revive",
                title="ks-clear-revive",
                base_domain="ks-clear-revive.example.com",
            )
        )
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="proj-r",
                title="Proj R",
                source_url="https://example.com/r/r",
            ),
        )
        edition = await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug="ed-r",
                title="Ed R",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        await state_store.upsert(
            org_id=org.id,
            resource_type=ResourceType.edition,
            ltd_id=100,
            ltd_slug="ed-r",
            docverse_id=edition.id,
        )
        # Soft-delete via the centralized chokepoint → stamps the tombstone.
        await edition_store.soft_delete(
            org_id=org.id,
            project_id=project.id,
            slug="ed-r",
            reason=TombstoneReason.manual_delete,
        )

    # Find the tombstoned state row.
    async with db_session.begin():
        state = await state_store.get(
            org_id=org.id,
            resource_type=ResourceType.edition,
            ltd_id=100,
            include_tombstoned=True,
        )
    assert state is not None
    assert state.date_tombstoned is not None

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        cleared = await service.clear(state_id=state.id, org_id=org.id)

    assert cleared.revived_docverse_row is True
    # Edition is now visible via the default (date_deleted IS NULL) read.
    async with db_session.begin():
        revived = await edition_store.get_by_slug(
            project_id=project.id, slug="ed-r"
        )
    assert revived is not None
    assert revived.id == edition.id


@pytest.mark.asyncio
async def test_clear_revives_soft_deleted_project(
    db_session: AsyncSession,
) -> None:
    """A soft-deleted project is revived in the same transaction."""
    logger = structlog.get_logger("test")
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)

    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="ks-clear-proj",
                title="ks-clear-proj",
                base_domain="ks-clear-proj.example.com",
            )
        )
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="kill-me",
                title="Kill Me",
                source_url="https://example.com/k/m",
            ),
        )
        await state_store.upsert(
            org_id=org.id,
            resource_type=ResourceType.project,
            ltd_slug="kill-me",
            docverse_id=project.id,
        )
        await proj_store.soft_delete(
            org_id=org.id,
            slug="kill-me",
            reason=TombstoneReason.manual_delete,
        )

    async with db_session.begin():
        state = await state_store.get(
            org_id=org.id,
            resource_type=ResourceType.project,
            ltd_slug="kill-me",
            include_tombstoned=True,
        )
    assert state is not None
    assert state.date_tombstoned is not None

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        cleared = await service.clear(state_id=state.id, org_id=org.id)

    assert cleared.revived_docverse_row is True
    async with db_session.begin():
        revived = await proj_store.get_by_slug(org_id=org.id, slug="kill-me")
    assert revived is not None
    assert revived.id == project.id


@pytest.mark.asyncio
async def test_clear_raises_when_state_not_found(
    db_session: AsyncSession,
) -> None:
    """A non-existent state_id raises NotFoundError."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-clear-miss")

    service = _build_service(db_session, logger=logger)
    with pytest.raises(NotFoundError):
        async with db_session.begin():
            await service.clear(state_id=999_999, org_id=org_id)


@pytest.mark.asyncio
async def test_clear_raises_when_row_not_tombstoned(
    db_session: AsyncSession,
) -> None:
    """An untombstoned row is treated as "no such tombstone" → 404."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-clear-no-tomb")
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        state = await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=12,
            ltd_slug="alive",
        )

    service = _build_service(db_session, logger=logger)
    with pytest.raises(NotFoundError):
        async with db_session.begin():
            await service.clear(state_id=state.id, org_id=org_id)


@pytest.mark.asyncio
async def test_clear_is_org_scoped(
    db_session: AsyncSession,
) -> None:
    """A state_id from another org raises NotFoundError, not silent clear."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_a = await _seed_org(db_session, slug="ks-clear-org-a")
        org_b = await _seed_org(db_session, slug="ks-clear-org-b")

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        recorded = await service.record(
            org_id=org_a,
            resource_type=ResourceType.edition,
            ltd_id=77,
            reason=TombstoneReason.manual_delete,
        )

    with pytest.raises(NotFoundError):
        async with db_session.begin():
            await service.clear(state_id=recorded.id, org_id=org_b)


@pytest.mark.asyncio
async def test_clear_emits_structured_log(
    db_session: AsyncSession,
) -> None:
    """``clear`` emits an audit log with org / resource_type / ltd_id."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-clear-log")

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        recorded = await service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=24,
            reason=TombstoneReason.lifecycle_delete,
        )

    with capture_logs() as captured:
        async with db_session.begin():
            await service.clear(state_id=recorded.id, org_id=org_id)

    events = [
        e for e in captured if e.get("event") == "Sync tombstone cleared"
    ]
    assert events, "expected a Sync tombstone cleared log line"
    event = events[-1]
    assert event["org_id"] == org_id
    assert event["state_id"] == recorded.id
    assert event["resource_type"] == "edition"
    assert event["ltd_id"] == 24
    assert event["previous_reason"] == "lifecycle_delete"
    assert event["revived_docverse_row"] is False
