"""Tests for the keeper-sync push processor (PRD #803).

The processor turns a GitHub ``push`` delivery into a push hint on the
``keeper_sync_state`` row of every LTD-synced project bound to the
pushed repository. These tests run it against the database directly,
inside the transaction the webhook handler would own; the handler's own
tests post signed deliveries end to end.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import structlog
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import KeeperSyncConfig, OrganizationCreate, ProjectCreate
from docverse.models.projects import ProjectGitHubBindingCreate
from docverse_server.dbschema.keeper_sync_state import SqlKeeperSyncState
from docverse_server.services.keeper_sync.push_hints import (
    ANNOTATION_GITHUB_PUSHED_REFS,
)
from docverse_server.services.keeper_sync_push_processor import (
    KeeperSyncPushProcessor,
    StampedProject,
)
from docverse_server.storage.keeper_sync import (
    KeeperSyncState,
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore

_NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
_WINDOW = timedelta(hours=1)


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


def _make_processor(
    session: AsyncSession,
    *,
    enabled: bool = True,
) -> KeeperSyncPushProcessor:
    return KeeperSyncPushProcessor(
        project_store=ProjectStore(session=session, logger=_logger()),
        org_store=OrganizationStore(session=session, logger=_logger()),
        state_store=KeeperSyncStateStore(session=session, logger=_logger()),
        logger=_logger(),
        enabled=enabled,
    )


async def _seed_org(
    session: AsyncSession,
    slug: str,
    *,
    sync: KeeperSyncConfig | None = None,
) -> int:
    """Create an org, with keeper-sync enabled for every slug by default."""
    store = OrganizationStore(session=session, logger=_logger())
    org = await store.create(
        OrganizationCreate(
            slug=slug,
            title=f"Org {slug}",
            base_domain=f"{slug}.example.com",
        )
    )
    await store.update_keeper_sync_config(
        slug,
        sync or KeeperSyncConfig(enabled=True, project_slugs="*"),
    )
    return org.id


async def _seed_synced_project(
    session: AsyncSession,
    *,
    org_id: int,
    slug: str,
    github_owner: str = "lsst-sqre",
    github_repo: str = "sqr-112",
    repo_id: int | None = 4242,
    state: bool = True,
    annotations: dict[str, Any] | None = None,
) -> int:
    """Create a GitHub-bound project and, by default, its sync state row."""
    store = ProjectStore(session=session, logger=_logger())
    project = await store.create(
        org_id=org_id,
        data=ProjectCreate(
            slug=slug,
            title=f"Project {slug}",
            github=ProjectGitHubBindingCreate(
                owner=github_owner, repo=github_repo
            ),
        ),
        github_owner=github_owner,
        github_repo=github_repo,
    )
    if repo_id is not None:
        await store.apply_installation_scope(
            installation_id=99,
            owner=github_owner,
            owner_id=999,
            repo=github_repo,
            repo_id=repo_id,
        )
    if state:
        await KeeperSyncStateStore(session=session, logger=_logger()).upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug=slug,
            docverse_id=project.id,
            annotations=annotations,
        )
    return project.id


async def _project_state(
    session: AsyncSession, *, org_id: int, ltd_slug: str
) -> KeeperSyncState | None:
    store = KeeperSyncStateStore(session=session, logger=_logger())
    return await store.get(
        org_id=org_id, resource_type=ResourceType.project, ltd_slug=ltd_slug
    )


def _pushed_refs(state: KeeperSyncState | None) -> Any:
    assert state is not None
    assert state.annotations is not None
    return state.annotations.get(ANNOTATION_GITHUB_PUSHED_REFS)


def _push_payload(
    *,
    owner: str = "lsst-sqre",
    repo: str = "sqr-112",
    repo_id: int | None = 4242,
    ref: str = "refs/heads/tickets/DM-1",
    deleted: bool = False,
) -> dict[str, Any]:
    repository: dict[str, Any] = {
        "name": repo,
        "full_name": f"{owner}/{repo}",
        "owner": {"login": owner, "name": owner},
    }
    if repo_id is not None:
        repository["id"] = repo_id
    return {
        "ref": ref,
        "before": "before-sha",
        "after": "after-sha",
        "deleted": deleted,
        "repository": repository,
        "commits": [],
        "size": 0,
    }


@pytest.mark.asyncio
async def test_branch_push_stamps_the_synced_project(
    db_session: AsyncSession,
) -> None:
    """A branch push stamps its normalized ref with the push time."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-branch")
        await _seed_synced_project(db_session, org_id=org_id, slug="sqr-112")
        await db_session.commit()

    async with db_session.begin():
        stamped = await _make_processor(db_session).process(
            _push_payload(), now=_NOW
        )
        await db_session.commit()

    assert stamped == [
        StampedProject(
            org_slug="ks-push-branch",
            project_slug="sqr-112",
            github_ref="tickets/DM-1",
        )
    ]
    async with db_session.begin():
        state = await _project_state(
            db_session, org_id=org_id, ltd_slug="sqr-112"
        )
    assert _pushed_refs(state) == {"tickets/DM-1": _NOW.isoformat()}


@pytest.mark.asyncio
async def test_repeat_push_overwrites_the_stamp(
    db_session: AsyncSession,
) -> None:
    """A second push to the same ref moves its push time forward."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-repeat")
        await _seed_synced_project(db_session, org_id=org_id, slug="sqr-112")
        await db_session.commit()

    later = _NOW + timedelta(minutes=10)
    for now in (_NOW, later):
        async with db_session.begin():
            await _make_processor(db_session).process(_push_payload(), now=now)
            await db_session.commit()

    async with db_session.begin():
        state = await _project_state(
            db_session, org_id=org_id, ltd_slug="sqr-112"
        )
    assert _pushed_refs(state) == {"tickets/DM-1": later.isoformat()}


@pytest.mark.asyncio
async def test_tag_push_stamps_the_bare_tag(db_session: AsyncSession) -> None:
    """A tag push stamps the tag's name without ``refs/tags/``."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-tag")
        await _seed_synced_project(db_session, org_id=org_id, slug="sqr-112")
        await db_session.commit()

    async with db_session.begin():
        stamped = await _make_processor(db_session).process(
            _push_payload(ref="refs/tags/v1.0"), now=_NOW
        )
        await db_session.commit()

    assert [project.github_ref for project in stamped] == ["v1.0"]
    async with db_session.begin():
        state = await _project_state(
            db_session, org_id=org_id, ltd_slug="sqr-112"
        )
    assert _pushed_refs(state) == {"v1.0": _NOW.isoformat()}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(
            _push_payload(deleted=True),
            id="deleted",
        ),
        pytest.param(
            _push_payload(ref="refs/pull/7/merge"),
            id="pull-request-ref",
        ),
        pytest.param(
            _push_payload(ref="refs/heads/"),
            id="empty-branch-name",
        ),
        pytest.param(
            {**_push_payload(), "ref": None},
            id="no-ref",
        ),
        pytest.param(
            {**_push_payload(), "repository": None},
            id="no-repository",
        ),
        pytest.param(
            _push_payload(owner="lsst-sqre", repo="unbound", repo_id=1),
            id="repository-with-no-project",
        ),
    ],
)
async def test_ignored_pushes_stamp_nothing(
    db_session: AsyncSession,
    payload: dict[str, Any],
) -> None:
    """Pushes keeper-sync has no business with leave the row untouched."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-ignored")
        await _seed_synced_project(db_session, org_id=org_id, slug="sqr-112")
        await db_session.commit()

    async with db_session.begin():
        stamped = await _make_processor(db_session).process(payload, now=_NOW)
        await db_session.commit()

    assert stamped == []
    async with db_session.begin():
        state = await _project_state(
            db_session, org_id=org_id, ltd_slug="sqr-112"
        )
    assert state is not None
    assert state.annotations is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sync",
    [
        pytest.param(
            KeeperSyncConfig(enabled=False, project_slugs="*"),
            id="sync-disabled",
        ),
        pytest.param(
            KeeperSyncConfig(enabled=True, project_slugs=["dmtn-001"]),
            id="not-included",
        ),
        pytest.param(
            KeeperSyncConfig(
                enabled=True,
                project_slugs="*",
                exclude_project_slugs=["sqr-112"],
            ),
            id="excluded",
        ),
    ],
)
async def test_ineligible_org_or_scope_stamps_nothing(
    db_session: AsyncSession,
    sync: KeeperSyncConfig,
) -> None:
    """A project whose org syncs nothing, or not it, is not stamped."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-scope", sync=sync)
        await _seed_synced_project(db_session, org_id=org_id, slug="sqr-112")
        await db_session.commit()

    async with db_session.begin():
        stamped = await _make_processor(db_session).process(
            _push_payload(), now=_NOW
        )
        await db_session.commit()

    assert stamped == []
    async with db_session.begin():
        state = await _project_state(
            db_session, org_id=org_id, ltd_slug="sqr-112"
        )
    assert state is not None
    assert state.annotations is None


@pytest.mark.asyncio
async def test_project_without_state_row_is_not_stamped(
    db_session: AsyncSession,
) -> None:
    """A bound project keeper-sync never imported gets no state row."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-nostate")
        await _seed_synced_project(
            db_session, org_id=org_id, slug="sqr-112", state=False
        )
        await db_session.commit()

    async with db_session.begin():
        stamped = await _make_processor(db_session).process(
            _push_payload(), now=_NOW
        )
        await db_session.commit()

    assert stamped == []
    async with db_session.begin():
        assert (
            await _project_state(db_session, org_id=org_id, ltd_slug="sqr-112")
            is None
        )


@pytest.mark.asyncio
async def test_disabled_hot_path_stamps_nothing(
    db_session: AsyncSession,
) -> None:
    """With the hot path switched off, an eligible push stamps nothing."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-off")
        await _seed_synced_project(db_session, org_id=org_id, slug="sqr-112")
        await db_session.commit()

    async with db_session.begin():
        stamped = await _make_processor(db_session, enabled=False).process(
            _push_payload(), now=_NOW
        )
        await db_session.commit()

    assert stamped == []
    async with db_session.begin():
        state = await _project_state(
            db_session, org_id=org_id, ltd_slug="sqr-112"
        )
    assert state is not None
    assert state.annotations is None


@pytest.mark.asyncio
async def test_repository_bound_to_several_projects_stamps_all(
    db_session: AsyncSession,
) -> None:
    """Every synced project backed by the repository is stamped.

    One is matched by ``repository.id``, the other by the owner/name
    fallback for a project whose numeric id is still unresolved, and
    they sit in different organizations.
    """
    async with db_session.begin():
        org_a = await _seed_org(db_session, "ks-push-multi-a")
        org_b = await _seed_org(db_session, "ks-push-multi-b")
        await _seed_synced_project(db_session, org_id=org_a, slug="sqr-112")
        await _seed_synced_project(
            db_session, org_id=org_b, slug="sqr-112-copy", repo_id=None
        )
        await db_session.commit()

    async with db_session.begin():
        stamped = await _make_processor(db_session).process(
            _push_payload(), now=_NOW
        )
        await db_session.commit()

    assert {(p.org_slug, p.project_slug) for p in stamped} == {
        ("ks-push-multi-a", "sqr-112"),
        ("ks-push-multi-b", "sqr-112-copy"),
    }
    async with db_session.begin():
        for org_id, slug in ((org_a, "sqr-112"), (org_b, "sqr-112-copy")):
            state = await _project_state(
                db_session, org_id=org_id, ltd_slug=slug
            )
            assert _pushed_refs(state) == {"tickets/DM-1": _NOW.isoformat()}


@pytest.mark.asyncio
async def test_stamp_merges_into_existing_annotations(
    db_session: AsyncSession,
) -> None:
    """Other annotation keys survive, and the cap drops the oldest first.

    The row already carries the tier crons' keys, twenty live stamps,
    and one stamp older than the window. The new push leaves twenty-two
    refs, so the cap drops the two oldest pushes: the expired stamp,
    which ``tier_main`` would otherwise have pruned, and then the oldest
    live one, so the map stays at twenty.
    """
    live = {
        f"branch-{index:02d}": (
            _NOW - timedelta(minutes=50 - index)
        ).isoformat()
        for index in range(20)
    }
    annotations = {
        "date_main_last_polled": "2026-10-09T11:55:00+00:00",
        ANNOTATION_GITHUB_PUSHED_REFS: {
            **live,
            "expired": (_NOW - _WINDOW).isoformat(),
        },
    }
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-merge")
        await _seed_synced_project(
            db_session,
            org_id=org_id,
            slug="sqr-112",
            annotations=annotations,
        )
        await db_session.commit()

    async with db_session.begin():
        await _make_processor(db_session).process(_push_payload(), now=_NOW)
        await db_session.commit()

    async with db_session.begin():
        state = await _project_state(
            db_session, org_id=org_id, ltd_slug="sqr-112"
        )
    assert state is not None
    assert state.annotations is not None
    assert state.annotations["date_main_last_polled"] == (
        "2026-10-09T11:55:00+00:00"
    )
    refs = _pushed_refs(state)
    assert len(refs) == 20
    assert "expired" not in refs
    assert "branch-00" not in refs
    assert refs["tickets/DM-1"] == _NOW.isoformat()


@pytest.mark.asyncio
async def test_process_defaults_the_push_time_to_now(
    db_session: AsyncSession,
) -> None:
    """Without an explicit ``now``, the stamp is the processing time."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-clock")
        await _seed_synced_project(db_session, org_id=org_id, slug="sqr-112")
        await db_session.commit()

    before = datetime.now(tz=UTC)
    async with db_session.begin():
        await _make_processor(db_session).process(_push_payload())
        await db_session.commit()
    after = datetime.now(tz=UTC)

    async with db_session.begin():
        state = await _project_state(
            db_session, org_id=org_id, ltd_slug="sqr-112"
        )
    pushed_at = datetime.fromisoformat(_pushed_refs(state)["tickets/DM-1"])
    assert before <= pushed_at <= after


@pytest.mark.asyncio
async def test_tombstoned_project_is_not_stamped(
    db_session: AsyncSession,
) -> None:
    """A project whose state row is tombstoned is deleted, not synced."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-tombstoned")
        await _seed_synced_project(db_session, org_id=org_id, slug="sqr-112")
        await db_session.execute(
            update(SqlKeeperSyncState)
            .where(
                SqlKeeperSyncState.org_id == org_id,
                SqlKeeperSyncState.ltd_slug == "sqr-112",
            )
            .values(
                date_tombstoned=_NOW,
                tombstone_reason=TombstoneReason.manual_delete.value,
            )
        )
        await db_session.commit()

    async with db_session.begin():
        stamped = await _make_processor(db_session).process(
            _push_payload(), now=_NOW
        )
        await db_session.commit()

    assert stamped == []
