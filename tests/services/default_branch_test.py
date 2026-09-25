"""Tests for the ``DefaultBranchService`` convergence rule.

The one guarded rule that converges a project's ``__main`` edition on
its repository's default branch (PRD #721): record the branch, rewrite
``__main``'s tracked ref only when that ref is gone, retire the draft
the old tracking let accumulate on the new branch, and repoint
``__main`` at that branch's newest completed build.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple

import pytest
import structlog
from fastapi import FastAPI
from safir.arq import MockArqQueue
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    EditionKindSource,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.models.projects import ProjectGitHubBindingCreate
from docverse.models.queue_enums import PublishStatus
from docverse_server.config import Configuration
from docverse_server.dbschema.build import SqlBuild
from docverse_server.domain.build import Build
from docverse_server.domain.edition import DEFAULT_EDITION_SLUG, Edition
from docverse_server.domain.project import Project
from docverse_server.factory import Factory
from docverse_server.services.default_branch import (
    DefaultBranchOutcome,
    DefaultBranchService,
    DefaultBranchTrigger,
)
from docverse_server.services.lock_service import LockKey, LockService
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from tests.support.lock_service_spy import LockEvent, RecordingLockService

_config = Configuration()
_BASE = datetime(2026, 1, 1, tzinfo=UTC)


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


@dataclass
class _StubPublishingService:
    """Records ``unpublish`` calls in place of the CDN publisher."""

    calls: list[tuple[int, str, str]] = field(default_factory=list)

    async def unpublish(
        self, *, org_id: int, project_slug: str, edition_slug: str
    ) -> None:
        self.calls.append((org_id, project_slug, edition_slug))


@dataclass
class _Harness:
    """A service wired to one session, plus what the tests inspect."""

    service: DefaultBranchService
    factory: Factory
    publishing: _StubPublishingService


def _harness(
    db_session: AsyncSession, *, lock_service: LockService | None = None
) -> _Harness:
    factory = Factory(
        session=db_session,
        logger=_logger(),
        arq_queue=MockArqQueue(),
        default_queue_name=_config.arq_queue_name,
    )
    publishing = _StubPublishingService()
    service = DefaultBranchService(
        project_store=factory.create_project_store(),
        edition_store=factory.create_edition_store(),
        build_store=factory.create_build_store(),
        edition_service=factory.create_edition_service(),
        publishing_service=publishing,  # type: ignore[arg-type]
        lock_service=lock_service or factory.create_lock_service(),
        logger=_logger(),
    )
    return _Harness(service=service, factory=factory, publishing=publishing)


class _Seeded(NamedTuple):
    org_id: int
    project_id: int
    main_id: int


async def _seed(
    db_session: AsyncSession,
    *,
    main_mode: TrackingMode = TrackingMode.git_ref,
    main_params: dict[str, Any] | None = None,
) -> _Seeded:
    """Seed an org, a GitHub-bound project, and its ``__main`` edition.

    ``__main`` tracks ``master`` unless told otherwise, and is
    ``lifecycle_exempt`` with a custom title so a test can see that the
    rewrite leaves both alone.
    """
    logger = _logger()
    org = await OrganizationStore(session=db_session, logger=logger).create(
        OrganizationCreate(
            slug="db-org", title="DB Org", base_domain="db-org.example.com"
        )
    )
    project_store = ProjectStore(session=db_session, logger=logger)
    project = await project_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug="db-proj",
            title="DB Project",
            github=ProjectGitHubBindingCreate(owner="acme", repo="docs"),
        ),
        github_owner="acme",
        github_repo="docs",
    )
    main = await EditionStore(
        session=db_session, logger=logger
    ).create_internal(
        project_id=project.id,
        slug=DEFAULT_EDITION_SLUG,
        title="Latest",
        kind=EditionKind.main,
        tracking_mode=main_mode,
        tracking_params=(
            main_params
            if main_params is not None
            else (
                {"git_ref": "master"}
                if main_mode is TrackingMode.git_ref
                else None
            )
        ),
        lifecycle_exempt=True,
    )
    return _Seeded(org_id=org.id, project_id=project.id, main_id=main.id)


async def _seed_build(
    db_session: AsyncSession,
    *,
    project_id: int,
    git_ref: str,
    days: int,
    status: BuildStatus = BuildStatus.completed,
) -> Build:
    """Create a build on ``git_ref`` dated ``days`` after the base."""
    store = BuildStore(session=db_session, logger=_logger())
    build = await store.create(
        project_id=project_id,
        project_slug="db-proj",
        data=BuildCreate(git_ref=git_ref, content_hash="sha256:" + "b" * 64),
        uploader="testuser",
    )
    if status is not BuildStatus.pending:
        await store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
    if status not in (BuildStatus.pending, BuildStatus.processing):
        await store.transition_status(build_id=build.id, new_status=status)
    await db_session.execute(
        update(SqlBuild)
        .where(SqlBuild.id == build.id)
        .values(date_created=_BASE + timedelta(days=days))
    )
    return build


async def _seed_draft(
    db_session: AsyncSession,
    *,
    project_id: int,
    slug: str,
    git_ref: str,
    kind: EditionKind = EditionKind.draft,
    tracking_mode: TrackingMode = TrackingMode.git_ref,
    alternate_name: str | None = None,
) -> int:
    params: dict[str, Any] = {"git_ref": git_ref}
    if alternate_name is not None:
        params["alternate_name"] = alternate_name
    edition = await EditionStore(session=db_session, logger=_logger()).create(
        project_id=project_id,
        data=EditionCreate(
            slug=slug,
            title=slug,
            kind=kind,
            tracking_mode=tracking_mode,
            tracking_params=params,
        ),
    )
    return edition.id


async def _serve(
    db_session: AsyncSession, *, edition_id: int, build_id: int
) -> None:
    await EditionStore(session=db_session, logger=_logger()).set_current_build(
        edition_id=edition_id, build_id=build_id
    )


async def _project(db_session: AsyncSession, project_id: int) -> Project:
    async with db_session.begin():
        project = await ProjectStore(
            session=db_session, logger=_logger()
        ).get_by_id(project_id)
    assert project is not None
    return project


async def _edition(db_session: AsyncSession, edition_id: int) -> Edition:
    async with db_session.begin():
        edition = await EditionStore(
            session=db_session, logger=_logger()
        ).get_by_id(edition_id)
    assert edition is not None
    return edition


async def _apply(
    db_session: AsyncSession,
    harness: _Harness,
    project_id: int,
    *,
    default_branch: str = "main",
    old_default_branch: str | None = "master",
    live_refs: frozenset[str] | None = None,
) -> DefaultBranchOutcome:
    project = await _project(db_session, project_id)
    async with db_session.begin():
        outcome = await harness.service.apply(
            project=project,
            default_branch=default_branch,
            trigger=DefaultBranchTrigger.webhook,
            old_default_branch=old_default_branch,
            live_refs=live_refs,
        )
        await db_session.commit()
    return outcome


@pytest.mark.asyncio
async def test_apply_records_the_default_branch(
    app: FastAPI, db_session: AsyncSession
) -> None:
    """The column takes the branch GitHub reports, whatever ``__main`` does."""
    async with db_session.begin():
        seeded = await _seed(db_session, main_params={"git_ref": "main"})
        await db_session.commit()

    outcome = await _apply(db_session, _harness(db_session), seeded.project_id)

    assert outcome.column_changed is True
    project = await _project(db_session, seeded.project_id)
    assert project.github_default_branch == "main"


@pytest.mark.asyncio
async def test_apply_rewrites_main_tracking_the_old_default_branch(
    app: FastAPI, db_session: AsyncSession
) -> None:
    """``__main`` on the branch that stopped being the default follows it.

    Only ``tracking_params.git_ref`` moves: the rewrite is not a PATCH,
    so ``kind``, ``kind_source``, ``title``, and ``lifecycle_exempt``
    keep whatever they were.
    """
    async with db_session.begin():
        seeded = await _seed(db_session)
        await db_session.commit()
    before = await _edition(db_session, seeded.main_id)

    outcome = await _apply(db_session, _harness(db_session), seeded.project_id)

    assert outcome.main_rewritten is True
    assert outcome.rewritten_from == "master"
    main = await _edition(db_session, seeded.main_id)
    assert main.tracking_mode is TrackingMode.git_ref
    assert main.tracking_params == {"git_ref": "main"}
    assert main.kind is EditionKind.main
    assert main.kind_source is before.kind_source
    assert main.kind_source is EditionKindSource.derived
    assert main.title == "Latest"
    assert main.lifecycle_exempt is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("main_mode", "main_params"),
    [
        (TrackingMode.git_ref, {"git_ref": "docs"}),
        (TrackingMode.lsst_doc, None),
    ],
    ids=["tracks-a-live-branch", "lsst-doc"],
)
async def test_apply_leaves_main_alone_unless_its_ref_is_gone(
    app: FastAPI,
    db_session: AsyncSession,
    main_mode: TrackingMode,
    main_params: dict[str, Any] | None,
) -> None:
    """A ``__main`` pinned to a live branch, or in ``lsst_doc``, stays put.

    ``docs`` is not the branch that stopped being the default, so
    nothing says it is gone; ``lsst_doc`` carries no ``git_ref`` to
    rewrite. Only the column moves: no rewrite, no retired draft
    (the ``main`` draft here survives), and no repoint even though a
    newer ``main`` build exists.
    """
    async with db_session.begin():
        seeded = await _seed(
            db_session, main_mode=main_mode, main_params=main_params
        )
        served = await _seed_build(
            db_session, project_id=seeded.project_id, git_ref="docs", days=1
        )
        await _serve(db_session, edition_id=seeded.main_id, build_id=served.id)
        await _seed_build(
            db_session, project_id=seeded.project_id, git_ref="main", days=2
        )
        draft_id = await _seed_draft(
            db_session,
            project_id=seeded.project_id,
            slug="main",
            git_ref="main",
        )
        await db_session.commit()
    before = await _edition(db_session, seeded.main_id)
    harness = _harness(db_session)

    outcome = await _apply(db_session, harness, seeded.project_id)

    assert outcome.column_changed is True
    assert outcome.main_rewritten is False
    assert outcome.drafts_retired == ()
    assert outcome.repointed_build_id is None
    main = await _edition(db_session, seeded.main_id)
    assert main.tracking_params == before.tracking_params
    assert main.current_build_id == served.id
    assert (await _edition(db_session, draft_id)).slug == "main"
    assert harness.publishing.calls == []
    assert harness.factory.queue_dispatcher.pending == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("live_refs", "rewritten"),
    [
        (frozenset({"main", "v1.0"}), True),
        (frozenset({"main", "master"}), False),
    ],
    ids=["ref-absent", "ref-live"],
)
async def test_apply_reads_a_live_ref_set_as_evidence(
    app: FastAPI,
    db_session: AsyncSession,
    live_refs: frozenset[str],
    rewritten: bool,  # noqa: FBT001
) -> None:
    """With no old default branch, a live ref set decides "gone".

    The audit's arm of the rule: ``__main`` tracking a ref the
    repository no longer has is rewritten; one tracking a ref still in
    the set is not, even though it differs from the default branch.
    """
    async with db_session.begin():
        seeded = await _seed(db_session)
        await db_session.commit()

    outcome = await _apply(
        db_session,
        _harness(db_session),
        seeded.project_id,
        old_default_branch=None,
        live_refs=live_refs,
    )

    assert outcome.main_rewritten is rewritten
    main = await _edition(db_session, seeded.main_id)
    expected = "main" if rewritten else "master"
    assert main.tracking_params == {"git_ref": expected}


@pytest.mark.asyncio
async def test_apply_retires_the_duplicate_draft(
    app: FastAPI, db_session: AsyncSession
) -> None:
    """The ``main`` draft auto-created before the rename is retired.

    Once ``__main`` tracks ``main`` the draft would match every push
    alongside it, so it goes the way ``RefDeletedWebhookProcessor``
    retires a draft: soft-deleted as a lifecycle delete and unpublished.
    A deployment-scoped draft on ``main`` never matches alongside
    ``__main``, a release is not a draft, and a draft on another branch
    is not a duplicate, so all three stay.
    """
    async with db_session.begin():
        seeded = await _seed(db_session)
        draft_id = await _seed_draft(
            db_session,
            project_id=seeded.project_id,
            slug="main",
            git_ref="main",
        )
        alternate_id = await _seed_draft(
            db_session,
            project_id=seeded.project_id,
            slug="main-usdf",
            git_ref="main",
            tracking_mode=TrackingMode.alternate_git_ref,
            alternate_name="usdf",
        )
        release_id = await _seed_draft(
            db_session,
            project_id=seeded.project_id,
            slug="pinned",
            git_ref="main",
            kind=EditionKind.release,
        )
        other_id = await _seed_draft(
            db_session,
            project_id=seeded.project_id,
            slug="feature",
            git_ref="feature",
        )
        await db_session.commit()
    harness = _harness(db_session)

    outcome = await _apply(db_session, harness, seeded.project_id)

    assert outcome.drafts_retired == (draft_id,)
    assert harness.publishing.calls == [(seeded.org_id, "db-proj", "main")]
    async with db_session.begin():
        store = EditionStore(session=db_session, logger=_logger())
        assert await store.get_by_id(draft_id) is None
        for kept in (alternate_id, release_id, other_id):
            assert await store.get_by_id(kept) is not None


@pytest.mark.asyncio
async def test_apply_repoints_main_at_the_newest_completed_build(
    app: FastAPI, db_session: AsyncSession
) -> None:
    """``__main`` serves the new branch at once, and publishes it.

    The newest *completed* ``main`` build wins over a newer one still
    processing, which tracking will pick up when it completes. The
    repoint runs the usual publish sequence — history row,
    ``publish_status`` pending, a deferred ``publish_edition`` job — and
    moves the project's clock.
    """
    async with db_session.begin():
        seeded = await _seed(db_session)
        served = await _seed_build(
            db_session, project_id=seeded.project_id, git_ref="master", days=1
        )
        await _serve(db_session, edition_id=seeded.main_id, build_id=served.id)
        await _seed_build(
            db_session, project_id=seeded.project_id, git_ref="main", days=2
        )
        newest = await _seed_build(
            db_session, project_id=seeded.project_id, git_ref="main", days=3
        )
        await _seed_build(
            db_session,
            project_id=seeded.project_id,
            git_ref="main",
            days=4,
            status=BuildStatus.processing,
        )
        await db_session.commit()
    clock_before = (await _project(db_session, seeded.project_id)).date_updated
    harness = _harness(db_session)

    outcome = await _apply(db_session, harness, seeded.project_id)

    assert outcome.repointed_build_id == newest.id
    main = await _edition(db_session, seeded.main_id)
    assert main.current_build_id == newest.id
    assert main.publish_status is PublishStatus.pending
    async with db_session.begin():
        history = await EditionBuildHistoryStore(
            session=db_session, logger=_logger()
        ).list_by_edition(seeded.main_id)
    assert history[0].build_id == newest.id
    assert history[0].publish_status is PublishStatus.pending
    [pending] = harness.factory.queue_dispatcher.pending
    assert pending.job_type == "publish_edition"
    assert pending.payload["edition_id"] == seeded.main_id
    assert pending.payload["build_id"] == newest.id
    assert pending.payload["history_id"] == history[0].id
    project = await _project(db_session, seeded.project_id)
    assert project.date_updated > clock_before


@pytest.mark.asyncio
async def test_apply_keeps_the_pointer_when_the_new_branch_is_older(
    app: FastAPI, db_session: AsyncSession
) -> None:
    """The stale-build guard holds: an older ``main`` build is not served.

    The column and the tracking still converge, so the next ``main``
    push advances ``__main`` through ordinary tracking.
    """
    async with db_session.begin():
        seeded = await _seed(db_session)
        await _seed_build(
            db_session, project_id=seeded.project_id, git_ref="main", days=1
        )
        served = await _seed_build(
            db_session, project_id=seeded.project_id, git_ref="master", days=2
        )
        await _serve(db_session, edition_id=seeded.main_id, build_id=served.id)
        await db_session.commit()
    harness = _harness(db_session)

    outcome = await _apply(db_session, harness, seeded.project_id)

    assert outcome.column_changed is True
    assert outcome.main_rewritten is True
    assert outcome.repointed_build_id is None
    main = await _edition(db_session, seeded.main_id)
    assert main.tracking_params == {"git_ref": "main"}
    assert main.current_build_id == served.id
    assert main.publish_status is None
    assert harness.factory.queue_dispatcher.pending == ()


@pytest.mark.asyncio
async def test_apply_is_inert_on_redelivery(
    app: FastAPI, db_session: AsyncSession
) -> None:
    """Applying the same branch twice changes nothing the second time.

    The column already holds the branch and ``__main`` already tracks
    it, so there is nothing to rewrite, retire, or repoint, and the
    project's clock stays where the first delivery left it.
    """
    async with db_session.begin():
        seeded = await _seed(db_session)
        await _seed_build(
            db_session, project_id=seeded.project_id, git_ref="main", days=1
        )
        await _seed_draft(
            db_session,
            project_id=seeded.project_id,
            slug="main",
            git_ref="main",
        )
        await db_session.commit()
    first = await _apply(db_session, _harness(db_session), seeded.project_id)
    assert first.main_rewritten is True
    clock_before = (await _project(db_session, seeded.project_id)).date_updated
    harness = _harness(db_session)

    outcome = await _apply(db_session, harness, seeded.project_id)

    assert outcome == DefaultBranchOutcome(
        column_changed=False,
        rewritten_from=None,
        drafts_retired=(),
        repointed_build_id=None,
    )
    assert harness.publishing.calls == []
    assert harness.factory.queue_dispatcher.pending == ()
    project = await _project(db_session, seeded.project_id)
    assert project.date_updated == clock_before


@pytest.mark.asyncio
async def test_apply_holds_the_main_edition_update_lock(
    app: FastAPI, db_session: AsyncSession
) -> None:
    """The whole convergence runs under ``__main``'s ``EDITION_UPDATE``.

    The key the tracking, keeper-sync, and ``publish_edition`` writers
    take for the same edition, so none of them interleaves a pointer
    or tracking write with the rewrite and repoint.
    """
    async with db_session.begin():
        seeded = await _seed(db_session)
        await db_session.commit()
    events: list[LockEvent] = []
    lock_service = RecordingLockService(
        session=db_session, logger=_logger(), events=events
    )

    await _apply(
        db_session,
        _harness(db_session, lock_service=lock_service),
        seeded.project_id,
    )

    expected = LockKey.for_edition_update(
        org_id=seeded.org_id,
        project_id=seeded.project_id,
        edition_id=seeded.main_id,
    )
    assert [(e.event, e.lock_key) for e in events] == [
        ("enter", expected),
        ("exit", expected),
    ]
