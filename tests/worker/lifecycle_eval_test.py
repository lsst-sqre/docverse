"""Tests for the ``lifecycle_eval`` per-org worker function.

Seeds an org with multiple projects plus shifted ``date_updated`` /
``date_completed`` timestamps and asserts the worker soft-deletes the
expected rows, transitions the ``queue_jobs`` row correctly, and
finalises the parent ``lifecycle_eval_runs`` row when the queue is
drained.
"""

from __future__ import annotations

import importlib
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select, update

from docverse.client.models import (
    BuildCreate,
    EditionKind,
    LifecycleEvalRunStatus,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.client.models.queue_enums import JobKind, JobStatus
from docverse.dbschema.build import SqlBuild
from docverse.dbschema.edition import SqlEdition
from docverse.dbschema.edition_build_history import SqlEditionBuildHistory
from docverse.dbschema.organization import SqlOrganization
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.lifecycle import (
    BuildHistoryOrphanRule,
    DraftInactivityRule,
    LifecycleRuleSet,
    RefDeletedRule,
)
from docverse.factory import Factory
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.editionpublisher import (
    EditionPublisher,
    MockEditionPublisher,
)
from docverse.storage.keeper_sync import KeeperSyncStateStore, ResourceType
from docverse.storage.lifecycle_eval_run_store import LifecycleEvalRunStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.lifecycle_eval import lifecycle_eval
from tests.worker.conftest import make_worker_ctx

NOW = datetime(2026, 5, 12, 12, 0, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _freeze_eval_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the worker's lifecycle evaluation clock to ``NOW``.

    The fixtures below build their ``date_updated`` / ``date_completed``
    timestamps as offsets from the frozen ``NOW`` constant, but the
    worker evaluates against ``_utcnow()`` (real wall clock in
    production). Monkeypatching ``_utcnow`` to return ``NOW`` keeps the
    fixture dates and the evaluation clock pinned together so the suite
    is deterministic regardless of the real wall-clock date.
    """
    # Resolve the real submodule via import_module: the functions package
    # __init__ rebinds the ``lifecycle_eval`` attribute to the worker
    # function, shadowing the submodule, so a dotted-string monkeypatch
    # target would resolve to the function and miss ``_utcnow``.
    module = importlib.import_module(
        "docverse.worker.functions.lifecycle_eval"
    )
    monkeypatch.setattr(module, "_utcnow", lambda: NOW)


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(
    db_session: AsyncSession,
    *,
    slug: str = "lce-org",
    lifecycle_rules: LifecycleRuleSet | None = None,
) -> tuple[int, str]:
    org_store = OrganizationStore(session=db_session, logger=_logger())
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title=f"LCE Org {slug}",
            base_domain=f"{slug}.example.com",
            lifecycle_rules=lifecycle_rules,
        )
    )
    return org.id, org.slug


async def _seed_project(
    db_session: AsyncSession,
    *,
    org_id: int,
    slug: str,
    lifecycle_rules: LifecycleRuleSet | None = None,
) -> int:
    project_store = ProjectStore(session=db_session, logger=_logger())
    project = await project_store.create(
        org_id=org_id,
        data=ProjectCreate(
            slug=slug,
            title=f"Project {slug}",
            source_url=f"https://example.com/{slug}",
            lifecycle_rules=lifecycle_rules,
        ),
    )
    return project.id


async def _seed_edition(
    db_session: AsyncSession,
    *,
    project_id: int,
    slug: str,
    kind: EditionKind,
    date_updated: datetime,
    lifecycle_exempt: bool = False,
    current_build_id: int | None = None,
) -> int:
    edition_store = EditionStore(session=db_session, logger=_logger())
    edition = await edition_store.create_internal(
        project_id=project_id,
        slug=slug,
        title=f"Edition {slug}",
        kind=kind,
        tracking_mode=TrackingMode.git_ref,
        lifecycle_exempt=lifecycle_exempt,
    )
    # Bypass ``onupdate=func.now()`` so the seeded timestamp survives.
    await db_session.execute(
        update(SqlEdition)
        .where(SqlEdition.id == edition.id)
        .values(
            date_updated=date_updated,
            current_build_id=current_build_id,
        )
    )
    return edition.id


async def _seed_build(
    db_session: AsyncSession,
    *,
    project_id: int,
    project_slug: str,
    date_completed: datetime,
) -> int:
    build_store = BuildStore(session=db_session, logger=_logger())
    # content_hash must be unique-per-project, so derive it from the
    # build's eventual id by using project + completion timestamp.
    content_hash = (
        f"sha256:{project_id:08x}{int(date_completed.timestamp()):016x}"
        + "0" * 40
    )
    build = await build_store.create(
        project_id=project_id,
        project_slug=project_slug,
        data=BuildCreate(
            git_ref="main",
            content_hash=content_hash,
        ),
        uploader="seed",
    )
    await db_session.execute(
        update(SqlBuild)
        .where(SqlBuild.id == build.id)
        .values(
            date_created=date_completed,
            date_completed=date_completed,
        )
    )
    return build.id


async def _seed_run_and_queue_job(
    db_session: AsyncSession, *, org_id: int, org_slug: str
) -> tuple[int, int]:
    """Create one ``lifecycle_eval_runs`` row + a per-org queue_jobs row.

    Mirrors the dispatcher contract documented on
    :mod:`docverse.worker.functions.lifecycle_eval`: ``subject_label``
    is the org's slug so the per-org mutex stays human-meaningful for
    operators inspecting ``queue_jobs`` and the internal ``org_id``
    is never reused as the queue's user-visible subject.
    """
    run_store = LifecycleEvalRunStore(session=db_session, logger=_logger())
    run = await run_store.create()
    await run_store.transition_status(
        run_id=run.id, new_status=LifecycleEvalRunStatus.in_progress
    )
    queue_job_row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        kind=JobKind.lifecycle_eval.value,
        status=JobStatus.queued.value,
        org_id=org_id,
        lifecycle_eval_run_id=run.id,
        subject_label=org_slug,
    )
    db_session.add(queue_job_row)
    await db_session.flush()
    return run.id, queue_job_row.id


@pytest.mark.asyncio
async def test_lifecycle_eval_soft_deletes_stale_drafts_and_orphan_builds(
    app: None,
    db_session: AsyncSession,
) -> None:
    """End-to-end: stale drafts + orphan builds get ``date_deleted`` set.

    Seeds two projects under one org:

    * ``project-a`` has org-level ``draft_inactivity(max_days_inactive=
      30)`` so its stale draft (updated 60 days ago) becomes a soft-
      delete candidate but its fresh draft and exempt draft do not.
    * ``project-b`` has its own ``build_history_orphan`` rule that
      replaces org rules — it has an unreferenced old build that must
      be deleted and a current build that must not.

    Asserts the worker correctly applies the per-project rule
    resolution (org rules everywhere except where project overrides),
    soft-deletes the right rows, completes the queue_job, and finalises
    the parent run to ``succeeded``.
    """
    org_rules = LifecycleRuleSet(
        root=[DraftInactivityRule(max_days_inactive=30)]
    )
    project_b_rules = LifecycleRuleSet(
        root=[BuildHistoryOrphanRule(min_position=1, min_age_days=30)]
    )

    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, lifecycle_rules=org_rules
        )
        project_a_id = await _seed_project(
            db_session, org_id=org_id, slug="project-a"
        )
        project_b_id = await _seed_project(
            db_session,
            org_id=org_id,
            slug="project-b",
            lifecycle_rules=project_b_rules,
        )

        # project-a editions: stale draft (60d), fresh draft (5d), exempt
        # stale draft (60d, lifecycle_exempt=True), and a release edition
        # which is never a draft_inactivity candidate.
        stale_draft_id = await _seed_edition(
            db_session,
            project_id=project_a_id,
            slug="stale-draft",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=60),
        )
        fresh_draft_id = await _seed_edition(
            db_session,
            project_id=project_a_id,
            slug="fresh-draft",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=5),
        )
        exempt_draft_id = await _seed_edition(
            db_session,
            project_id=project_a_id,
            slug="exempt-draft",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=60),
            lifecycle_exempt=True,
        )
        release_id = await _seed_edition(
            db_session,
            project_id=project_a_id,
            slug="release-1",
            kind=EditionKind.release,
            date_updated=NOW - timedelta(days=60),
        )

        # project-b builds: one current (40d old, protected), one
        # orphan (40d old, no edition reference).
        current_build_id = await _seed_build(
            db_session,
            project_id=project_b_id,
            project_slug="project-b",
            date_completed=NOW - timedelta(days=40),
        )
        orphan_build_id = await _seed_build(
            db_session,
            project_id=project_b_id,
            project_slug="project-b",
            date_completed=NOW - timedelta(days=40),
        )
        # project-b also has a draft edition that references the current
        # build. project-b has no draft_inactivity rule so the edition
        # is not a candidate even though it's stale.
        await _seed_edition(
            db_session,
            project_id=project_b_id,
            slug="main",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=60),
            current_build_id=current_build_id,
        )

        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(http_client=http_client)
    result = await lifecycle_eval(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "lifecycle_eval_run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await http_client.aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            stale = await edition_store.get_by_slug(
                project_id=project_a_id, slug="stale-draft"
            )
            # stale-draft is soft-deleted, so get_by_slug filters it out.
            assert stale is None
            fresh = await edition_store.get_by_slug(
                project_id=project_a_id, slug="fresh-draft"
            )
            assert fresh is not None
            assert fresh.id == fresh_draft_id
            exempt = await edition_store.get_by_slug(
                project_id=project_a_id, slug="exempt-draft"
            )
            assert exempt is not None
            assert exempt.id == exempt_draft_id
            release = await edition_store.get_by_slug(
                project_id=project_a_id, slug="release-1"
            )
            assert release is not None
            assert release.id == release_id

            build_store = BuildStore(session=session, logger=_logger())
            current_build = await build_store.get_by_id(current_build_id)
            assert current_build is not None
            assert current_build.date_deleted is None
            orphan_build = await build_store.get_by_id(orphan_build_id)
            assert orphan_build is not None
            assert orphan_build.date_deleted is not None

            # Lookup soft-deleted edition row directly to verify
            # date_deleted set.
            sql = await session.execute(
                select(SqlEdition.date_deleted).where(
                    SqlEdition.id == stale_draft_id
                )
            )
            assert sql.scalar_one() is not None

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            qj = await queue_job_store.get(queue_job_id)
            assert qj is not None
            assert qj.status == JobStatus.completed

            run_store = LifecycleEvalRunStore(
                session=session, logger=_logger()
            )
            run = await run_store.get(run_id)
            assert run is not None
            # One child queue_job, terminal → run rolls to succeeded.
            assert run.status is LifecycleEvalRunStatus.succeeded


@pytest.mark.asyncio
async def test_lifecycle_eval_no_rules_is_a_noop(
    app: None,
    db_session: AsyncSession,
) -> None:
    """An org with no rules anywhere completes cleanly and deletes nothing.

    Pre-flight in the dispatcher skips orgs with no rules, but this
    test exercises the defensive path where a queue_job for an org
    that turns out to have no rules still completes safely without
    mutating anything.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="lce-empty-org")
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="empty-project"
        )
        edition_id = await _seed_edition(
            db_session,
            project_id=project_id,
            slug="ancient-draft",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=365),
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(http_client=http_client)
    result = await lifecycle_eval(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "lifecycle_eval_run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await http_client.aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            edition = await edition_store.get_by_slug(
                project_id=project_id, slug="ancient-draft"
            )
            assert edition is not None
            assert edition.id == edition_id

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            qj = await queue_job_store.get(queue_job_id)
            assert qj is not None
            assert qj.status == JobStatus.completed


@pytest.mark.asyncio
async def test_lifecycle_eval_project_rules_replace_org_rules(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A project's explicit ``[]`` rule set disables inherited org rules.

    Per SQR-112: project rules **replace** org rules with no merging.
    This test seeds an org with a draft_inactivity rule and one
    project that explicitly opts out with ``LifecycleRuleSet(root=[])``,
    plus another project that inherits the org rule. The stale draft in
    the opt-out project survives; the stale draft in the inheriting
    project gets soft-deleted.
    """
    org_rules = LifecycleRuleSet(
        root=[DraftInactivityRule(max_days_inactive=30)]
    )
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, slug="lce-opt-out-org", lifecycle_rules=org_rules
        )
        opt_out_id = await _seed_project(
            db_session,
            org_id=org_id,
            slug="opt-out",
            lifecycle_rules=LifecycleRuleSet(root=[]),
        )
        inheriting_id = await _seed_project(
            db_session, org_id=org_id, slug="inheriting"
        )
        opt_out_draft_id = await _seed_edition(
            db_session,
            project_id=opt_out_id,
            slug="should-survive",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=60),
        )
        inheriting_draft_id = await _seed_edition(
            db_session,
            project_id=inheriting_id,
            slug="should-be-deleted",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=60),
        )

        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(http_client=http_client)
    result = await lifecycle_eval(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "lifecycle_eval_run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await http_client.aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            opt_out_survivor = await edition_store.get_by_slug(
                project_id=opt_out_id, slug="should-survive"
            )
            assert opt_out_survivor is not None
            assert opt_out_survivor.id == opt_out_draft_id

            inheriting_victim = await edition_store.get_by_slug(
                project_id=inheriting_id, slug="should-be-deleted"
            )
            # Soft-deleted → get_by_slug returns None.
            assert inheriting_victim is None
            # Confirm via direct row lookup that date_deleted is set.
            row_result = await session.execute(
                select(SqlEdition.date_deleted).where(
                    SqlEdition.id == inheriting_draft_id
                )
            )
            assert row_result.scalar_one() is not None


@pytest.mark.asyncio
async def test_lifecycle_eval_ignores_ref_deleted_rule(
    app: None,
    db_session: AsyncSession,
) -> None:
    """An org with both rules: lifecycle_eval inactivates, never ref-deletes.

    Mirror guard for the cross-firing fix: ``lifecycle_eval`` owns
    ``DraftInactivityRule`` and ``BuildHistoryOrphanRule`` but never
    ``RefDeletedRule`` (the ``git_ref_audit`` cron owns that). It filters
    the resolved rule set down to its own kinds before evaluation, so a
    co-configured ``RefDeletedRule`` performs no ref-deletion here.

    Seeds one project with two drafts: a stale draft (updated 60 days
    ago) that ``DraftInactivityRule`` soft-deletes, and a recently-
    updated draft tracking a git ref that survives — lifecycle_eval does
    no ref-deletion (``RefDeletedRule`` is filtered out and ``live_refs``
    is ``None`` anyway). Guards against a future regression where
    lifecycle_eval starts honoring ``RefDeletedRule``.
    """
    org_rules = LifecycleRuleSet(
        root=[DraftInactivityRule(max_days_inactive=30), RefDeletedRule()]
    )
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, slug="lce-ref-deleted-org", lifecycle_rules=org_rules
        )
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="ref-deleted-proj"
        )
        stale_draft_id = await _seed_edition(
            db_session,
            project_id=project_id,
            slug="stale-draft",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=60),
        )
        recent_draft_id = await _seed_edition(
            db_session,
            project_id=project_id,
            slug="recent-ref-draft",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=5),
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(http_client=http_client)
    result = await lifecycle_eval(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "lifecycle_eval_run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await http_client.aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            # Stale draft is soft-deleted by DraftInactivityRule.
            stale = await edition_store.get_by_slug(
                project_id=project_id, slug="stale-draft"
            )
            assert stale is None
            row_result = await session.execute(
                select(SqlEdition.date_deleted).where(
                    SqlEdition.id == stale_draft_id
                )
            )
            assert row_result.scalar_one() is not None

            # Recent ref-tracking draft survives: no ref-deletion here.
            recent = await edition_store.get_by_slug(
                project_id=project_id, slug="recent-ref-draft"
            )
            assert recent is not None
            assert recent.id == recent_draft_id
            assert recent.date_deleted is None


@pytest.mark.asyncio
async def test_lifecycle_eval_failure_marks_queue_job_and_finalises_run(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exception inside the worker → queue_job ``failed``, run finalised.

    Drives a failure inside ``_evaluate_org`` by patching
    ``OrganizationStore.get_by_id`` to raise so the worker's
    except-branch must mark the queue_job ``failed`` and call
    ``maybe_finalise_lifecycle_run`` — which rolls the single-child run
    to ``partial_failure``.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="lce-fail-org")
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    async def _raise(*args: object, **kwargs: object) -> None:
        msg = "synthetic failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(OrganizationStore, "get_by_id", _raise)

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(http_client=http_client)
    with pytest.raises(RuntimeError, match="synthetic failure"):
        await lifecycle_eval(
            ctx,
            {
                "org_id": org_id,
                "org_slug": org_slug,
                "lifecycle_eval_run_id": run_id,
                "queue_job_id": queue_job_id,
            },
        )
    await http_client.aclose()

    async for session in db_session_dependency():
        async with session.begin():
            queue_job_store = QueueJobStore(session=session, logger=_logger())
            qj = await queue_job_store.get(queue_job_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            assert qj.errors is not None
            assert "synthetic failure" in qj.errors["message"]

            run_store = LifecycleEvalRunStore(
                session=session, logger=_logger()
            )
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status is LifecycleEvalRunStatus.partial_failure


@pytest.mark.asyncio
async def test_lifecycle_eval_protects_shared_build_referenced_elsewhere(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A build held by another edition's ``current_build_id`` is not deleted.

    Per user story 18: a build shared across two editions is kept as
    long as at least one edition still holds it within its retention
    window. This test exercises the worker's batched cross-project /
    cross-edition loader path so the in-memory protection logic
    actually sees both editions' state when deciding what to delete.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session,
            slug="lce-shared-build-org",
            lifecycle_rules=LifecycleRuleSet(
                root=[BuildHistoryOrphanRule(min_position=5, min_age_days=30)]
            ),
        )
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="proj-shared"
        )
        shared_build_id = await _seed_build(
            db_session,
            project_id=project_id,
            project_slug="proj-shared",
            date_completed=NOW - timedelta(days=60),
        )
        protector_edition_id = await _seed_edition(
            db_session,
            project_id=project_id,
            slug="protector",
            kind=EditionKind.release,
            date_updated=NOW,
            current_build_id=shared_build_id,
        )
        other_edition_id = await _seed_edition(
            db_session,
            project_id=project_id,
            slug="other",
            kind=EditionKind.release,
            date_updated=NOW,
        )
        # Record a history row on the "other" edition that references
        # the shared build at position 10 — high enough that, alone, the
        # build would be orphaned by min_position=5. The protector
        # edition holding it as current_build_id is what saves it.
        history_store = EditionBuildHistoryStore(
            session=db_session, logger=_logger()
        )
        await history_store.record(
            edition_id=other_edition_id, build_id=shared_build_id
        )
        # Bump it to position 10.
        await db_session.execute(
            update(SqlEditionBuildHistory)
            .where(SqlEditionBuildHistory.edition_id == other_edition_id)
            .values(position=10)
        )

        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(http_client=http_client)
    result = await lifecycle_eval(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "lifecycle_eval_run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await http_client.aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            build_store = BuildStore(session=session, logger=_logger())
            build = await build_store.get_by_id(shared_build_id)
            assert build is not None
            assert build.date_deleted is None

            edition_store = EditionStore(session=session, logger=_logger())
            protector = await edition_store.get_by_slug(
                project_id=project_id, slug="protector"
            )
            assert protector is not None
            assert protector.id == protector_edition_id


@pytest.mark.asyncio
async def test_lifecycle_eval_enqueues_dashboard_build_for_affected_projects(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Edition soft-delete triggers one ``dashboard_build`` per project.

    Mirrors the user-initiated DELETE handler's post-commit
    ``try_enqueue_dashboard_build_by_slug`` call so the cached
    dashboard artifact does not keep showing editions whose
    ``date_deleted`` is set.

    Seeds two projects under one org with the same
    ``draft_inactivity`` rule:

    * ``with-stale`` has a stale draft that the worker will
      soft-delete, so it must get one ``dashboard_build`` row.
    * ``no-stale`` has only a fresh draft, so it must not get a
      ``dashboard_build`` row.

    The assertion checks the global count of ``dashboard_build`` rows
    is exactly one and that the row is scoped to ``with-stale`` —
    not ``no-stale`` and not some other org. The
    ``has_active_dashboard_build`` helper on ``QueueJobStore`` is
    used to confirm the per-project scoping the cached dashboard
    consumer relies on.
    """
    org_rules = LifecycleRuleSet(
        root=[DraftInactivityRule(max_days_inactive=30)]
    )
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session,
            slug="lce-dash-enq-org",
            lifecycle_rules=org_rules,
        )
        affected_project_id = await _seed_project(
            db_session, org_id=org_id, slug="with-stale"
        )
        untouched_project_id = await _seed_project(
            db_session, org_id=org_id, slug="no-stale"
        )
        await _seed_edition(
            db_session,
            project_id=affected_project_id,
            slug="stale-draft",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=60),
        )
        await _seed_edition(
            db_session,
            project_id=untouched_project_id,
            slug="fresh-draft",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=5),
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(http_client=http_client)
    result = await lifecycle_eval(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "lifecycle_eval_run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await http_client.aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            dash_rows = await session.execute(
                select(SqlQueueJob).where(
                    SqlQueueJob.kind == JobKind.dashboard_build.value
                )
            )
            rows = list(dash_rows.scalars().all())
            assert len(rows) == 1
            assert rows[0].org_id == org_id
            assert rows[0].project_id == affected_project_id

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            assert await queue_job_store.has_active_dashboard_build(
                org_id=org_id, project_id=affected_project_id
            )
            assert not await queue_job_store.has_active_dashboard_build(
                org_id=org_id, project_id=untouched_project_id
            )


@pytest.mark.asyncio
async def test_lifecycle_eval_skips_dashboard_build_for_build_only_deletes(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A project whose only deletion is a build does not get a dashboard_build.

    The cached dashboard artifact is rebuilt from the edition list,
    not the build list — soft-deleting an orphan build does not
    change what the dashboard renders, so an enqueue would be wasted
    work. Only projects with at least one edition soft-delete should
    get a ``dashboard_build`` row.
    """
    org_rules = LifecycleRuleSet(
        root=[BuildHistoryOrphanRule(min_position=1, min_age_days=30)]
    )
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session,
            slug="lce-build-only-org",
            lifecycle_rules=org_rules,
        )
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="build-only"
        )
        current_build_id = await _seed_build(
            db_session,
            project_id=project_id,
            project_slug="build-only",
            date_completed=NOW - timedelta(days=40),
        )
        await _seed_build(
            db_session,
            project_id=project_id,
            project_slug="build-only",
            date_completed=NOW - timedelta(days=40),
        )
        await _seed_edition(
            db_session,
            project_id=project_id,
            slug="main",
            kind=EditionKind.release,
            date_updated=NOW,
            current_build_id=current_build_id,
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(http_client=http_client)
    result = await lifecycle_eval(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "lifecycle_eval_run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await http_client.aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            dash_rows = await session.execute(
                select(SqlQueueJob).where(
                    SqlQueueJob.kind == JobKind.dashboard_build.value
                )
            )
            assert list(dash_rows.scalars().all()) == []


def _mock_create_edition_publisher(publisher: EditionPublisher) -> object:
    """Build a ``create_edition_publisher_for_org`` replacement.

    Mirrors the helper in ``tests/worker/publish_edition_test.py``: returns
    the supplied publisher regardless of the (org_id, service_label) the
    factory was called with so the test does not have to seed the full
    service-config + credential resolver path.
    """

    async def _create(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
    ) -> EditionPublisher:
        _ = (self, org_id, service_label)
        return publisher

    return _create


@pytest.mark.asyncio
async def test_lifecycle_eval_unpublishes_each_soft_deleted_edition(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each soft-deleted edition records one ``unpublish`` call on the CDN.

    Seeds an org with a configured ``cdn_service_label`` plus two stale
    drafts and one fresh draft under a single project so the
    ``draft_inactivity`` rule matches exactly the two stale drafts. The
    test patches ``Factory.create_edition_publisher_for_org`` to return a
    ``MockEditionPublisher`` and asserts that ``unpublish_calls``
    contains exactly one entry per soft-deleted edition with the right
    ``(project_slug, edition_slug)`` shape — and that the fresh draft
    (which is not soft-deleted) does not appear.
    """
    org_rules = LifecycleRuleSet(
        root=[DraftInactivityRule(max_days_inactive=30)]
    )
    mock_publisher = MockEditionPublisher()

    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session,
            slug="lce-unpub-org",
            lifecycle_rules=org_rules,
        )
        # Stamp a CDN service label so the publishing service resolves
        # the publisher rather than no-opping.
        await db_session.execute(
            update(SqlOrganization)
            .where(SqlOrganization.id == org_id)
            .values(cdn_service_label="cdn-prod")
        )
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="unpub-proj"
        )
        await _seed_edition(
            db_session,
            project_id=project_id,
            slug="stale-a",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=60),
        )
        await _seed_edition(
            db_session,
            project_id=project_id,
            slug="stale-b",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=90),
        )
        await _seed_edition(
            db_session,
            project_id=project_id,
            slug="fresh",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=5),
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_create_edition_publisher(mock_publisher),
    )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(http_client=http_client)
    result = await lifecycle_eval(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "lifecycle_eval_run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await http_client.aclose()
    assert result == "completed"

    recorded_slugs = sorted(
        call.edition_slug for call in mock_publisher.unpublish_calls
    )
    assert recorded_slugs == ["stale-a", "stale-b"]
    for call in mock_publisher.unpublish_calls:
        assert call.project_slug == "unpub-proj"


@pytest.mark.asyncio
async def test_lifecycle_eval_writes_lifecycle_delete_tombstone(
    app: None,
    db_session: AsyncSession,
) -> None:
    """The worker's soft-delete stamps a ``lifecycle_delete`` tombstone."""
    org_rules = LifecycleRuleSet(
        root=[DraftInactivityRule(max_days_inactive=30)]
    )
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, slug="lce-tomb-org", lifecycle_rules=org_rules
        )
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="lce-tomb-proj"
        )
        stale_id = await _seed_edition(
            db_session,
            project_id=project_id,
            slug="stale-tomb",
            kind=EditionKind.draft,
            date_updated=NOW - timedelta(days=60),
        )
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=9090,
            ltd_slug="stale-tomb",
            docverse_id=stale_id,
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(http_client=http_client)
    result = await lifecycle_eval(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "lifecycle_eval_run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await http_client.aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            state_store = KeeperSyncStateStore(
                session=session, logger=_logger()
            )
            state = await state_store.get(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=9090,
                include_tombstoned=True,
            )
            assert state is not None
            assert state.date_tombstoned is not None
            assert state.tombstone_reason == "lifecycle_delete"
