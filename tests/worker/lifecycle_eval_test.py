"""Tests for the ``lifecycle_eval`` per-org worker function.

Seeds an org with multiple projects plus shifted ``date_updated`` /
``date_completed`` timestamps and asserts the worker soft-deletes the
expected rows, transitions the ``queue_jobs`` row correctly, and
finalises the parent ``lifecycle_eval_runs`` row when the queue is
drained.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import update

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
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.lifecycle import (
    BuildHistoryOrphanRule,
    DraftInactivityRule,
    LifecycleRuleSet,
)
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.lifecycle_eval_run_store import LifecycleEvalRunStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.lifecycle_eval import lifecycle_eval
from tests.worker.conftest import make_worker_ctx

NOW = datetime(2026, 5, 12, 12, 0, 0, tzinfo=UTC)


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
            doc_repo=f"https://example.com/{slug}",
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
    db_session: AsyncSession, *, org_id: int
) -> tuple[int, int]:
    """Create one ``lifecycle_eval_runs`` row + a per-org queue_jobs row."""
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
        subject_label=str(org_id),
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
            db_session, org_id=org_id
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
                update(SqlEdition)
                .where(SqlEdition.id == stale_draft_id)
                .returning(SqlEdition.date_deleted)
                .values()
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
            db_session, org_id=org_id
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
            db_session, org_id=org_id
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
                update(SqlEdition)
                .where(SqlEdition.id == inheriting_draft_id)
                .returning(SqlEdition.date_deleted)
                .values()
            )
            assert row_result.scalar_one() is not None


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
            db_session, org_id=org_id
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
            db_session, org_id=org_id
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
