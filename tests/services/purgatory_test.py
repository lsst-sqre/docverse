"""Tests for the ``purgatory_cleanup`` planning and reclamation service.

:class:`~docverse_server.services.purgatory.PurgatoryService` is the
half of the sweep that decides *what* to reclaim and *how*, with the
worker owning every transaction around it. Two things are worth pinning
here: the partition ``plan`` draws between builds nothing is serving and
builds a live edition still points at — reclaiming one of the latter
would take a served edition's content out from under it — and the exact
pair of object-store calls ``reclaim`` makes, including that a store
failure comes back out rather than being counted as a successful purge.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import structlog
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    BuildCreate,
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse_server.dbschema.build import SqlBuild
from docverse_server.domain.build import Build
from docverse_server.domain.organization import Organization
from docverse_server.services.purgatory import PurgatoryService
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.objectstore import (
    MockObjectStore,
    ObjectStoreError,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore

_HASH = "sha256:" + "d" * 64

#: Retention short enough that a build deleted "a while ago" in these
#: tests is always out of the window, and long enough that one deleted
#: "just now" is always inside it.
_RETENTION = timedelta(days=7)


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _service(db_session: AsyncSession) -> PurgatoryService:
    logger = _logger()
    return PurgatoryService(
        build_store=BuildStore(session=db_session, logger=logger),
        edition_store=EditionStore(session=db_session, logger=logger),
        logger=logger,
    )


async def _seed_org_and_project(
    db_session: AsyncSession,
) -> tuple[Organization, int]:
    """Create an organization with one project, returning both."""
    logger = _logger()
    org = await OrganizationStore(session=db_session, logger=logger).create(
        OrganizationCreate(
            slug="purg-org",
            title="Purgatory Org",
            base_domain="purg-org.example.com",
            purgatory_retention=int(_RETENTION.total_seconds()),
        )
    )
    project = await ProjectStore(session=db_session, logger=logger).create(
        org_id=org.id,
        data=ProjectCreate(
            slug="purg-proj",
            title="Purgatory Project",
            source_url="https://example.com/example/purg",
        ),
    )
    return org, project.id


async def _seed_build(
    db_session: AsyncSession,
    *,
    project_id: int,
    git_ref: str = "main",
    content_hash: str = _HASH,
) -> Build:
    """Create a live build in the seeded project."""
    return await BuildStore(session=db_session, logger=_logger()).create(
        project_id=project_id,
        project_slug="purg-proj",
        data=BuildCreate(git_ref=git_ref, content_hash=content_hash),
        uploader="testuser",
    )


async def _backdate_deletion(
    db_session: AsyncSession, *, build_id: int, deleted_at: datetime
) -> None:
    """Soft-delete a build at a chosen instant.

    ``soft_delete`` stamps ``func.now()``, which is always inside any
    plausible retention window; the plan's whole job is judging that
    timestamp against the org's retention, so tests have to place it.
    """
    await db_session.execute(
        update(SqlBuild)
        .where(SqlBuild.id == build_id)
        .values(date_deleted=deleted_at)
    )


@pytest.mark.asyncio
async def test_plan_offers_a_build_nothing_is_serving(
    db_session: AsyncSession,
) -> None:
    """An expired build with no live pointer is the sweep's ordinary work.

    This is the case the whole job exists for: a soft-deleted build past
    its organization's retention, unreferenced, still holding its tree
    and tarball on the store.
    """
    now = datetime.now(tz=UTC)
    async with db_session.begin():
        org, project_id = await _seed_org_and_project(db_session)
        build = await _seed_build(db_session, project_id=project_id)
        await _backdate_deletion(
            db_session, build_id=build.id, deleted_at=now - timedelta(days=30)
        )
        plan = await _service(db_session).plan(org=org, now=now, limit=10)
        await db_session.commit()

    assert [candidate.id for candidate in plan.purgeable] == [build.id]
    assert plan.referenced == ()


@pytest.mark.asyncio
async def test_plan_holds_back_a_build_a_live_edition_serves(
    db_session: AsyncSession,
) -> None:
    """A served build is quarantined, not reclaimed, however old it is.

    Retention says nothing about whether anything still resolves to the
    build. An edition whose ``current_build_id`` is this row is serving
    its tree right now, so reclaiming it would 404 a live URL. The 409
    guard on DELETE stops new cases arising, but rows soft-deleted
    before that guard existed are exactly what the first sweep will
    meet.
    """
    now = datetime.now(tz=UTC)
    logger = _logger()
    async with db_session.begin():
        org, project_id = await _seed_org_and_project(db_session)
        build = await _seed_build(db_session, project_id=project_id)
        edition_store = EditionStore(session=db_session, logger=logger)
        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="serving",
                title="Serving",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        await edition_store.set_current_build(
            edition_id=edition.id, build_id=build.id
        )
        await _backdate_deletion(
            db_session, build_id=build.id, deleted_at=now - timedelta(days=30)
        )
        plan = await _service(db_session).plan(org=org, now=now, limit=10)
        await db_session.commit()

    assert plan.purgeable == ()
    assert [held.build.id for held in plan.referenced] == [build.id]


@pytest.mark.asyncio
async def test_plan_names_the_editions_holding_a_build_back(
    db_session: AsyncSession,
) -> None:
    """The slugs travel with the held-back build, for the warning log.

    A skipped build is only actionable if the operator can see what is
    pinning it — that is what tells them which edition to roll back
    before the next tick can reclaim the bytes.
    """
    now = datetime.now(tz=UTC)
    logger = _logger()
    async with db_session.begin():
        org, project_id = await _seed_org_and_project(db_session)
        build = await _seed_build(db_session, project_id=project_id)
        edition_store = EditionStore(session=db_session, logger=logger)
        for slug in ("beta", "alpha"):
            edition = await edition_store.create(
                project_id=project_id,
                data=EditionCreate(
                    slug=slug,
                    title=slug,
                    kind=EditionKind.draft,
                    tracking_mode=TrackingMode.git_ref,
                ),
            )
            await edition_store.set_current_build(
                edition_id=edition.id, build_id=build.id
            )
        await _backdate_deletion(
            db_session, build_id=build.id, deleted_at=now - timedelta(days=30)
        )
        plan = await _service(db_session).plan(org=org, now=now, limit=10)
        await db_session.commit()

    assert plan.referenced[0].edition_slugs == ("alpha", "beta")


@pytest.mark.asyncio
async def test_plan_derives_the_cutoff_from_the_organization(
    db_session: AsyncSession,
) -> None:
    """Retention is read off the org row, not from a service constant.

    The per-org job is handed its own organization and has to judge that
    org's builds by that org's window; a build deleted inside it is
    still restorable and must not appear anywhere in the plan.
    """
    now = datetime.now(tz=UTC)
    async with db_session.begin():
        org, project_id = await _seed_org_and_project(db_session)
        build = await _seed_build(db_session, project_id=project_id)
        await _backdate_deletion(
            db_session,
            build_id=build.id,
            deleted_at=now - _RETENTION + timedelta(hours=1),
        )
        plan = await _service(db_session).plan(org=org, now=now, limit=10)
        await db_session.commit()

    assert plan.purgeable == ()
    assert plan.referenced == ()


@pytest.mark.asyncio
async def test_reclaim_empties_the_build_tree(
    db_session: AsyncSession,
) -> None:
    """Everything under ``storage_prefix`` goes, and the count is reported.

    The unpacked tree is the bulk of what a build occupies, and the
    worker's ``objects_deleted`` progress counter is this number.
    """
    async with db_session.begin():
        _, project_id = await _seed_org_and_project(db_session)
        build = await _seed_build(db_session, project_id=project_id)
        await db_session.commit()

    store = MockObjectStore()
    for name in ("index.html", "api/index.html", "_static/style.css"):
        await store.upload_object(
            key=f"{build.storage_prefix}{name}",
            data=b"x",
            content_type="text/plain",
        )
    await store.upload_object(
        key="purg-proj/__builds/other/index.html",
        data=b"x",
        content_type="text/plain",
    )

    outcome = await _service(db_session).reclaim(
        build=build, object_store=store
    )

    assert outcome.objects_deleted == 3
    assert await store.list_objects(prefix=build.storage_prefix) == []
    assert await store.list_objects(prefix="purg-proj/__builds/other/") == [
        "purg-proj/__builds/other/index.html"
    ]


@pytest.mark.asyncio
async def test_reclaim_removes_the_staging_tarball(
    db_session: AsyncSession,
) -> None:
    """The tarball is reclaimed too, and its absence is not an error.

    ``build_processing`` deletes the tarball at completion, so most
    builds reach the sweep with the key already gone — but a build that
    failed or was cancelled mid-upload still has it, and that copy is
    the second half of what the build occupies.
    """
    async with db_session.begin():
        _, project_id = await _seed_org_and_project(db_session)
        with_tarball = await _seed_build(db_session, project_id=project_id)
        without_tarball = await _seed_build(
            db_session,
            project_id=project_id,
            git_ref="other",
            content_hash="sha256:" + "e" * 64,
        )
        await db_session.commit()

    store = MockObjectStore()
    await store.upload_object(
        key=with_tarball.staging_key,
        data=b"tarball",
        content_type="application/gzip",
    )
    service = _service(db_session)

    await service.reclaim(build=with_tarball, object_store=store)
    await service.reclaim(build=without_tarball, object_store=store)

    assert await store.list_objects(prefix="__staging/") == []


@pytest.mark.asyncio
async def test_reclaim_reports_the_recorded_size_as_bytes_reclaimed(
    db_session: AsyncSession,
) -> None:
    """``bytes_reclaimed`` is the row's own inventory, not a re-measure.

    The objects are gone by the time anyone would want the number, so
    the size recorded at completion is the only thing left to report —
    and it is what the completion metric sums.
    """
    async with db_session.begin():
        _, project_id = await _seed_org_and_project(db_session)
        build = await _seed_build(db_session, project_id=project_id)
        await db_session.commit()

    outcome = await _service(db_session).reclaim(
        build=build.model_copy(update={"total_size_bytes": 4096}),
        object_store=MockObjectStore(),
    )

    assert outcome.bytes_reclaimed == 4096


@pytest.mark.asyncio
async def test_reclaim_reports_zero_bytes_when_the_row_never_sized_itself(
    db_session: AsyncSession,
) -> None:
    """A build that never completed has no inventory to report.

    ``total_size_bytes`` is written when processing finishes, so a
    cancelled or failed build reaches the sweep with it null. The
    counter has to stay a number the worker can sum.
    """
    async with db_session.begin():
        _, project_id = await _seed_org_and_project(db_session)
        build = await _seed_build(db_session, project_id=project_id)
        await db_session.commit()

    outcome = await _service(db_session).reclaim(
        build=build, object_store=MockObjectStore()
    )

    assert build.total_size_bytes is None
    assert outcome.bytes_reclaimed == 0


@pytest.mark.asyncio
async def test_reclaim_lets_an_object_store_failure_through(
    db_session: AsyncSession,
) -> None:
    """A refused delete must not read as a completed purge.

    The worker stamps ``date_purged`` on the strength of ``reclaim``
    returning, and the stamp takes the build out of every later work
    list. Swallowing the error here would record content as reclaimed
    while the bytes are still on the store with nothing pointing at
    them.
    """

    class _RefusingStore(MockObjectStore):
        async def delete_prefix(self, *, prefix: str) -> int:
            raise ObjectStoreError(
                bucket="docverse",
                prefix=prefix,
                operation="DeleteObjects",
                failures=[f"{prefix}index.html (AccessDenied)"],
            )

    async with db_session.begin():
        _, project_id = await _seed_org_and_project(db_session)
        build = await _seed_build(db_session, project_id=project_id)
        await db_session.commit()

    with pytest.raises(ObjectStoreError):
        await _service(db_session).reclaim(
            build=build, object_store=_RefusingStore()
        )
