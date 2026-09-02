"""Tests for BuildStore."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
import structlog
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    BuildCreate,
    BuildStatus,
    JobKind,
    JobStatus,
    OrganizationCreate,
    ProjectCreate,
)
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import (
    generate_base32_id,
    serialize_base32_id,
    validate_base32_id,
)
from docverse_server.domain.build import Build
from docverse_server.domain.content_hash import PLACEHOLDER_CONTENT_HASH
from docverse_server.exceptions import InvalidBuildStateError
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore


@pytest.fixture
def build_store(
    db_session: AsyncSession,
) -> BuildStore:
    logger = structlog.get_logger("docverse")
    return BuildStore(session=db_session, logger=logger)


async def _create_org_and_project(
    db_session: AsyncSession,
) -> tuple[int, int]:
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug="build-org",
            title="Build Org",
            base_domain="build.example.com",
        )
    )
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug="build-proj",
            title="Build Project",
            source_url="https://example.com/example/repo",
        ),
    )
    return org.id, project.id


def _build_data() -> BuildCreate:
    return BuildCreate(
        git_ref="main",
        content_hash="sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
    )


@pytest.mark.asyncio
async def test_create_build(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await db_session.commit()
    assert build.status == BuildStatus.pending
    assert build.public_id > 0
    assert build.staging_key.startswith("__staging/")
    assert build.uploader == "testuser"
    assert build.git_ref == "main"


@pytest.mark.asyncio
async def test_create_build_defaults_content_hash_to_placeholder(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    """An omitted client digest becomes the placeholder, not NULL.

    ``BuildCreate.content_hash`` is optional and deprecated, but
    ``builds.content_hash`` is ``NOT NULL`` — so the store, which is the
    one place a pending row is built, supplies the placeholder rather
    than letting the insert fail. The real identity lands later, when
    the worker transitions the build to ``completed``.
    """
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=BuildCreate(git_ref="main"),
            uploader="testuser",
        )
        await db_session.commit()
    assert build.content_hash == PLACEHOLDER_CONTENT_HASH


@pytest.mark.asyncio
async def test_create_build_keeps_supplied_content_hash(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    """A digest an old client did send is stored verbatim while pending."""
    data = _build_data()
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=data,
            uploader="testuser",
        )
        await db_session.commit()
    assert build.content_hash == data.content_hash


@pytest.mark.asyncio
async def test_create_build_sets_storage_prefix(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    """storage_prefix is computed as {project_slug}/__builds/{base32_id}/."""
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await db_session.commit()

    base32_id = serialize_base32_id(build.public_id)
    expected = f"build-proj/__builds/{base32_id}/"
    assert build.storage_prefix == expected


@pytest.mark.asyncio
async def test_transition_pending_to_processing(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        processing = await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        await db_session.commit()
    assert processing.status == BuildStatus.processing
    assert processing.date_uploaded is not None


@pytest.mark.asyncio
async def test_transition_processing_to_completed(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        completed = await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.completed
        )
        await db_session.commit()
    assert completed.status == BuildStatus.completed
    assert completed.date_completed is not None


@pytest.mark.asyncio
async def test_transition_to_completed_writes_content_hash(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    """Completing a build adopts the server-computed content identity.

    The build-processing worker hands the manifest hash it computed
    during extraction to the very call that marks the build complete, so
    no reader can observe a ``completed`` row still carrying the
    client's tarball digest.
    """
    manifest_hash = "sha256:" + "b" * 64
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        assert build.content_hash != manifest_hash
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        completed = await build_store.transition_status(
            build_id=build.id,
            new_status=BuildStatus.completed,
            content_hash=manifest_hash,
        )
        await db_session.commit()
    assert completed.status == BuildStatus.completed
    assert completed.content_hash == manifest_hash


@pytest.mark.asyncio
async def test_transition_to_completed_without_hash_keeps_stored_value(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    """Omitting ``content_hash`` leaves the stored hash alone.

    Keeper-sync completes its builds without the argument — the copier
    already wrote the manifest hash through ``update_content_hash``
    while the row was pending — so the completing transition must not
    clobber what is already there.
    """
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        completed = await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.completed
        )
        await db_session.commit()
    assert completed.content_hash == build.content_hash


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target",
    [BuildStatus.processing, BuildStatus.failed],
)
async def test_transition_rejects_content_hash_off_completed(
    db_session: AsyncSession,
    build_store: BuildStore,
    target: BuildStatus,
) -> None:
    """Only the completing transition may carry a content hash.

    ``completed`` is the one status at which the content is known and
    final; accepting a hash alongside ``processing`` or ``failed`` would
    stamp an identity onto content that is still arriving or never
    landed at all, so the combination is refused as caller misuse.
    """
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        if target is BuildStatus.failed:
            await build_store.transition_status(
                build_id=build.id, new_status=BuildStatus.processing
            )
        with pytest.raises(ValueError, match="content_hash"):
            await build_store.transition_status(
                build_id=build.id,
                new_status=target,
                content_hash="sha256:" + "c" * 64,
            )
        await db_session.commit()

    async with db_session.begin():
        unchanged = await build_store.get_by_id(build.id)
        assert unchanged is not None
        assert unchanged.content_hash == build.content_hash


@pytest.mark.asyncio
async def test_transition_processing_to_failed(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        failed = await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.failed
        )
        await db_session.commit()
    assert failed.status == BuildStatus.failed
    assert failed.date_completed is not None


@pytest.mark.asyncio
async def test_invalid_transition_raises(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        # Cannot go directly from pending to completed
        with pytest.raises(InvalidBuildStateError):
            await build_store.transition_status(
                build_id=build.id, new_status=BuildStatus.completed
            )
        await db_session.commit()


@pytest.mark.asyncio
async def test_transition_pending_to_cancelled(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    """A build deleted before it was ever uploaded can be cancelled.

    ``cancelled`` is terminal, so it gets ``date_completed`` exactly as
    ``completed`` and ``failed`` do: the row stops being something a
    worker might still be holding.
    """
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        cancelled = await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.cancelled
        )
        await db_session.commit()
    assert cancelled.status == BuildStatus.cancelled
    assert cancelled.date_completed is not None


@pytest.mark.asyncio
async def test_transition_processing_to_superseded(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    """The stale-skip path's transition, with its completion stamp."""
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        superseded = await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.superseded
        )
        await db_session.commit()
    assert superseded.status == BuildStatus.superseded
    assert superseded.date_completed is not None


@pytest.mark.asyncio
async def test_transition_processing_to_cancelled(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    """A build deleted mid-processing lands on ``cancelled``."""
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        cancelled = await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.cancelled
        )
        await db_session.commit()
    assert cancelled.status == BuildStatus.cancelled
    assert cancelled.date_completed is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal", [BuildStatus.superseded, BuildStatus.cancelled]
)
async def test_new_terminal_statuses_reject_further_transitions(
    db_session: AsyncSession,
    build_store: BuildStore,
    terminal: BuildStatus,
) -> None:
    """Neither new status is a waypoint: nothing follows it.

    This is what lets a caller read ``superseded``/``cancelled`` as
    "this build will never be published" without also checking whether
    some later path might still revive the row.
    """
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        await build_store.transition_status(
            build_id=build.id, new_status=terminal
        )
        await db_session.commit()

    async with db_session.begin():
        for target in BuildStatus:
            with pytest.raises(InvalidBuildStateError):
                await build_store.transition_status(
                    build_id=build.id, new_status=target
                )


@pytest.mark.asyncio
async def test_list_by_project(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="user1",
        )
        await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=BuildCreate(
                git_ref="v1.0",
                content_hash="sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            ),
            uploader="user2",
        )
        result = await build_store.list_by_project(project_id, limit=25)
        await db_session.commit()
    assert len(result.entries) == 2


@pytest.mark.asyncio
async def test_get_by_public_id(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        found = await build_store.get_by_public_id(
            project_id=project_id, public_id=build.public_id
        )
        await db_session.commit()
    assert found is not None
    assert found.id == build.id


@pytest.mark.asyncio
async def test_soft_delete_build(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        deleted = await build_store.soft_delete(build_id=build.id)
        assert deleted is True
        found = await build_store.get_by_public_id(
            project_id=project_id, public_id=build.public_id
        )
        await db_session.commit()
    assert found is None


@pytest.mark.asyncio
async def test_update_content_hash_rejects_non_pending(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    """update_content_hash must refuse builds past the pending stage."""
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        with pytest.raises(InvalidBuildStateError):
            await build_store.update_content_hash(
                build_id=build.id, content_hash="sha256:" + "0" * 64
            )
        await db_session.commit()


@pytest.mark.asyncio
async def test_update_content_hash_not_found_omits_target_state_tag(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    """The not-found path is not a transition.

    It must not tag ``build_target_state``. The org / project / edition
    slugs the caller threaded down should still surface for triage.
    """
    with pytest.raises(InvalidBuildStateError) as exc_info:
        await build_store.update_content_hash(
            build_id=999_999,
            content_hash="sha256:" + "0" * 64,
            org_slug="build-org",
            project_slug="build-proj",
            edition_slug="main",
        )
    info = exc_info.value.to_sentry()
    assert "build_target_state" not in info.tags
    assert "build_current_state" not in info.tags
    assert info.tags["org_slug"] == "build-org"
    assert info.tags["project_slug"] == "build-proj"
    transition = info.contexts["build_transition"]
    assert transition["edition_slug"] == "main"
    assert transition["org_slug"] == "build-org"
    assert transition["project_slug"] == "build-proj"


@pytest.mark.asyncio
async def test_update_inventory(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        updated = await build_store.update_inventory(
            build_id=build.id, object_count=42, total_size_bytes=1024000
        )
        await db_session.commit()
    assert updated.object_count == 42
    assert updated.total_size_bytes == 1024000


@pytest.mark.asyncio
async def test_get_latest_build_id_for_ref(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    """Returns the max *live* build id for a (project, git_ref) pair.

    A second build on the same ref supersedes the first; a build on a
    different ref does not influence the answer; an unknown ref or
    project returns ``None``. Soft-deleted rows do not count: deleting
    the newest build hands the ref back to the newest live one, and a
    ref whose only build is deleted has no latest id at all.
    """
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)

        first_main = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:" + "1" * 64,
            ),
            uploader="testuser",
        )
        second_main = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:" + "2" * 64,
            ),
            uploader="testuser",
        )
        other_ref = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=BuildCreate(
                git_ref="release/v1",
                content_hash="sha256:" + "3" * 64,
            ),
            uploader="testuser",
        )
        await db_session.commit()

    async with db_session.begin():
        latest_main = await build_store.get_latest_build_id_for_ref(
            project_id=project_id, git_ref="main"
        )
        assert latest_main == second_main.id
        assert latest_main != first_main.id

        latest_release = await build_store.get_latest_build_id_for_ref(
            project_id=project_id, git_ref="release/v1"
        )
        assert latest_release == other_ref.id

        missing_ref = await build_store.get_latest_build_id_for_ref(
            project_id=project_id, git_ref="does-not-exist"
        )
        assert missing_ref is None

        missing_project = await build_store.get_latest_build_id_for_ref(
            project_id=project_id + 9999, git_ref="main"
        )
        assert missing_project is None

    async with db_session.begin():
        assert await build_store.soft_delete(build_id=second_main.id) is True
        assert await build_store.soft_delete(build_id=other_ref.id) is True
        await db_session.commit()

    async with db_session.begin():
        # A tombstone is not a supersession marker (#575): with the
        # newest row deleted, the newest live build owns the ref again
        # instead of being stranded behind a build nobody will publish.
        after_delete = await build_store.get_latest_build_id_for_ref(
            project_id=project_id, git_ref="main"
        )
        assert after_delete == first_main.id

        # A ref whose only build is deleted has no latest build at all.
        emptied_ref = await build_store.get_latest_build_id_for_ref(
            project_id=project_id, git_ref="release/v1"
        )
        assert emptied_ref is None


@pytest.mark.asyncio
async def test_public_ids_sort_in_creation_order(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    """Two builds created in succession sort by public_id in creation order.

    A short real-time gap guarantees the two mints land in distinct
    milliseconds so the time-ordered high bits establish the ordering
    independent of the random low bits.
    """
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        first = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await asyncio.sleep(0.005)
        second = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await db_session.commit()

    assert second.public_id > first.public_id


@pytest.mark.asyncio
async def test_create_retries_on_public_id_collision(
    db_session: AsyncSession,
    build_store: BuildStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A colliding public_id is re-minted with no error and no merged rows."""
    collision_id = 424242
    fresh_id = 999999

    def _fake_ids() -> Iterator[int]:
        yield from (collision_id, fresh_id)

    ids = _fake_ids()
    monkeypatch.setattr(
        "docverse_server.storage._public_id.generate_resource_id",
        lambda: next(ids),
    )

    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        # Pre-insert a build occupying ``collision_id`` and flush it into the
        # outer transaction so the retried insert races a persistent row.
        existing = SqlBuild(
            public_id=collision_id,
            project_id=project_id,
            git_ref="pre-existing",
            content_hash="sha256:0",
            status=BuildStatus.pending,
            staging_key="__staging/pre.tar.gz",
            storage_prefix="build-proj/__builds/pre/",
            uploader="pre",
        )
        db_session.add(existing)
        await db_session.flush()

        created = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await db_session.commit()

    # The retry minted the fresh id, leaving the pre-existing row untouched.
    assert created.public_id == fresh_id

    async with db_session.begin():
        total = await db_session.scalar(
            select(func.count()).select_from(SqlBuild)
        )
        preserved = await db_session.scalar(
            select(SqlBuild.git_ref).where(SqlBuild.public_id == collision_id)
        )
    assert total == 2
    assert preserved == "pre-existing"


async def _create_processing_build(
    db_session: AsyncSession,
    build_store: BuildStore,
    *,
    project_id: int,
    git_ref: str,
    uploaded_hours_ago: float,
) -> Build:
    """Create a ``processing`` build whose ``date_uploaded`` is back-dated.

    ``transition_status`` always stamps ``date_uploaded`` with the
    current time, so the age the stranded sweep filters on is written
    afterwards by direct UPDATE.
    """
    build = await build_store.create(
        project_id=project_id,
        project_slug="build-proj",
        data=BuildCreate(git_ref=git_ref, content_hash="sha256:" + "a" * 64),
        uploader="testuser",
    )
    await build_store.transition_status(
        build_id=build.id, new_status=BuildStatus.processing
    )
    await db_session.execute(
        update(SqlBuild)
        .where(SqlBuild.id == build.id)
        .values(
            date_uploaded=(
                datetime.now(tz=UTC) - timedelta(hours=uploaded_hours_ago)
            )
        )
    )
    return build


async def _seed_queue_job(
    db_session: AsyncSession,
    *,
    org_id: int,
    build_id: int,
    status: JobStatus,
) -> None:
    """Attach one ``build_processing`` queue job row to a build."""
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        kind=JobKind.build_processing.value,
        status=status.value,
        org_id=org_id,
        build_id=build_id,
    )
    db_session.add(row)
    await db_session.flush()


@pytest.mark.asyncio
async def test_fail_stranded_processing(
    db_session: AsyncSession,
    build_store: BuildStore,
) -> None:
    """Only threshold-old ``processing`` rows with no live job are swept.

    The invariant ``processing`` is supposed to carry is "a worker is
    on it". A row that no ``queued``/``in_progress`` queue job vouches
    for any more has lost its worker, so the reaper's sweep retires it
    to ``failed``. Rows a live job still covers, rows younger than the
    cutoff, soft-deleted rows and rows that never left ``pending`` are
    none of the sweep's business.
    """
    async with db_session.begin():
        org_id, project_id = await _create_org_and_project(db_session)
        stranded = await _create_processing_build(
            db_session,
            build_store,
            project_id=project_id,
            git_ref="stranded",
            uploaded_hours_ago=24,
        )
        # A terminal job is no vouch either: the silent sweep failing a
        # job is exactly how a build becomes strandable.
        job_failed = await _create_processing_build(
            db_session,
            build_store,
            project_id=project_id,
            git_ref="job-failed",
            uploaded_hours_ago=24,
        )
        await _seed_queue_job(
            db_session,
            org_id=org_id,
            build_id=job_failed.id,
            status=JobStatus.failed,
        )
        in_progress = await _create_processing_build(
            db_session,
            build_store,
            project_id=project_id,
            git_ref="in-progress",
            uploaded_hours_ago=24,
        )
        await _seed_queue_job(
            db_session,
            org_id=org_id,
            build_id=in_progress.id,
            status=JobStatus.in_progress,
        )
        queued = await _create_processing_build(
            db_session,
            build_store,
            project_id=project_id,
            git_ref="queued",
            uploaded_hours_ago=24,
        )
        await _seed_queue_job(
            db_session,
            org_id=org_id,
            build_id=queued.id,
            status=JobStatus.queued,
        )
        fresh = await _create_processing_build(
            db_session,
            build_store,
            project_id=project_id,
            git_ref="fresh",
            uploaded_hours_ago=0,
        )
        deleted = await _create_processing_build(
            db_session,
            build_store,
            project_id=project_id,
            git_ref="deleted",
            uploaded_hours_ago=24,
        )
        assert await build_store.soft_delete(build_id=deleted.id) is True
        pending = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=BuildCreate(
                git_ref="pending", content_hash="sha256:" + "b" * 64
            ),
            uploader="testuser",
        )
        await db_session.commit()

    async with db_session.begin():
        reaped = await build_store.fail_stranded_processing(
            older_than=datetime.now(tz=UTC) - timedelta(hours=8)
        )
        await db_session.commit()

    assert {build.id for build in reaped} == {stranded.id, job_failed.id}
    assert all(build.status == BuildStatus.failed for build in reaped)
    assert all(build.date_completed is not None for build in reaped)

    async with db_session.begin():
        for spared in (in_progress, queued, fresh, deleted):
            row = await build_store.get_by_id(spared.id)
            assert row is not None
            assert row.status == BuildStatus.processing
            assert row.date_completed is None
        pending_row = await build_store.get_by_id(pending.id)
        assert pending_row is not None
        assert pending_row.status == BuildStatus.pending
