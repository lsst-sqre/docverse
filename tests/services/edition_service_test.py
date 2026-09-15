"""Tests for :class:`~docverse_server.services.edition.EditionService`.

Focused on the lock order the edition PATCH shares with the rest of the
tree (PRD #634, review of PR #643): a PATCH that carries both an edition
metadata field *and* ``build`` writes two tables, and it has to reach
them in the one order ``ProjectStore.soft_delete`` cascades in —
projects, then editions, then builds.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress

import pytest
import structlog
from fastapi import FastAPI
from safir.arq import MockArqQueue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from docverse.models import (
    BuildCreate,
    EditionKind,
    EditionUpdate,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse_server.config import Configuration
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.edition import DEFAULT_EDITION_SLUG
from docverse_server.factory import Factory
from docverse_server.services.edition import EditionService
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.keeper_sync import TombstoneReason
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from tests.support.rowlocks import (
    LOCK_WAIT_TIMEOUT,
    backend_pid,
    wait_until_blocked_or_finished,
)

_HASH = "sha256:" + "c" * 64
_config = Configuration()


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _edition_service(db_session: AsyncSession) -> EditionService:
    factory = Factory(
        session=db_session,
        logger=_logger(),
        arq_queue=MockArqQueue(),
        default_queue_name=_config.arq_queue_name,
    )
    return factory.create_edition_service()


async def _seed(db_session: AsyncSession) -> tuple[int, int, str]:
    """Insert an org, a project, its ``__main`` edition, and a build.

    Returns ``(org_id, edition_id, build_public_id)``.
    """
    logger = _logger()
    org = await OrganizationStore(session=db_session, logger=logger).create(
        OrganizationCreate(
            slug="es-org",
            title="ES Org",
            base_domain="es-org.example.com",
        )
    )
    project = await ProjectStore(session=db_session, logger=logger).create(
        org_id=org.id,
        data=ProjectCreate(
            slug="es-proj",
            title="ES Project",
            source_url="https://example.com/example/es",
        ),
    )
    edition = await EditionStore(
        session=db_session, logger=logger
    ).create_internal(
        project_id=project.id,
        slug=DEFAULT_EDITION_SLUG,
        title="Main",
        kind=EditionKind.main,
        tracking_mode=TrackingMode.git_ref,
        tracking_params={"git_ref": "main"},
    )
    build = await BuildStore(session=db_session, logger=logger).create(
        project_id=project.id,
        project_slug=project.slug,
        data=BuildCreate(git_ref="main", content_hash=_HASH),
        uploader="testuser",
    )
    return org.id, edition.id, serialize_base32_id(build.public_id)


@pytest.mark.asyncio
async def test_update_with_metadata_and_build_does_not_deadlock(
    app: FastAPI,
    db_session: AsyncSession,
    db_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PATCH carrying metadata *and* ``build`` never deadlocks a DELETE.

    ``EditionService.update`` writes two rows in two different tables,
    and a build-only PATCH emits no edition ``UPDATE`` at all — which is
    why every other test of this path passes whichever order the two are
    applied in. With the metadata update flushed first, the PATCH held
    the edition row and then asked for the project row, while a
    concurrent ``DELETE /orgs/{org}/projects/{project}`` held the
    project row and was cascading toward that same edition. PostgreSQL
    aborts one of them, and neither the DELETE handler nor the PATCH has
    anywhere to retry.

    Applying the build override first puts the project row ahead of the
    edition row on this path too, so the DELETE simply waits.
    """
    async with db_session.begin():
        org_id, edition_id, build_public_id = await _seed(db_session)
        await db_session.commit()

    # Park the PATCH immediately after its metadata write, the point at
    # which the old order was already holding the edition row.
    at_metadata = asyncio.Event()
    release_metadata = asyncio.Event()
    update_edition = EditionStore.update

    async def paused_update(
        self: EditionStore, **kwargs: object
    ) -> object | None:
        result = await update_edition(self, **kwargs)  # type: ignore[arg-type]
        at_metadata.set()
        await release_metadata.wait()
        return result

    monkeypatch.setattr(EditionStore, "update", paused_update)

    async with (
        db_session_factory() as patch_session,
        db_session_factory() as delete_session,
        db_session_factory() as probe,
    ):
        delete_pid = await backend_pid(delete_session)

        async def run_patch() -> None:
            service = _edition_service(patch_session)
            await service.update(
                org_slug="es-org",
                project_slug="es-proj",
                slug=DEFAULT_EDITION_SLUG,
                data=EditionUpdate(title="Renamed", build=build_public_id),
            )
            await patch_session.commit()

        async def run_delete() -> None:
            store = ProjectStore(session=delete_session, logger=_logger())
            await store.soft_delete(
                org_id=org_id,
                slug="es-proj",
                reason=TombstoneReason.manual_delete,
            )
            await delete_session.commit()

        patching = asyncio.ensure_future(run_patch())
        deleting: asyncio.Task[None] | None = None
        parked = False
        try:
            await asyncio.wait_for(
                at_metadata.wait(), timeout=LOCK_WAIT_TIMEOUT
            )
            deleting = asyncio.ensure_future(run_delete())
            parked = await wait_until_blocked_or_finished(
                probe, pid=delete_pid, task=deleting
            )
            release_metadata.set()
            await patching
            await deleting
        finally:
            release_metadata.set()
            for task in (patching, deleting):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            await patch_session.rollback()
            await delete_session.rollback()

    # The DELETE waited on the project row the PATCH took first.
    assert parked

    async with db_session_factory() as reader:
        row = (
            await reader.execute(
                select(SqlEdition).where(SqlEdition.id == edition_id)
            )
        ).scalar_one()
        # Both sides landed: the PATCH's title and build, then the
        # DELETE's cascade over the top.
        assert row.title == "Renamed"
        assert row.current_build_id is not None
        assert row.date_deleted is not None
