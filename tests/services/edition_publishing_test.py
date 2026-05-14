"""Tests for EditionPublishingService."""

from __future__ import annotations

from types import TracebackType
from typing import Self

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    BuildCreate,
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    OrganizationUpdate,
    ProjectCreate,
    TrackingMode,
)
from docverse.client.models.queue_enums import PublishStatus
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.edition_build_history import EditionBuildHistory
from docverse.services.edition_publishing import EditionPublishingService
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.editionpublisher import (
    EditionPublisher,
    MockEditionPublisher,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

_HASH = "sha256:" + "a" * 64
_PROJECT_SLUG = "pub-proj"


class _FailingPublisher:
    """An EditionPublisher whose ``publish`` raises."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    async def publish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
        build_public_id: str,
        object_key_prefix: str,
    ) -> None:
        _ = (project_slug, edition_slug, build_public_id, object_key_prefix)
        raise self._exc

    async def unpublish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
    ) -> None:
        _ = (project_slug, edition_slug)
        raise self._exc


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _make_service(
    db_session: AsyncSession,
    *,
    publisher: EditionPublisher | None = None,
    provider_raises: bool = False,
) -> EditionPublishingService:
    async def provider(*, org_id: int, service_label: str) -> EditionPublisher:
        if provider_raises:
            msg = (
                "publisher_provider should not be called "
                f"(org_id={org_id}, service_label={service_label})"
            )
            raise AssertionError(msg)
        if publisher is None:
            msg = "No publisher configured for provider call"
            raise AssertionError(msg)
        return publisher

    logger = _logger()
    return EditionPublishingService(
        org_store=OrganizationStore(session=db_session, logger=logger),
        edition_store=EditionStore(session=db_session, logger=logger),
        history_store=EditionBuildHistoryStore(
            session=db_session, logger=logger
        ),
        publisher_provider=provider,
        logger=logger,
    )


async def _setup(
    db_session: AsyncSession,
    *,
    org_slug: str,
    cdn_service_label: str | None = None,
) -> tuple[int, Edition, Build, EditionBuildHistory]:
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    history_store = EditionBuildHistoryStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)

    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title="Publish Org",
            base_domain=f"{org_slug}.example.com",
        )
    )
    if cdn_service_label is not None:
        await org_store.update(
            org_slug,
            OrganizationUpdate(cdn_service_label=cdn_service_label),
        )
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug=_PROJECT_SLUG,
            title="Publish Project",
            doc_repo="https://github.com/example/repo",
        ),
    )
    edition = await edition_store.create(
        project_id=project.id,
        data=EditionCreate(
            slug="main",
            title="Latest",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "main"},
        ),
    )
    build = await build_store.create(
        project_id=project.id,
        data=BuildCreate(git_ref="main", content_hash=_HASH),
        uploader="testuser",
        project_slug=_PROJECT_SLUG,
    )
    history_entry = await history_store.record(
        edition_id=edition.id, build_id=build.id
    )
    return org.id, edition, build, history_entry


async def _fetch_edition(
    db_session: AsyncSession, edition_id: int, project_id: int, slug: str
) -> Edition:
    _ = edition_id
    store = EditionStore(session=db_session, logger=_logger())
    edition = await store.get_by_slug(project_id=project_id, slug=slug)
    assert edition is not None
    return edition


async def _fetch_history(
    db_session: AsyncSession, edition_id: int
) -> EditionBuildHistory:
    store = EditionBuildHistoryStore(session=db_session, logger=_logger())
    entries = await store.list_by_edition(edition_id)
    assert entries, "no history entry found"
    return entries[0]


@pytest.mark.asyncio
async def test_publish_no_cdn_marks_published_without_publisher(
    db_session: AsyncSession,
) -> None:
    """Org with cdn_service_label=None is marked published as a no-op."""
    service = _make_service(db_session, provider_raises=True)
    async with db_session.begin():
        org_id, edition, build, history_entry = await _setup(
            db_session, org_slug="no-cdn-org"
        )
        await service.publish(
            org_id=org_id,
            project_slug=_PROJECT_SLUG,
            edition=edition,
            build=build,
            history_entry=history_entry,
        )
        await db_session.commit()

    async with db_session.begin():
        refreshed = await _fetch_edition(
            db_session, edition.id, edition.project_id, edition.slug
        )
        assert refreshed.publish_status == PublishStatus.published
        refreshed_history = await _fetch_history(db_session, edition.id)
        assert refreshed_history.publish_status == PublishStatus.published


@pytest.mark.asyncio
async def test_publish_failure_propagates_and_leaves_status_unchanged(
    db_session: AsyncSession,
) -> None:
    """Publisher errors bubble up; rows are not marked published."""
    boom = RuntimeError("publisher exploded")
    failing_publisher = _FailingPublisher(boom)
    service = _make_service(db_session, publisher=failing_publisher)

    async with db_session.begin():
        org_id, edition, build, history_entry = await _setup(
            db_session,
            org_slug="fail-org",
            cdn_service_label="cdn-prod",
        )
        with pytest.raises(RuntimeError, match="publisher exploded"):
            await service.publish(
                org_id=org_id,
                project_slug=_PROJECT_SLUG,
                edition=edition,
                build=build,
                history_entry=history_entry,
            )
        await db_session.commit()

    async with db_session.begin():
        refreshed = await _fetch_edition(
            db_session, edition.id, edition.project_id, edition.slug
        )
        assert refreshed.publish_status is None
        refreshed_history = await _fetch_history(db_session, edition.id)
        assert refreshed_history.publish_status is None


@pytest.mark.asyncio
async def test_unpublish_no_cdn_is_a_noop(
    db_session: AsyncSession,
) -> None:
    """Org with cdn_service_label=None: unpublish does not call publisher."""
    service = _make_service(db_session, provider_raises=True)
    async with db_session.begin():
        org_id, edition, _build, _history_entry = await _setup(
            db_session, org_slug="no-cdn-unpub-org"
        )
        # Must not raise even though provider would AssertionError if called.
        await service.unpublish(
            org_id=org_id,
            project_slug=_PROJECT_SLUG,
            edition_slug=edition.slug,
        )
        await db_session.commit()


@pytest.mark.asyncio
async def test_unpublish_calls_publisher_when_cdn_configured(
    db_session: AsyncSession,
) -> None:
    """Configured CDN: unpublish records one call on the mock publisher."""
    mock_publisher = MockEditionPublisher()
    service = _make_service(db_session, publisher=mock_publisher)
    async with db_session.begin():
        org_id, edition, _build, _history_entry = await _setup(
            db_session,
            org_slug="cdn-unpub-org",
            cdn_service_label="cdn-prod",
        )
        await service.unpublish(
            org_id=org_id,
            project_slug=_PROJECT_SLUG,
            edition_slug=edition.slug,
        )
        await db_session.commit()

    assert len(mock_publisher.unpublish_calls) == 1
    call = mock_publisher.unpublish_calls[0]
    assert call.project_slug == _PROJECT_SLUG
    assert call.edition_slug == edition.slug


@pytest.mark.asyncio
async def test_publish_successful_calls_publisher_and_marks_published(
    db_session: AsyncSession,
) -> None:
    """Successful publish updates both rows and records one publish call."""
    mock_publisher = MockEditionPublisher()
    service = _make_service(db_session, publisher=mock_publisher)
    async with db_session.begin():
        org_id, edition, build, history_entry = await _setup(
            db_session,
            org_slug="pub-org",
            cdn_service_label="cdn-prod",
        )
        await service.publish(
            org_id=org_id,
            project_slug=_PROJECT_SLUG,
            edition=edition,
            build=build,
            history_entry=history_entry,
        )
        await db_session.commit()

    assert len(mock_publisher.calls) == 1
    call = mock_publisher.calls[0]
    assert call.project_slug == _PROJECT_SLUG
    assert call.edition_slug == edition.slug
    assert call.build_public_id == serialize_base32_id(build.public_id)
    assert call.object_key_prefix == build.storage_prefix

    async with db_session.begin():
        refreshed = await _fetch_edition(
            db_session, edition.id, edition.project_id, edition.slug
        )
        assert refreshed.publish_status == PublishStatus.published
        refreshed_history = await _fetch_history(db_session, edition.id)
        assert refreshed_history.publish_status == PublishStatus.published
