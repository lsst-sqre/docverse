"""Tests for EditionTrackingService."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import (
    BuildCreate,
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.dbschema.build import SqlBuild
from docverse.dbschema.organization import SqlOrganization
from docverse.dbschema.project import SqlProject
from docverse.domain.build import Build
from docverse.domain.organization import Organization
from docverse.domain.project import Project
from docverse.services.edition_tracking import EditionTrackingService
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

_HASH = "sha256:" + "a" * 64


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _make_service(
    db_session: async_scoped_session[AsyncSession],
) -> EditionTrackingService:
    logger = _logger()
    return EditionTrackingService(
        edition_store=EditionStore(session=db_session, logger=logger),
        history_store=EditionBuildHistoryStore(
            session=db_session, logger=logger
        ),
        project_store=ProjectStore(session=db_session, logger=logger),
        org_store=OrganizationStore(session=db_session, logger=logger),
        logger=logger,
    )


async def _setup(
    db_session: async_scoped_session[AsyncSession],
    *,
    org_slug: str = "track-org",
    org_slug_rewrite_rules: list[dict[str, Any]] | None = None,
) -> tuple[Organization, Project]:
    """Create an org and project, returning both."""
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title="Track Org",
            base_domain=f"{org_slug}.example.com",
        )
    )
    # Set slug_rewrite_rules directly on the SQL row because the
    # Pydantic model types the field as dict but the DB stores a list.
    if org_slug_rewrite_rules is not None:
        await db_session.execute(
            update(SqlOrganization)
            .where(SqlOrganization.id == org.id)
            .values(slug_rewrite_rules=org_slug_rewrite_rules)
        )
        await db_session.flush()
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug="track-proj",
            title="Track Project",
            doc_repo="https://github.com/example/repo",
        ),
    )
    return org, project


async def _create_build(
    db_session: async_scoped_session[AsyncSession],
    project_id: int,
    *,
    git_ref: str = "main",
    alternate_name: str | None = None,
) -> Build:
    logger = _logger()
    build_store = BuildStore(session=db_session, logger=logger)
    return await build_store.create(
        project_id=project_id,
        data=BuildCreate(
            git_ref=git_ref,
            alternate_name=alternate_name,
            content_hash=_HASH,
        ),
        uploader="testuser",
    )


@pytest.mark.asyncio
async def test_track_build_auto_creates_edition(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Auto-create an edition when no match exists."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session)
        build = await _create_build(db_session, project.id)

        result = await service.track_build(build)
        await db_session.commit()

    assert result.derived_slug == "main"
    assert result.suppressed is False
    assert len(result.outcomes) == 1
    outcome = result.outcomes[0]
    assert outcome.action == "created"
    assert outcome.slug == "main"
    assert outcome.build_id == build.id

    # Verify edition exists in DB
    async with db_session.begin():
        edition_store = EditionStore(session=db_session, logger=_logger())
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="main"
        )
        assert edition is not None
        assert edition.kind == EditionKind.draft
        assert edition.tracking_mode == TrackingMode.git_ref
        assert edition.tracking_params == {"git_ref": "main"}
        assert edition.current_build_id == build.id

        # Verify history recorded
        history_store = EditionBuildHistoryStore(
            session=db_session, logger=_logger()
        )
        history = await history_store.list_by_edition(edition.id)
        assert len(history) == 1
        assert history[0].build_id == build.id
        assert history[0].position == 1
        await db_session.commit()


@pytest.mark.asyncio
async def test_track_build_updates_existing_edition(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Update an existing edition's pointer."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="upd-org")
        edition_store = EditionStore(session=db_session, logger=_logger())
        await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug="main",
                title="Latest",
                kind=EditionKind.main,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )
        build = await _create_build(db_session, project.id)

        result = await service.track_build(build)
        await db_session.commit()

    assert len(result.outcomes) == 1
    assert result.outcomes[0].action == "updated"
    assert result.outcomes[0].slug == "main"


@pytest.mark.asyncio
async def test_track_build_stale_skipped(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Stale build is skipped and no history is recorded."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="stale-org")
        edition_store = EditionStore(session=db_session, logger=_logger())
        edition = await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug="main",
                title="Latest",
                kind=EditionKind.main,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )

        # Create newer and older builds
        newer_build = await _create_build(db_session, project.id)
        older_build = await _create_build(db_session, project.id)

        # Set controlled timestamps
        for bid, ts in [
            (newer_build.id, datetime(2025, 6, 1, tzinfo=UTC)),
            (older_build.id, datetime(2025, 1, 1, tzinfo=UTC)),
        ]:
            row = (
                await db_session.execute(
                    select(SqlBuild).where(SqlBuild.id == bid)
                )
            ).scalar_one()
            row.date_created = ts
        await db_session.flush()

        # Point edition to newer build first
        await edition_store.set_current_build(
            edition_id=edition.id, build_id=newer_build.id
        )

        # Now track the older build — should be skipped
        result = await service.track_build(older_build)
        await db_session.commit()

    assert len(result.outcomes) == 1
    assert result.outcomes[0].action == "skipped"

    # Verify no history was recorded for the stale build
    async with db_session.begin():
        history_store = EditionBuildHistoryStore(
            session=db_session, logger=_logger()
        )
        history = await history_store.list_by_edition(edition.id)
        assert len(history) == 0
        await db_session.commit()


@pytest.mark.asyncio
async def test_track_build_ignore_rule(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Ignore rule suppresses edition tracking entirely."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(
            db_session,
            org_slug="ignore-org",
            org_slug_rewrite_rules=[
                {"type": "ignore", "glob": "dependabot/*"}
            ],
        )
        build = await _create_build(
            db_session, project.id, git_ref="dependabot/bump-foo"
        )

        result = await service.track_build(build)
        await db_session.commit()

    assert result.suppressed is True
    assert result.derived_slug is None
    assert len(result.outcomes) == 0

    # Verify no editions were created
    async with db_session.begin():
        edition_store = EditionStore(session=db_session, logger=_logger())
        editions = await edition_store.find_matching_editions(
            project_id=project.id, git_ref="dependabot/bump-foo"
        )
        assert len(editions) == 0
        await db_session.commit()


@pytest.mark.asyncio
async def test_track_build_alternate_name(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Alternate name produces compound slug and alternate edition."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="alt-org")
        build = await _create_build(
            db_session, project.id, git_ref="main", alternate_name="usdf-dev"
        )

        result = await service.track_build(build)
        await db_session.commit()

    assert result.derived_slug == "usdf-dev--main"
    assert len(result.outcomes) == 1
    assert result.outcomes[0].action == "created"
    assert result.outcomes[0].slug == "usdf-dev--main"

    # Verify edition properties
    async with db_session.begin():
        edition_store = EditionStore(session=db_session, logger=_logger())
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="usdf-dev--main"
        )
        assert edition is not None
        assert edition.tracking_mode == TrackingMode.alternate_git_ref
        assert edition.tracking_params == {
            "git_ref": "main",
            "alternate_name": "usdf-dev",
        }
        await db_session.commit()


@pytest.mark.asyncio
async def test_track_build_invalid_slug(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Invalid slug derivation is handled gracefully."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="invalid-org")
        # A git ref starting with "__" produces an invalid slug
        build = await _create_build(
            db_session, project.id, git_ref="__reserved"
        )

        result = await service.track_build(build)
        await db_session.commit()

    assert result.derived_slug is None
    assert result.suppressed is False
    assert len(result.outcomes) == 0


@pytest.mark.asyncio
async def test_track_build_multiple_matches(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Multiple matching editions all get updated."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="multi-org")
        edition_store = EditionStore(session=db_session, logger=_logger())

        # Two editions both tracking "main"
        await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug="main",
                title="Latest",
                kind=EditionKind.main,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )
        await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug="main-mirror",
                title="Main Mirror",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )

        build = await _create_build(db_session, project.id)
        result = await service.track_build(build)
        await db_session.commit()

    assert len(result.outcomes) == 2
    assert all(o.action == "updated" for o in result.outcomes)
    slugs = {o.slug for o in result.outcomes}
    assert slugs == {"main", "main-mirror"}

    # Verify history for both
    async with db_session.begin():
        history_store = EditionBuildHistoryStore(
            session=db_session, logger=_logger()
        )
        for outcome in result.outcomes:
            history = await history_store.list_by_edition(outcome.edition_id)
            assert len(history) == 1
            assert history[0].build_id == build.id
        await db_session.commit()


@pytest.mark.asyncio
async def test_track_build_project_rules_override_org(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Project-level rules completely replace org-level rules."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(
            db_session,
            org_slug="override-org",
            # Org rule: strip "tags/" prefix, edition kind = release
            org_slug_rewrite_rules=[
                {
                    "type": "prefix_strip",
                    "prefix": "tags/",
                    "edition_kind": "release",
                }
            ],
        )
        # Override with project-level rule that ignores "tags/*"
        await db_session.execute(
            update(SqlProject)
            .where(SqlProject.id == project.id)
            .values(
                slug_rewrite_rules=[
                    {"type": "ignore", "glob": "tags/*"},
                ]
            )
        )
        await db_session.flush()

        build = await _create_build(
            db_session, project.id, git_ref="tags/v1.0"
        )
        result = await service.track_build(build)
        await db_session.commit()

    # Project rule ignores "tags/*", so suppressed
    assert result.suppressed is True
    assert result.derived_slug is None
    assert len(result.outcomes) == 0


@pytest.mark.asyncio
async def test_track_build_default_fallback_no_rules(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """No rules configured: default slash-to-hyphen fallback."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="norule-org")
        build = await _create_build(
            db_session, project.id, git_ref="feature/cool-thing"
        )

        result = await service.track_build(build)
        await db_session.commit()

    assert result.derived_slug == "feature-cool-thing"
    assert len(result.outcomes) == 1
    assert result.outcomes[0].action == "created"
    assert result.outcomes[0].slug == "feature-cool-thing"


@pytest.mark.asyncio
async def test_track_build_auto_create_race_guard(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Use existing edition instead of creating a duplicate."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="race-org")
        edition_store = EditionStore(session=db_session, logger=_logger())

        # Pre-create the edition that auto-create would make
        existing = await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug="main",
                title="Pre-existing",
                kind=EditionKind.main,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )

        build = await _create_build(db_session, project.id)
        result = await service.track_build(build)
        await db_session.commit()

    # The existing edition is found by find_matching_editions, so
    # it's treated as an existing edition update, not an auto-create.
    assert len(result.outcomes) == 1
    assert result.outcomes[0].action == "updated"
    assert result.outcomes[0].edition_id == existing.id
