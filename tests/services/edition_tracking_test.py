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


# ── Version-based tracking tests ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_track_build_semver_release(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Pre-created semver_release edition updated by stable tag."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="semver-org")
        edition_store = EditionStore(session=db_session, logger=_logger())
        await edition_store.create_internal(
            project_id=project.id,
            slug="latest",
            title="Latest Release",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.semver_release,
        )

        build = await _create_build(db_session, project.id, git_ref="v1.0.0")
        result = await service.track_build(build)
        await db_session.commit()

    # Should match the semver_release edition + auto-created major/minor
    updated_slugs = {o.slug for o in result.outcomes if o.action != "skipped"}
    assert "latest" in updated_slugs


@pytest.mark.asyncio
async def test_track_build_semver_prerelease_skipped(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Prerelease tags do NOT match semver_release/major/minor editions."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="pre-org")
        edition_store = EditionStore(session=db_session, logger=_logger())
        await edition_store.create_internal(
            project_id=project.id,
            slug="latest",
            title="Latest Release",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.semver_release,
        )

        build = await _create_build(
            db_session, project.id, git_ref="v2.0.0-rc.1"
        )
        result = await service.track_build(build)
        await db_session.commit()

    # No version-based editions should be matched/updated
    version_outcomes = [
        o for o in result.outcomes if o.slug in ("latest", "2", "2.0")
    ]
    assert len(version_outcomes) == 0


@pytest.mark.asyncio
async def test_track_build_semver_major_auto_create(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """v2.0.0 auto-creates slug '2' with kind major."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="major-org")
        build = await _create_build(db_session, project.id, git_ref="v2.0.0")
        await service.track_build(build)
        await db_session.commit()

    # Check auto-created major edition
    async with db_session.begin():
        edition_store = EditionStore(session=db_session, logger=_logger())
        major = await edition_store.get_by_slug(
            project_id=project.id, slug="2"
        )
        assert major is not None
        assert major.kind == EditionKind.major
        assert major.tracking_mode == TrackingMode.semver_major
        assert major.tracking_params == {"major_version": 2}
        assert major.current_build_id == build.id
        await db_session.commit()


@pytest.mark.asyncio
async def test_track_build_semver_major_update(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Existing major edition updated by newer patch in same stream."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(
            db_session,
            org_slug="majupd-org",
            org_slug_rewrite_rules=[
                {
                    "type": "prefix_strip",
                    "prefix": "v",
                    "edition_kind": "release",
                }
            ],
        )
        edition_store = EditionStore(session=db_session, logger=_logger())
        await edition_store.create_internal(
            project_id=project.id,
            slug="2",
            title="Latest 2.x",
            kind=EditionKind.major,
            tracking_mode=TrackingMode.semver_major,
            tracking_params={"major_version": 2},
        )

        build_v200 = await _create_build(
            db_session, project.id, git_ref="v2.0.0"
        )
        result1 = await service.track_build(build_v200)

        build_v210 = await _create_build(
            db_session, project.id, git_ref="v2.1.0"
        )
        result2 = await service.track_build(build_v210)
        await db_session.commit()

    # Both should have updated the major edition
    major_outcomes_1 = [o for o in result1.outcomes if o.slug == "2"]
    assert len(major_outcomes_1) == 1
    assert major_outcomes_1[0].action == "updated"

    major_outcomes_2 = [o for o in result2.outcomes if o.slug == "2"]
    assert len(major_outcomes_2) == 1
    assert major_outcomes_2[0].action == "updated"

    # Verify edition points to newer build
    async with db_session.begin():
        edition_store = EditionStore(session=db_session, logger=_logger())
        major = await edition_store.get_by_slug(
            project_id=project.id, slug="2"
        )
        assert major is not None
        assert major.current_build_id == build_v210.id
        await db_session.commit()


@pytest.mark.asyncio
async def test_track_build_semver_minor_auto_create(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """v2.1.0 auto-creates slug '2.1' with kind minor."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="minor-org")
        build = await _create_build(db_session, project.id, git_ref="v2.1.0")
        await service.track_build(build)
        await db_session.commit()

    async with db_session.begin():
        edition_store = EditionStore(session=db_session, logger=_logger())
        minor = await edition_store.get_by_slug(
            project_id=project.id, slug="2.1"
        )
        assert minor is not None
        assert minor.kind == EditionKind.minor
        assert minor.tracking_mode == TrackingMode.semver_minor
        assert minor.tracking_params == {
            "major_version": 2,
            "minor_version": 1,
        }
        assert minor.current_build_id == build.id
        await db_session.commit()


@pytest.mark.asyncio
async def test_track_build_eups_major(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """EUPS major: v12_0 updates, v11_0 skipped by version guard."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="eups-org")
        edition_store = EditionStore(session=db_session, logger=_logger())
        await edition_store.create_internal(
            project_id=project.id,
            slug="eups-latest",
            title="EUPS Latest",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.eups_major_release,
        )

        build_v12 = await _create_build(
            db_session, project.id, git_ref="v12_0"
        )
        result1 = await service.track_build(build_v12)

        build_v11 = await _create_build(
            db_session, project.id, git_ref="v11_0"
        )
        result2 = await service.track_build(build_v11)
        await db_session.commit()

    eups1 = [o for o in result1.outcomes if o.slug == "eups-latest"]
    assert len(eups1) == 1
    assert eups1[0].action == "updated"

    eups2 = [o for o in result2.outcomes if o.slug == "eups-latest"]
    assert len(eups2) == 1
    assert eups2[0].action == "skipped"


@pytest.mark.asyncio
async def test_track_build_eups_weekly(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """EUPS weekly: w_2024_05 updates, w_2024_04 skipped."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="weekly-org")
        edition_store = EditionStore(session=db_session, logger=_logger())
        await edition_store.create_internal(
            project_id=project.id,
            slug="weekly",
            title="Weekly",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.eups_weekly_release,
        )

        build_w05 = await _create_build(
            db_session, project.id, git_ref="w_2024_05"
        )
        result1 = await service.track_build(build_w05)

        build_w04 = await _create_build(
            db_session, project.id, git_ref="w_2024_04"
        )
        result2 = await service.track_build(build_w04)
        await db_session.commit()

    w1 = [o for o in result1.outcomes if o.slug == "weekly"]
    assert len(w1) == 1
    assert w1[0].action == "updated"

    w2 = [o for o in result2.outcomes if o.slug == "weekly"]
    assert len(w2) == 1
    assert w2[0].action == "skipped"


@pytest.mark.asyncio
async def test_track_build_eups_daily(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """EUPS daily: d_2024_01_15 updates, d_2024_01_14 skipped."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="daily-org")
        edition_store = EditionStore(session=db_session, logger=_logger())
        await edition_store.create_internal(
            project_id=project.id,
            slug="daily",
            title="Daily",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.eups_daily_release,
        )

        build_d15 = await _create_build(
            db_session, project.id, git_ref="d_2024_01_15"
        )
        result1 = await service.track_build(build_d15)

        build_d14 = await _create_build(
            db_session, project.id, git_ref="d_2024_01_14"
        )
        result2 = await service.track_build(build_d14)
        await db_session.commit()

    d1 = [o for o in result1.outcomes if o.slug == "daily"]
    assert len(d1) == 1
    assert d1[0].action == "updated"

    d2 = [o for o in result2.outcomes if o.slug == "daily"]
    assert len(d2) == 1
    assert d2[0].action == "skipped"


@pytest.mark.asyncio
async def test_track_build_lsst_doc(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """lsst_doc: v1.0 updates, v0.9 skipped by version guard."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="lsst-org")
        edition_store = EditionStore(session=db_session, logger=_logger())
        await edition_store.create_internal(
            project_id=project.id,
            slug="current",
            title="Current",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.lsst_doc,
        )

        build_v10 = await _create_build(db_session, project.id, git_ref="v1.0")
        result1 = await service.track_build(build_v10)

        build_v09 = await _create_build(db_session, project.id, git_ref="v0.9")
        result2 = await service.track_build(build_v09)
        await db_session.commit()

    lsst1 = [o for o in result1.outcomes if o.slug == "current"]
    assert len(lsst1) == 1
    assert lsst1[0].action == "updated"

    lsst2 = [o for o in result2.outcomes if o.slug == "current"]
    assert len(lsst2) == 1
    assert lsst2[0].action == "skipped"


@pytest.mark.asyncio
async def test_track_build_lsst_doc_main_fallback(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """lsst_doc: main accepted for unpublished, version upgrades from main."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="lsstmain-org")
        edition_store = EditionStore(session=db_session, logger=_logger())
        await edition_store.create_internal(
            project_id=project.id,
            slug="current",
            title="Current",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.lsst_doc,
        )

        # main accepted when unpublished
        main_build = await _create_build(
            db_session, project.id, git_ref="main"
        )
        result1 = await service.track_build(main_build)
        await db_session.commit()

    lsst1 = [o for o in result1.outcomes if o.slug == "current"]
    assert len(lsst1) == 1
    assert lsst1[0].action == "updated"

    # Version tag upgrades from main
    async with db_session.begin():
        version_build = await _create_build(
            db_session, project.id, git_ref="v1.0"
        )
        result2 = await service.track_build(version_build)
        await db_session.commit()

    lsst2 = [o for o in result2.outcomes if o.slug == "current"]
    assert len(lsst2) == 1
    assert lsst2[0].action == "updated"

    # Verify edition now points to version build
    async with db_session.begin():
        edition_store = EditionStore(session=db_session, logger=_logger())
        ed = await edition_store.get_by_slug(
            project_id=project.id, slug="current"
        )
        assert ed is not None
        assert ed.current_build_id == version_build.id

        # main should now be rejected (version → main)
        main_build2 = await _create_build(
            db_session, project.id, git_ref="main"
        )
        result3 = await service.track_build(main_build2)
        await db_session.commit()

    lsst3 = [o for o in result3.outcomes if o.slug == "current"]
    # main doesn't even match find_matching_editions when showing version
    assert len(lsst3) == 0


@pytest.mark.asyncio
async def test_track_build_lsst_doc_main_stale_skipped(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """lsst_doc main→main: stale build skipped by date guard."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(
            db_session, org_slug="lsst-main-stale-org"
        )
        edition_store = EditionStore(session=db_session, logger=_logger())
        edition = await edition_store.create_internal(
            project_id=project.id,
            slug="current",
            title="Current",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.lsst_doc,
        )

        # Create two main builds with controlled timestamps
        build_new = await _create_build(db_session, project.id, git_ref="main")
        build_old = await _create_build(db_session, project.id, git_ref="main")
        for bid, ts in [
            (build_new.id, datetime(2025, 6, 1, tzinfo=UTC)),
            (build_old.id, datetime(2025, 1, 1, tzinfo=UTC)),
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
            edition_id=edition.id, build_id=build_new.id
        )

        # Track older main build — should be skipped by date guard
        result = await service.track_build(build_old)
        await db_session.commit()

    outcomes = [o for o in result.outcomes if o.slug == "current"]
    assert len(outcomes) == 1
    assert outcomes[0].action == "skipped"

    # Verify edition still points to newer build
    async with db_session.begin():
        edition_store = EditionStore(session=db_session, logger=_logger())
        ed = await edition_store.get_by_slug(
            project_id=project.id, slug="current"
        )
        assert ed is not None
        assert ed.current_build_id == build_new.id
        await db_session.commit()


@pytest.mark.asyncio
async def test_track_build_multi_mode_match(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """One semver build updates git_ref + semver_release + major + minor."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(
            db_session,
            org_slug="multi-mode-org",
            org_slug_rewrite_rules=[
                {
                    "type": "prefix_strip",
                    "prefix": "v",
                    "edition_kind": "release",
                }
            ],
        )
        edition_store = EditionStore(session=db_session, logger=_logger())

        # Pre-create git_ref edition that matches "v2.1.0" → slug "2.1.0"
        await edition_store.create_internal(
            project_id=project.id,
            slug="2.1.0",
            title="v2.1.0",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "v2.1.0"},
        )

        # Pre-create semver_release edition
        await edition_store.create_internal(
            project_id=project.id,
            slug="latest",
            title="Latest Release",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.semver_release,
        )

        build = await _create_build(db_session, project.id, git_ref="v2.1.0")
        result = await service.track_build(build)
        await db_session.commit()

    outcomes_by_slug = {o.slug: o for o in result.outcomes}
    # Pre-existing editions should report "updated"
    assert outcomes_by_slug["2.1.0"].action == "updated"  # git_ref
    assert outcomes_by_slug["latest"].action == "updated"  # semver_release
    # Auto-created editions should report "created"
    assert outcomes_by_slug["2"].action == "created"  # auto-created major
    assert outcomes_by_slug["2.1"].action == "created"  # auto-created minor


@pytest.mark.asyncio
async def test_track_build_auto_created_git_ref_with_version_editions(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Auto-created git_ref edition reports 'created' with version editions.

    Auto-created git_ref edition reports 'created' even when version
    editions are also auto-created in the same call.
    """
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(
            db_session,
            org_slug="auto-git-ref-org",
            org_slug_rewrite_rules=[
                {
                    "type": "prefix_strip",
                    "prefix": "v",
                    "edition_kind": "release",
                }
            ],
        )

        # No pre-existing editions — git_ref + major + minor all auto-created
        build = await _create_build(db_session, project.id, git_ref="v3.2.0")
        result = await service.track_build(build)
        await db_session.commit()

    outcomes_by_slug = {o.slug: o for o in result.outcomes}
    assert (
        outcomes_by_slug["3.2.0"].action == "created"
    )  # auto-created git_ref
    assert outcomes_by_slug["3"].action == "created"  # auto-created major
    assert outcomes_by_slug["3.2"].action == "created"  # auto-created minor


@pytest.mark.asyncio
async def test_track_build_equal_version_updates(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Two builds with the same version tag both update (>= equality)."""
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="equal-ver-org")
        edition_store = EditionStore(session=db_session, logger=_logger())
        await edition_store.create_internal(
            project_id=project.id,
            slug="eups-latest",
            title="EUPS Latest",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.eups_major_release,
        )

        build1 = await _create_build(db_session, project.id, git_ref="v12_0")
        result1 = await service.track_build(build1)

        build2 = await _create_build(db_session, project.id, git_ref="v12_0")
        result2 = await service.track_build(build2)
        await db_session.commit()

    eups1 = [o for o in result1.outcomes if o.slug == "eups-latest"]
    assert len(eups1) == 1
    assert eups1[0].action == "updated"

    eups2 = [o for o in result2.outcomes if o.slug == "eups-latest"]
    assert len(eups2) == 1
    assert eups2[0].action == "updated"

    # Verify edition points to the second build
    async with db_session.begin():
        edition_store = EditionStore(session=db_session, logger=_logger())
        ed = await edition_store.get_by_slug(
            project_id=project.id, slug="eups-latest"
        )
        assert ed is not None
        assert ed.current_build_id == build2.id
        await db_session.commit()


@pytest.mark.asyncio
async def test_track_build_unparseable_ref_no_match(
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Build whose git_ref is unparseable for the mode produces no match.

    The edition matching layer rejects unparseable refs before
    ``_should_update`` runs, so the edition simply doesn't appear in
    outcomes.
    """
    service = _make_service(db_session)
    async with db_session.begin():
        _org, project = await _setup(db_session, org_slug="unparseable-org")
        edition_store = EditionStore(session=db_session, logger=_logger())
        await edition_store.create_internal(
            project_id=project.id,
            slug="eups-latest",
            title="EUPS Latest",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.eups_major_release,
        )

        # Seed with a valid build
        build_valid = await _create_build(
            db_session, project.id, git_ref="v12_0"
        )
        await service.track_build(build_valid)

        # Track a build whose git_ref doesn't parse for eups_major_release
        build_main = await _create_build(
            db_session, project.id, git_ref="main"
        )
        result = await service.track_build(build_main)
        await db_session.commit()

    # "main" doesn't parse as EUPS major, so the edition is not matched
    eups = [o for o in result.outcomes if o.slug == "eups-latest"]
    assert len(eups) == 0

    # Verify edition still points to original build
    async with db_session.begin():
        edition_store = EditionStore(session=db_session, logger=_logger())
        ed = await edition_store.get_by_slug(
            project_id=project.id, slug="eups-latest"
        )
        assert ed is not None
        assert ed.current_build_id == build_valid.id
        await db_session.commit()
