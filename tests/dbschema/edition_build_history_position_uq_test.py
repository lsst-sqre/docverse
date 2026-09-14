"""Test the ``edition_build_history`` position uniqueness migration.

``position`` is what "newest" means for an edition — the publish writers
and the reconciliation planner both resolve a pair to its lowest-position
row — but nothing ever made an edition's positions unique. Two writers
racing under READ COMMITTED could each bump a history the other's insert
was not yet visible in and commit two position-1 rows, after which the
two readers could pick different rows for one pair and the reconciler
would re-drive the same publish on every tick (PR #621 review, issue
#626).

``e6f7a8b9c0d1`` closes that at the schema level. The constraint is
``DEFERRABLE INITIALLY IMMEDIATE`` because ``record()`` bumps a whole
edition's positions in one ``UPDATE``: an immediate unique index checks
each row as it is rewritten and would reject the bump the moment it
rewrote position 1 into a 2 that another row still held, while a
deferred check sees the statement's finished result.

Pinned by stepping a fresh schema from the revision before the migration
to the migration itself, and back down again so a rollback is safe.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.support.migrations import alembic_downgrade, alembic_upgrade

# Revision immediately before the position-uniqueness migration.
PRE_POSITION_UQ_REVISION = "d5e6f7a8b9c0"

# The migration under test.
POSITION_UQ_REVISION = "e6f7a8b9c0d1"

CONSTRAINT_NAME = "uq_ebh_edition_position"

REDUNDANT_INDEX_NAME = "idx_ebh_edition_position"


async def _constraint(
    engine: AsyncEngine, name: str
) -> tuple[str, bool, bool] | None:
    """Return one constraint's definition and deferral flags.

    Asserting on ``pg_get_constraintdef`` plus ``condeferrable`` /
    ``condeferred`` pins what Postgres parsed the constraint into rather
    than the SQL the migration happened to write. The definition string
    spells ``DEFERRABLE`` out but not ``INITIALLY IMMEDIATE``, which is
    the default it prints nothing for — hence the flags.

    Returns
    -------
    tuple or None
        ``(definition, deferrable, initially_deferred)``, or `None` if no
        constraint of that name exists.
    """
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT pg_get_constraintdef(c.oid) AS definition,"
                    " c.condeferrable, c.condeferred"
                    " FROM pg_constraint c"
                    " JOIN pg_namespace n ON n.oid = c.connamespace"
                    " WHERE n.nspname = 'public' AND c.conname = :name"
                ),
                {"name": name},
            )
        ).one_or_none()
    if row is None:
        return None
    return row.definition, row.condeferrable, row.condeferred


async def _index_exists(engine: AsyncEngine, name: str) -> bool:
    """Report whether an index of this name exists in the public schema."""
    async with engine.connect() as conn:
        found = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM pg_class c"
                    " JOIN pg_namespace n ON n.oid = c.relnamespace"
                    " WHERE n.nspname = 'public' AND c.relname = :name"
                    " AND c.relkind = 'i'"
                ),
                {"name": name},
            )
        ).scalar_one()
    return bool(found)


async def _seed_project(engine: AsyncEngine) -> tuple[int, int]:
    """Insert an org, a project and one build.

    Returns
    -------
    tuple of int
        The project id and the build id every seeded history row points
        at; the tie under test is on ``(edition_id, position)``, so one
        build serves every row.
    """
    async with engine.begin() as conn:
        org_id: int = (
            await conn.execute(
                text(
                    "INSERT INTO organizations"
                    " (slug, title, base_domain, url_scheme,"
                    "  root_path_prefix, purgatory_retention_seconds)"
                    " VALUES ('pos-org', 'Position Org',"
                    "  'pos.example.com', 'subdomain', '/', 2592000)"
                    " RETURNING id"
                )
            )
        ).scalar_one()
        project_id: int = (
            await conn.execute(
                text(
                    "INSERT INTO projects (slug, title, org_id, source_url)"
                    " VALUES ('pos-proj', 'Position Project', :org_id,"
                    "  'https://github.com/example/pos-proj')"
                    " RETURNING id"
                ),
                {"org_id": org_id},
            )
        ).scalar_one()
        build_id: int = (
            await conn.execute(
                text(
                    "INSERT INTO builds"
                    " (public_id, project_id, git_ref, content_hash,"
                    "  status, staging_key, storage_prefix, uploader)"
                    " VALUES (7001, :project_id, 'main', 'sha256:abc',"
                    "  'completed', 'staging/pos', 'pos/', 'testuser')"
                    " RETURNING id"
                ),
                {"project_id": project_id},
            )
        ).scalar_one()
    return project_id, build_id


async def _seed_edition(
    engine: AsyncEngine, *, project_id: int, slug: str
) -> int:
    """Insert one edition into a project and return its id."""
    async with engine.begin() as conn:
        edition_id: int = (
            await conn.execute(
                text(
                    "INSERT INTO editions"
                    " (slug, title, project_id, kind, tracking_mode)"
                    " VALUES (:slug, :slug, :project_id, 'release',"
                    "  'git_ref')"
                    " RETURNING id"
                ),
                {"slug": slug, "project_id": project_id},
            )
        ).scalar_one()
    return edition_id


async def _insert_history(
    engine: AsyncEngine, *, edition_id: int, build_id: int, position: int
) -> int:
    """Insert one history row at an explicit position; return its id."""
    async with engine.begin() as conn:
        history_id: int = (
            await conn.execute(
                text(
                    "INSERT INTO edition_build_history"
                    " (edition_id, build_id, position)"
                    " VALUES (:edition_id, :build_id, :position)"
                    " RETURNING id"
                ),
                {
                    "edition_id": edition_id,
                    "build_id": build_id,
                    "position": position,
                },
            )
        ).scalar_one()
    return history_id


async def _positions_by_id(
    engine: AsyncEngine, *, edition_id: int
) -> dict[int, int]:
    """Return ``{history id: position}`` for one edition."""
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT id, position FROM edition_build_history"
                    " WHERE edition_id = :edition_id"
                ),
                {"edition_id": edition_id},
            )
        ).all()
    return {row.id: row.position for row in rows}


@pytest.mark.asyncio
async def test_migration_adds_the_position_unique_constraint(
    fresh_engine: AsyncEngine,
) -> None:
    """An edition's positions become unique, and deferrable."""
    await alembic_upgrade(POSITION_UQ_REVISION)

    constraint = await _constraint(fresh_engine, CONSTRAINT_NAME)

    assert constraint is not None
    definition, deferrable, initially_deferred = constraint
    assert definition == 'UNIQUE (edition_id, "position") DEFERRABLE'
    assert deferrable is True
    # INITIALLY IMMEDIATE: every statement still checks its own result,
    # so no caller has to opt in and nobody is handed a violation
    # deferred to commit.
    assert initially_deferred is False


@pytest.mark.asyncio
async def test_migration_drops_the_redundant_plain_index(
    fresh_engine: AsyncEngine,
) -> None:
    """The constraint's index replaces the hand-rolled one it duplicates.

    ``idx_ebh_edition_position`` covered exactly ``(edition_id,
    position)``, which is what the constraint's own index now covers.
    Keeping both would pay for two identical btrees on a table written
    once per build for no read the planner could not already serve.
    """
    await alembic_upgrade(POSITION_UQ_REVISION)

    assert not await _index_exists(fresh_engine, REDUNDANT_INDEX_NAME)
    assert await _index_exists(fresh_engine, CONSTRAINT_NAME)


@pytest.mark.asyncio
async def test_migration_renumbers_pre_existing_ties(
    fresh_engine: AsyncEngine,
) -> None:
    """Rows already tied are renumbered newest-first rather than refused.

    Production has run without the constraint for the whole life of the
    table, so the upgrade has to assume ties exist. It resolves them the
    same way the readers do — lowest position first, newest ``id``
    first — so a deploy cannot reorder an edition's history relative to
    what the code was already reading.

    Editions with no tie keep their numbering untouched, gaps included:
    the renumbering is a repair, not a normalization pass over every row
    in the table.
    """
    await alembic_upgrade(PRE_POSITION_UQ_REVISION)
    project_id, build_id = await _seed_project(fresh_engine)
    tied_id = await _seed_edition(
        fresh_engine, project_id=project_id, slug="tied"
    )
    untied_id = await _seed_edition(
        fresh_engine, project_id=project_id, slug="untied"
    )
    oldest = await _insert_history(
        fresh_engine, edition_id=tied_id, build_id=build_id, position=2
    )
    tie_loser = await _insert_history(
        fresh_engine, edition_id=tied_id, build_id=build_id, position=1
    )
    tie_winner = await _insert_history(
        fresh_engine, edition_id=tied_id, build_id=build_id, position=1
    )
    gap_head = await _insert_history(
        fresh_engine, edition_id=untied_id, build_id=build_id, position=1
    )
    gap_tail = await _insert_history(
        fresh_engine, edition_id=untied_id, build_id=build_id, position=3
    )

    await alembic_upgrade(POSITION_UQ_REVISION)

    assert await _positions_by_id(fresh_engine, edition_id=tied_id) == {
        tie_winner: 1,
        tie_loser: 2,
        oldest: 3,
    }
    assert await _positions_by_id(fresh_engine, edition_id=untied_id) == {
        gap_head: 1,
        gap_tail: 3,
    }


@pytest.mark.asyncio
async def test_constraint_rejects_a_second_row_at_one_position(
    fresh_engine: AsyncEngine,
) -> None:
    """Two rows at one position for one edition cannot coexist.

    This is the backstop behind the edition row lock ``record()`` now
    takes: the lock makes the losing writer wait rather than collide, and
    the constraint is what says the collision was never allowed in the
    first place.
    """
    await alembic_upgrade(POSITION_UQ_REVISION)
    project_id, build_id = await _seed_project(fresh_engine)
    edition_id = await _seed_edition(
        fresh_engine, project_id=project_id, slug="dupe"
    )
    await _insert_history(
        fresh_engine, edition_id=edition_id, build_id=build_id, position=1
    )

    with pytest.raises(IntegrityError):
        await _insert_history(
            fresh_engine, edition_id=edition_id, build_id=build_id, position=1
        )


@pytest.mark.asyncio
async def test_constraint_allows_a_whole_edition_position_bump(
    fresh_engine: AsyncEngine,
) -> None:
    """``record()``'s one-statement position bump still commits.

    The bump rewrites every row of an edition at once, so an immediate
    unique index would reject it as soon as it turned a 1 into a 2 that
    another row had not yet vacated — and which of the table's rows it
    rewrites first is the heap's business, not the statement's. The
    ``DEFERRABLE`` declaration moves the check to the end of the
    statement, where the result is consistent.
    """
    await alembic_upgrade(POSITION_UQ_REVISION)
    project_id, build_id = await _seed_project(fresh_engine)
    edition_id = await _seed_edition(
        fresh_engine, project_id=project_id, slug="bump"
    )
    for position in (3, 2, 1):
        await _insert_history(
            fresh_engine,
            edition_id=edition_id,
            build_id=build_id,
            position=position,
        )

    async with fresh_engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE edition_build_history SET position = position + 1"
                " WHERE edition_id = :edition_id"
            ),
            {"edition_id": edition_id},
        )

    positions = await _positions_by_id(fresh_engine, edition_id=edition_id)
    assert sorted(positions.values()) == [2, 3, 4]


@pytest.mark.asyncio
async def test_downgrade_restores_the_plain_index(
    fresh_engine: AsyncEngine,
) -> None:
    """Rolling back removes the constraint and puts its index back."""
    await alembic_upgrade(POSITION_UQ_REVISION)

    await alembic_downgrade(PRE_POSITION_UQ_REVISION)

    assert await _constraint(fresh_engine, CONSTRAINT_NAME) is None
    assert await _index_exists(fresh_engine, REDUNDANT_INDEX_NAME)
