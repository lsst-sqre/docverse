"""Test the projects GitHub-binding migration (``x2y3z4a5b6c7``).

The migration backfills ``github_owner`` / ``github_repo`` from every
existing ``projects.doc_repo`` URL, renames ``doc_repo`` to nullable
``source_url`` (existing values preserved), and adds the both-or-
neither check constraint plus the two webhook-lookup indexes. Any
non-``github.com`` ``doc_repo`` value aborts the migration with the
offending project ids surfaced in the error message.

This test seeds projects at the pre-migration revision and exercises
both branches:

- happy path — a mix of canonical, mixed-case, deep-path, and
  ``.git``-suffixed ``github.com`` URLs all parse cleanly and the
  resulting columns / indexes are correct.
- loud failure — a single non-``github.com`` row aborts the migration
  with the offending project ids listed in the ``RuntimeError``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
import structlog
from alembic.config import Config
from safir.database import (
    create_database_engine,
    drop_database,
    initialize_database,
    stamp_database_async,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from alembic import command
from docverse.config import config
from docverse.dbschema import Base

# Revision immediately before the projects GitHub-binding migration.
PRE_GITHUB_BINDING_REVISION = "w1x2y3z4a5b6"


@pytest_asyncio.fixture
async def fresh_engine() -> AsyncGenerator[AsyncEngine]:
    """Yield an engine pointing at a freshly-dropped DB.

    Mirrors the fixture in ``active_uq_cleanup_test.py``: the ``app``
    conftest fixture would jump straight to head via
    ``initialize_database`` + ``stamp_database_async``, but this test
    needs to step the schema forward from a known earlier revision.
    On teardown the schema is restored to head so subsequent tests'
    ``app`` fixtures find a clean DB.
    """
    logger = structlog.get_logger("docverse")
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    await drop_database(engine, Base.metadata)
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    try:
        yield engine
    finally:
        await drop_database(engine, Base.metadata)
        async with engine.begin() as conn:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        await initialize_database(
            engine, logger, schema=Base.metadata, reset=True
        )
        await stamp_database_async(engine)
        await engine.dispose()


def _alembic_config() -> Config:
    return Config("alembic.ini")


async def _alembic_upgrade(target: str) -> None:
    # ``run_migrations_online`` calls ``asyncio.run`` internally, so the
    # alembic command has to run on a thread that owns its own loop.
    await asyncio.to_thread(command.upgrade, _alembic_config(), target)


async def _seed_org(engine: AsyncEngine) -> int:
    async with engine.begin() as conn:
        org_id = (
            await conn.execute(
                text(
                    "INSERT INTO organizations"
                    " (slug, title, base_domain, url_scheme,"
                    "  root_path_prefix, purgatory_retention_seconds)"
                    " VALUES ('ghbindingorg', 'GitHub Binding Org',"
                    "  'gh-binding.example.com', 'subdomain', '/',"
                    "  2592000)"
                    " RETURNING id"
                )
            )
        ).scalar_one()
    return int(org_id)


async def _seed_project(
    engine: AsyncEngine, *, org_id: int, slug: str, doc_repo: str
) -> int:
    async with engine.begin() as conn:
        project_id = (
            await conn.execute(
                text(
                    "INSERT INTO projects"
                    " (slug, title, org_id, doc_repo)"
                    " VALUES (:slug, :title, :org, :doc_repo)"
                    " RETURNING id"
                ),
                {
                    "slug": slug,
                    "title": slug.replace("-", " ").title(),
                    "org": org_id,
                    "doc_repo": doc_repo,
                },
            )
        ).scalar_one()
    return int(project_id)


@pytest.mark.asyncio
async def test_projects_github_binding_migration_happy_path(
    fresh_engine: AsyncEngine,
) -> None:
    """A mix of GitHub URLs all parse and the schema lands correctly."""
    await _alembic_upgrade(PRE_GITHUB_BINDING_REVISION)
    org_id = await _seed_org(fresh_engine)
    canonical_id = await _seed_project(
        fresh_engine,
        org_id=org_id,
        slug="canonical",
        doc_repo="https://github.com/lsst/pipelines_lsst_io",
    )
    mixed_case_id = await _seed_project(
        fresh_engine,
        org_id=org_id,
        slug="mixed-case",
        doc_repo="https://GitHub.com/Owner/RepoName",
    )
    deep_path_id = await _seed_project(
        fresh_engine,
        org_id=org_id,
        slug="deep-path",
        doc_repo="https://github.com/org/repo/tree/main/docs",
    )
    dot_git_id = await _seed_project(
        fresh_engine,
        org_id=org_id,
        slug="dot-git",
        doc_repo="https://github.com/example/repo.git",
    )

    await _alembic_upgrade("head")

    async with fresh_engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT id, source_url, github_owner, github_repo,"
                    " github_owner_id, github_repo_id,"
                    " github_installation_id"
                    " FROM projects ORDER BY id"
                )
            )
        ).all()
        by_id = {row.id: row for row in rows}
        canonical = by_id[canonical_id]
        assert canonical.source_url == (
            "https://github.com/lsst/pipelines_lsst_io"
        )
        assert canonical.github_owner == "lsst"
        assert canonical.github_repo == "pipelines_lsst_io"
        # Numeric / installation ids are populated opportunistically by
        # later code paths, not the migration.
        assert canonical.github_owner_id is None
        assert canonical.github_repo_id is None
        assert canonical.github_installation_id is None

        mixed_case = by_id[mixed_case_id]
        # Hostname comparison is case-insensitive, owner/repo are
        # preserved verbatim from the URL path.
        assert mixed_case.github_owner == "Owner"
        assert mixed_case.github_repo == "RepoName"

        deep_path = by_id[deep_path_id]
        # Only the first two path segments are used; deeper path
        # components (``tree/main/docs``) are ignored.
        assert deep_path.github_owner == "org"
        assert deep_path.github_repo == "repo"

        dot_git = by_id[dot_git_id]
        # The conventional ``.git`` suffix is stripped from the repo
        # name to match what GitHub returns for the repo.
        assert dot_git.github_owner == "example"
        assert dot_git.github_repo == "repo"

        # The check constraint exists and the both-or-neither predicate
        # is what we declared.
        constraint_def = (
            await conn.execute(
                text(
                    "SELECT pg_get_constraintdef(c.oid)"
                    " FROM pg_constraint c"
                    " JOIN pg_class t ON t.oid = c.conrelid"
                    " WHERE t.relname = 'projects'"
                    " AND c.conname ="
                    " 'ck_projects_github_owner_repo_both_or_neither'"
                )
            )
        ).scalar_one()
        assert "github_owner IS NULL" in constraint_def
        assert "github_repo IS NULL" in constraint_def

        # Both new indexes exist on the projects table.
        idx_names = {
            row[0]
            for row in (
                await conn.execute(
                    text(
                        "SELECT indexname FROM pg_indexes"
                        " WHERE schemaname = 'public'"
                        " AND tablename = 'projects'"
                    )
                )
            ).all()
        }
        assert "idx_projects_github_owner_repo" in idx_names
        assert "idx_projects_github_repo_id" in idx_names

        # The owner/repo index uses the functional ``lower(...)`` form
        # so webhook lookups can match case-insensitively without a
        # secondary index.
        owner_repo_def = (
            await conn.execute(
                text(
                    "SELECT pg_get_indexdef(c.oid)"
                    " FROM pg_class c"
                    " JOIN pg_namespace n ON n.oid = c.relnamespace"
                    " WHERE n.nspname = 'public'"
                    " AND c.relname = 'idx_projects_github_owner_repo'"
                )
            )
        ).scalar_one()
        assert "lower((github_owner)" in owner_repo_def
        assert "lower((github_repo)" in owner_repo_def

        # The ``doc_repo`` column is gone; ``source_url`` is the
        # nullable replacement and carries the original URL.
        columns = {
            row.column_name: row
            for row in (
                await conn.execute(
                    text(
                        "SELECT column_name, is_nullable, data_type,"
                        " character_maximum_length"
                        " FROM information_schema.columns"
                        " WHERE table_schema = 'public'"
                        " AND table_name = 'projects'"
                    )
                )
            ).all()
        }
        assert "doc_repo" not in columns
        assert "source_url" in columns
        assert columns["source_url"].is_nullable == "YES"
        assert columns["source_url"].character_maximum_length == 512


@pytest.mark.asyncio
async def test_projects_github_binding_migration_aborts_on_non_github(
    fresh_engine: AsyncEngine,
) -> None:
    """A non-github.com ``doc_repo`` aborts with offending project ids."""
    await _alembic_upgrade(PRE_GITHUB_BINDING_REVISION)
    org_id = await _seed_org(fresh_engine)
    await _seed_project(
        fresh_engine,
        org_id=org_id,
        slug="ok-row",
        doc_repo="https://github.com/example/ok",
    )
    bad_id = await _seed_project(
        fresh_engine,
        org_id=org_id,
        slug="gitlab-row",
        doc_repo="https://gitlab.com/example/repo",
    )
    bad_id_2 = await _seed_project(
        fresh_engine,
        org_id=org_id,
        slug="onprem-row",
        doc_repo="https://git.example.com/example/repo",
    )

    with pytest.raises(RuntimeError) as excinfo:
        await _alembic_upgrade("head")

    # The error message must surface the offending project ids so the
    # operator can address them without grepping the table by hand.
    message = str(excinfo.value)
    assert str(bad_id) in message
    assert str(bad_id_2) in message
    assert "github.com" in message
