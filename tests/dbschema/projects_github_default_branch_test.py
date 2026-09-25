"""Test the ``projects.github_default_branch`` migration (``c0d1e2f3a4b5``).

PRD #721 (DM-56241) gives Docverse a place to remember a project's
GitHub default branch so ``__main`` can follow a ``master`` → ``main``
rename. The column is nullable with no data step: ``NULL`` means "not
yet learned", every consumer falls back to ``"main"``, and the resolve
worker and the daily ``git_ref_audit`` fill it afterwards. This test
steps a fresh schema across the migration with a bound project already
in place, pins the column's type and nullability, and pins the
downgrade so a rollback is safe.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.support.migrations import alembic_downgrade, alembic_upgrade

# Revision immediately before this migration (projects listing index).
PRE_DEFAULT_BRANCH_REVISION = "b9c0d1e2f3a4"

# The migration under test.
DEFAULT_BRANCH_REVISION = "c0d1e2f3a4b5"


async def _default_branch_column(
    engine: AsyncEngine,
) -> tuple[str, int | None, str] | None:
    """Return ``(data_type, max_length, is_nullable)``, or ``None``."""
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT data_type, character_maximum_length, is_nullable"
                    " FROM information_schema.columns"
                    " WHERE table_name = 'projects'"
                    " AND column_name = 'github_default_branch'"
                )
            )
        ).first()
    if row is None:
        return None
    return row.data_type, row.character_maximum_length, row.is_nullable


@pytest.mark.asyncio
async def test_migration_adds_nullable_github_default_branch(
    fresh_engine: AsyncEngine,
) -> None:
    """The column arrives nullable, and a pre-existing project reads NULL."""
    await alembic_upgrade(PRE_DEFAULT_BRANCH_REVISION)
    assert await _default_branch_column(fresh_engine) is None

    async with fresh_engine.begin() as conn:
        org_id = (
            await conn.execute(
                text(
                    "INSERT INTO organizations"
                    " (public_id, slug, title, base_domain, url_scheme,"
                    "  root_path_prefix, purgatory_retention_seconds)"
                    " VALUES (1, 'gdb-org', 'GDB Org',"
                    "  'gdb.example.com', 'subdomain', '/', 2592000)"
                    " RETURNING id"
                )
            )
        ).scalar_one()
        await conn.execute(
            text(
                "INSERT INTO projects"
                " (public_id, slug, title, org_id, github_owner,"
                "  github_repo)"
                " VALUES (1, 'gdb-proj', 'GDB Proj', :org, 'acme', 'docs')"
            ),
            {"org": org_id},
        )

    await alembic_upgrade(DEFAULT_BRANCH_REVISION)

    assert await _default_branch_column(fresh_engine) == (
        "character varying",
        255,
        "YES",
    )

    # No data step: an existing bound project has not learned its
    # default branch yet, so it reads NULL until a resolve or audit.
    async with fresh_engine.connect() as conn:
        existing = (
            await conn.execute(
                text("SELECT slug, github_default_branch FROM projects")
            )
        ).one()
    assert existing.slug == "gdb-proj"
    assert existing.github_default_branch is None


@pytest.mark.asyncio
async def test_downgrade_drops_github_default_branch(
    fresh_engine: AsyncEngine,
) -> None:
    """Rolling back removes the column."""
    await alembic_upgrade(DEFAULT_BRANCH_REVISION)
    assert await _default_branch_column(fresh_engine) is not None

    await alembic_downgrade(PRE_DEFAULT_BRANCH_REVISION)

    assert await _default_branch_column(fresh_engine) is None
