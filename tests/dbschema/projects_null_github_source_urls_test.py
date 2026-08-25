"""Test the null-github-source_url migration (``y3z4a5b6c7d8``).

The migration nulls every ``projects.source_url`` that parses as a
github.com repository URL, leaving the structured ``github_*`` binding
untouched, while preserving non-GitHub URLs verbatim. After the
migration a GitHub-bound project's effective source URL is derived from
the binding (``Project.effective_source_url``) rather than from the
nulled column.

This test seeds projects at the revision immediately before the
migration and asserts:

- a github.com ``source_url`` alongside a binding is nulled, the binding
  is unchanged, and the domain model still derives the canonical URL;
- a non-GitHub ``source_url`` (no binding) survives untouched.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from docverse_server.domain.project import Project as ProjectDomain
from tests.support.migrations import alembic_upgrade

# Revision immediately before the null-github-source_url migration.
PRE_NULL_REVISION = "x2y3z4a5b6c7"


async def _seed_org(engine: AsyncEngine) -> int:
    async with engine.begin() as conn:
        org_id = (
            await conn.execute(
                text(
                    "INSERT INTO organizations"
                    " (slug, title, base_domain, url_scheme,"
                    "  root_path_prefix, purgatory_retention_seconds)"
                    " VALUES ('nullghorg', 'Null GH Org',"
                    "  'null-gh.example.com', 'subdomain', '/',"
                    "  2592000)"
                    " RETURNING id"
                )
            )
        ).scalar_one()
    return int(org_id)


async def _seed_project(
    engine: AsyncEngine,
    *,
    org_id: int,
    slug: str,
    source_url: str | None,
    github_owner: str | None = None,
    github_repo: str | None = None,
) -> int:
    async with engine.begin() as conn:
        project_id = (
            await conn.execute(
                text(
                    "INSERT INTO projects"
                    " (slug, title, org_id, source_url,"
                    "  github_owner, github_repo)"
                    " VALUES (:slug, :title, :org, :source_url,"
                    "  :owner, :repo)"
                    " RETURNING id"
                ),
                {
                    "slug": slug,
                    "title": slug.replace("-", " ").title(),
                    "org": org_id,
                    "source_url": source_url,
                    "owner": github_owner,
                    "repo": github_repo,
                },
            )
        ).scalar_one()
    return int(project_id)


@pytest.mark.asyncio
async def test_null_github_source_urls_migration(
    fresh_engine: AsyncEngine,
) -> None:
    """github.com source_urls are nulled; bindings and non-GitHub URLs hold."""
    await alembic_upgrade(PRE_NULL_REVISION)
    org_id = await _seed_org(fresh_engine)
    bound_id = await _seed_project(
        fresh_engine,
        org_id=org_id,
        slug="bound",
        source_url="https://github.com/lsst/pipelines_lsst_io",
        github_owner="lsst",
        github_repo="pipelines_lsst_io",
    )
    deep_path_id = await _seed_project(
        fresh_engine,
        org_id=org_id,
        slug="deep-path",
        source_url="https://github.com/lsst/docs/tree/main/docs",
        github_owner="lsst",
        github_repo="docs",
    )
    gitlab_id = await _seed_project(
        fresh_engine,
        org_id=org_id,
        slug="gitlab",
        source_url="https://gitlab.com/lsst/mirror",
    )

    await alembic_upgrade("head")

    async with fresh_engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT id, source_url, github_owner, github_repo"
                    " FROM projects ORDER BY id"
                )
            )
        ).all()
    by_id = {row.id: row for row in rows}

    # The github.com source_url is nulled; the binding is untouched.
    bound = by_id[bound_id]
    assert bound.source_url is None
    assert bound.github_owner == "lsst"
    assert bound.github_repo == "pipelines_lsst_io"

    # A deep-path github.com URL also parses as a repo URL and is nulled.
    deep_path = by_id[deep_path_id]
    assert deep_path.source_url is None
    assert deep_path.github_owner == "lsst"
    assert deep_path.github_repo == "docs"

    # The non-GitHub URL survives untouched (no binding to derive from).
    gitlab = by_id[gitlab_id]
    assert gitlab.source_url == "https://gitlab.com/lsst/mirror"
    assert gitlab.github_owner is None
    assert gitlab.github_repo is None

    # Reads still derive the canonical URL for the bound row from its
    # binding even though the stored column is now NULL.
    now = datetime.now(tz=UTC)
    derived = ProjectDomain(
        id=bound.id,
        slug="bound",
        title="Bound",
        org_id=org_id,
        source_url=bound.source_url,
        github_owner=bound.github_owner,
        github_repo=bound.github_repo,
        date_created=now,
        date_updated=now,
    ).effective_source_url
    assert derived == "https://github.com/lsst/pipelines_lsst_io"
