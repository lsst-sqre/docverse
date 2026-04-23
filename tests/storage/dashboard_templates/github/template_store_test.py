"""Tests for DashboardGitHubTemplateStore."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.dashboard_github_template import (
    SqlDashboardGitHubTemplate,
)
from docverse.dbschema.dashboard_github_template_file import (
    SqlDashboardGitHubTemplateFile,
)
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateStore,
    GitHubTemplateFileInput,
    GitHubTemplateKey,
)

_KEY = GitHubTemplateKey(
    github_owner="acme",
    github_repo="dashboard-templates",
    github_ref="main",
    root_path="/",
)


def _store(session: AsyncSession) -> DashboardGitHubTemplateStore:
    logger = structlog.get_logger("test")
    return DashboardGitHubTemplateStore(session=session, logger=logger)


def _files() -> list[GitHubTemplateFileInput]:
    return [
        GitHubTemplateFileInput(
            relative_path="dashboard.html.jinja",
            is_text=True,
            data=b"<html></html>",
        ),
        GitHubTemplateFileInput(
            relative_path="dashboard.css",
            is_text=True,
            data=b"body { color: red; }",
        ),
        GitHubTemplateFileInput(
            relative_path="logo.svg",
            is_text=False,
            data=b"\x89PNG\r\n",
        ),
    ]


_TEMPLATE_TOML = b"""\
[dashboard]
template = "dashboard.html.jinja"

[dashboard.assets]
css = ["dashboard.css"]
images = ["logo.svg"]
"""


@pytest.mark.asyncio
async def test_upsert_creates_new_template_and_files(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        store = _store(db_session)
        result = await store.upsert(
            key=_KEY,
            commit_sha="deadbeef",
            etag="etag-1",
            template_toml=_TEMPLATE_TOML,
            files=_files(),
        )
        await db_session.commit()
    assert result.changed is True
    assert result.template.commit_sha == "deadbeef"
    assert result.template.etag == "etag-1"
    assert result.template.template_toml == _TEMPLATE_TOML
    assert result.template.github_owner_id is None
    assert result.template.github_repo_id is None


@pytest.mark.asyncio
async def test_upsert_round_trips_github_numeric_ids(
    db_session: AsyncSession,
) -> None:
    """Populated numeric IDs survive an upsert → re-fetch round-trip."""
    async with db_session.begin():
        store = _store(db_session)
        result = await store.upsert(
            key=_KEY,
            commit_sha="deadbeef",
            etag="etag-1",
            template_toml=_TEMPLATE_TOML,
            files=_files(),
            github_owner_id=12345,
            github_repo_id=67890,
        )
        await db_session.commit()
    assert result.template.github_owner_id == 12345
    assert result.template.github_repo_id == 67890

    async with db_session.begin():
        store = _store(db_session)
        fetched = await store.get_by_key(_KEY)
        await db_session.rollback()
    assert fetched is not None
    assert fetched.github_owner_id == 12345
    assert fetched.github_repo_id == 67890

    async with db_session.begin():
        store = _store(db_session)
        files = await store.list_files(result.template.id)
        await db_session.rollback()
    paths = sorted(f.relative_path for f in files)
    assert paths == ["dashboard.css", "dashboard.html.jinja", "logo.svg"]
    by_path = {f.relative_path: f for f in files}
    assert by_path["dashboard.html.jinja"].is_text is True
    assert by_path["logo.svg"].is_text is False
    assert by_path["logo.svg"].data == b"\x89PNG\r\n"
    assert by_path["dashboard.css"].size_bytes == len(b"body { color: red; }")


@pytest.mark.asyncio
async def test_upsert_idempotent_when_etag_unchanged(
    db_session: AsyncSession,
) -> None:
    """Same-ETag re-upsert returns the existing row and writes nothing."""
    async with db_session.begin():
        store = _store(db_session)
        first = await store.upsert(
            key=_KEY,
            commit_sha="deadbeef",
            etag="etag-1",
            template_toml=_TEMPLATE_TOML,
            files=_files(),
        )
        await db_session.commit()

    async with db_session.begin():
        original_files = sorted(
            (f.relative_path, f.id)
            for f in (
                await db_session.execute(
                    select(SqlDashboardGitHubTemplateFile).where(
                        SqlDashboardGitHubTemplateFile.github_template_id
                        == first.template.id,
                    )
                )
            )
            .scalars()
            .all()
        )
        await db_session.rollback()

    async with db_session.begin():
        store = _store(db_session)
        second = await store.upsert(
            key=_KEY,
            commit_sha="ignored-different-sha",
            etag="etag-1",
            template_toml=b"different-bytes",
            files=[],
        )
        await db_session.commit()

    assert second.changed is False
    assert second.template.id == first.template.id
    assert second.template.commit_sha == "deadbeef"
    assert second.template.etag == "etag-1"
    assert second.template.template_toml == _TEMPLATE_TOML

    async with db_session.begin():
        after_files = sorted(
            (f.relative_path, f.id)
            for f in (
                await db_session.execute(
                    select(SqlDashboardGitHubTemplateFile).where(
                        SqlDashboardGitHubTemplateFile.github_template_id
                        == first.template.id,
                    )
                )
            )
            .scalars()
            .all()
        )
        await db_session.rollback()
    assert after_files == original_files


@pytest.mark.asyncio
async def test_upsert_replaces_files_when_etag_changes(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        store = _store(db_session)
        first = await store.upsert(
            key=_KEY,
            commit_sha="deadbeef",
            etag="etag-1",
            template_toml=_TEMPLATE_TOML,
            files=_files(),
        )
        await db_session.commit()

    new_files = [
        GitHubTemplateFileInput(
            relative_path="dashboard.html.jinja",
            is_text=True,
            data=b"<html>v2</html>",
        ),
        GitHubTemplateFileInput(
            relative_path="new_asset.js",
            is_text=True,
            data=b"console.log('hi')",
        ),
    ]
    async with db_session.begin():
        store = _store(db_session)
        second = await store.upsert(
            key=_KEY,
            commit_sha="cafef00d",
            etag="etag-2",
            template_toml=b"[dashboard]\n",
            files=new_files,
        )
        await db_session.commit()

    assert second.changed is True
    assert second.template.id == first.template.id
    assert second.template.commit_sha == "cafef00d"
    assert second.template.etag == "etag-2"
    assert second.template.template_toml == b"[dashboard]\n"

    async with db_session.begin():
        store = _store(db_session)
        files = await store.list_files(second.template.id)
        await db_session.rollback()
    paths = sorted(f.relative_path for f in files)
    assert paths == ["dashboard.html.jinja", "new_asset.js"]
    by_path = {f.relative_path: f for f in files}
    assert by_path["dashboard.html.jinja"].data == b"<html>v2</html>"


@pytest.mark.asyncio
async def test_get_by_key_returns_none_when_missing(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        store = _store(db_session)
        result = await store.get_by_key(_KEY)
        await db_session.rollback()
    assert result is None


@pytest.mark.asyncio
async def test_get_by_key_returns_template(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        store = _store(db_session)
        upsert = await store.upsert(
            key=_KEY,
            commit_sha="deadbeef",
            etag="etag-1",
            template_toml=_TEMPLATE_TOML,
            files=_files(),
        )
        await db_session.commit()
    async with db_session.begin():
        store = _store(db_session)
        fetched = await store.get_by_key(_KEY)
        await db_session.rollback()
    assert fetched is not None
    assert fetched.id == upsert.template.id


@pytest.mark.asyncio
async def test_get_file_returns_single_row(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        store = _store(db_session)
        upsert = await store.upsert(
            key=_KEY,
            commit_sha="deadbeef",
            etag="etag-1",
            template_toml=_TEMPLATE_TOML,
            files=_files(),
        )
        await db_session.commit()
    async with db_session.begin():
        store = _store(db_session)
        file_row = await store.get_file(
            template_id=upsert.template.id,
            relative_path="dashboard.css",
        )
        missing = await store.get_file(
            template_id=upsert.template.id,
            relative_path="not-here",
        )
        await db_session.rollback()
    assert file_row is not None
    assert file_row.data == b"body { color: red; }"
    assert missing is None


@pytest.mark.asyncio
async def test_unique_template_key_blocks_direct_duplicate(
    db_session: AsyncSession,
) -> None:
    """Two raw template rows with the same dedup key are rejected.

    The store's :meth:`upsert` is the supported entry point; this test
    inserts ORM rows directly to prove the underlying unique constraint
    is in place if a future caller bypasses the store.
    """
    async with db_session.begin():
        db_session.add(
            SqlDashboardGitHubTemplate(
                github_owner=_KEY.github_owner,
                github_repo=_KEY.github_repo,
                github_ref=_KEY.github_ref,
                root_path=_KEY.root_path,
                commit_sha="a",
                etag="a",
                template_toml=b"a",
            )
        )
        await db_session.commit()
    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(
                SqlDashboardGitHubTemplate(
                    github_owner=_KEY.github_owner,
                    github_repo=_KEY.github_repo,
                    github_ref=_KEY.github_ref,
                    root_path=_KEY.root_path,
                    commit_sha="b",
                    etag="b",
                    template_toml=b"b",
                )
            )


@pytest.mark.asyncio
async def test_unique_template_file_path_blocks_duplicate(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        store = _store(db_session)
        upsert = await store.upsert(
            key=_KEY,
            commit_sha="deadbeef",
            etag="etag-1",
            template_toml=_TEMPLATE_TOML,
            files=[
                GitHubTemplateFileInput(
                    relative_path="dup.txt",
                    is_text=True,
                    data=b"a",
                ),
            ],
        )
        await db_session.commit()
    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(
                SqlDashboardGitHubTemplateFile(
                    github_template_id=upsert.template.id,
                    relative_path="dup.txt",
                    is_text=True,
                    data=b"b",
                    size_bytes=1,
                )
            )
