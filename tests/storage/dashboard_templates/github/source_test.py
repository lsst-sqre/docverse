"""Tests for GitHubTemplateSource."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateStore,
    GitHubTemplateFileInput,
    GitHubTemplateKey,
    GitHubTemplateSource,
)
from docverse.storage.dashboard_templates.template_source import TemplateSource

_KEY = GitHubTemplateKey(
    github_owner="acme",
    github_repo="dashboard-templates",
    github_ref="main",
    root_path="/",
)

_TEMPLATE_TOML = b"""\
[dashboard]
template = "dashboard.html.jinja"

[dashboard.assets]
css = ["dashboard.css"]
images = ["logo.svg"]

[switcher]
include_kinds = ["main", "release"]
"""


async def _seed_template(session: AsyncSession) -> int:
    logger = structlog.get_logger("test")
    store = DashboardGitHubTemplateStore(session=session, logger=logger)
    result = await store.upsert(
        key=_KEY,
        commit_sha="deadbeef",
        etag="etag-1",
        template_toml=_TEMPLATE_TOML,
        files=[
            GitHubTemplateFileInput(
                relative_path="dashboard.html.jinja",
                is_text=True,
                data=b"<html>hi</html>",
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
        ],
    )
    return result.template.id


@pytest.mark.asyncio
async def test_github_template_source_satisfies_protocol(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        template_id = await _seed_template(db_session)
        await db_session.commit()
    source = GitHubTemplateSource(template_id=template_id, session=db_session)
    assert isinstance(source, TemplateSource)


@pytest.mark.asyncio
async def test_github_template_source_loads_config_after_preload(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        template_id = await _seed_template(db_session)
        await db_session.commit()
    source = GitHubTemplateSource(template_id=template_id, session=db_session)
    await source.preload()
    config = source.load_config()
    assert config.dashboard.template == "dashboard.html.jinja"
    assert config.dashboard.css == ("dashboard.css",)
    assert config.switcher.include_kinds == ("main", "release")


@pytest.mark.asyncio
async def test_github_template_source_caches_parsed_config(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        template_id = await _seed_template(db_session)
        await db_session.commit()
    source = GitHubTemplateSource(template_id=template_id, session=db_session)
    await source.preload()
    first = source.load_config()
    second = source.load_config()
    assert first is second


@pytest.mark.asyncio
async def test_github_template_source_reads_text_template(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        template_id = await _seed_template(db_session)
        await db_session.commit()
    source = GitHubTemplateSource(template_id=template_id, session=db_session)
    await source.preload()
    text = source.read_template("dashboard.html.jinja")
    assert text == "<html>hi</html>"


@pytest.mark.asyncio
async def test_github_template_source_reads_binary_asset(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        template_id = await _seed_template(db_session)
        await db_session.commit()
    source = GitHubTemplateSource(template_id=template_id, session=db_session)
    await source.preload()
    data = source.read_asset("logo.svg")
    assert data == b"\x89PNG\r\n"


@pytest.mark.asyncio
async def test_github_template_source_missing_file_raises_filenotfound(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        template_id = await _seed_template(db_session)
        await db_session.commit()
    source = GitHubTemplateSource(template_id=template_id, session=db_session)
    await source.preload()
    with pytest.raises(FileNotFoundError):
        source.read_asset("does-not-exist")


@pytest.mark.asyncio
async def test_github_template_source_unknown_template_raises(
    db_session: AsyncSession,
) -> None:
    source = GitHubTemplateSource(template_id=999_999, session=db_session)
    with pytest.raises(LookupError):
        await source.preload()


@pytest.mark.asyncio
async def test_github_template_source_methods_require_preload(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        template_id = await _seed_template(db_session)
        await db_session.commit()
    source = GitHubTemplateSource(template_id=template_id, session=db_session)
    with pytest.raises(RuntimeError):
        source.load_config()
    with pytest.raises(RuntimeError):
        source.read_template("dashboard.html.jinja")
    with pytest.raises(RuntimeError):
        source.read_asset("logo.svg")
