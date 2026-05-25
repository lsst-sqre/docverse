"""Integration tests for DashboardPublisher."""

from __future__ import annotations

import json

import pytest
import structlog
from rubin.repertoire import DiscoveryClient
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.client.models import (
    BuildCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.services.dashboard.publisher import DashboardPublisher
from docverse.services.dashboard_templates.resolver import (
    ResolvedTemplate,
    ResolvedTemplateOrigin,
    TemplateResolver,
)
from docverse.storage.build_store import BuildStore
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
    GitHubTemplateFileInput,
    GitHubTemplateKey,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.objectstore import MockObjectStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

_HASH = "sha256:" + "a" * 64


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _make_publisher(
    session: AsyncSession, discovery_client: DiscoveryClient
) -> DashboardPublisher:
    logger = _logger()
    resolver = TemplateResolver(
        binding_store=DashboardGitHubTemplateBindingStore(
            session=session, logger=logger
        ),
        template_store=DashboardGitHubTemplateStore(
            session=session, logger=logger
        ),
        logger=logger,
    )
    return DashboardPublisher(
        org_store=OrganizationStore(session=session, logger=logger),
        project_store=ProjectStore(session=session, logger=logger),
        edition_store=EditionStore(session=session, logger=logger),
        build_store=BuildStore(session=session, logger=logger),
        discovery=discovery_client,
        logger=logger,
        template_resolver=resolver,
    )


@pytest.mark.asyncio
async def test_publisher_uploads_dashboard_and_switcher(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)

    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="pub-org",
                title="Pub Org",
                base_domain="pub.example.com",
            )
        )
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="pub-proj",
                title="Pub Project",
                source_url="https://github.com/example/pub",
            ),
        )
        await edition_store.create_internal(
            project_id=project.id,
            slug="__main",
            title="Latest",
            kind=EditionKind.main,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "main"},
        )
        build = await build_store.create(
            project_id=project.id,
            data=BuildCreate(git_ref="v1.0.0", content_hash=_HASH),
            uploader="testuser",
            project_slug="pub-proj",
        )
        release = await edition_store.create_internal(
            project_id=project.id,
            slug="v1.0.0",
            title="v1.0.0",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "v1.0.0"},
        )
        await edition_store.set_current_build(
            edition_id=release.id,
            build_id=build.id,
            skip_date_guard=True,
        )
        await db_session.commit()

    publisher = _make_publisher(db_session, discovery_client)
    mock_store = MockObjectStore()

    async def _provider() -> MockObjectStore:
        return mock_store

    async with db_session.begin():
        context, progress = await publisher.publish(
            org_id=org.id,
            project_id=project.id,
            object_store_provider=_provider,
        )

    # 3 top-level artifacts (dashboard, switcher, 404) + 2 per-edition JSON
    # files (__main + v1.0.0).
    assert progress.object_count == 5
    assert progress.total_size_bytes > 0

    html_obj = mock_store.objects["pub-proj/__dashboard.html"]
    assert html_obj.content_type == "text/html; charset=utf-8"
    html_text = html_obj.data.decode("utf-8")
    assert "v1.0.0" in html_text
    # Assets from template.toml must be inlined into the rendered HTML:
    # CSS in a single <style>, JS in a single <script>, SVG raw, and the
    # PNG favicon as a base64 data URI.
    assert "<style>" in html_text
    assert "<script>" in html_text
    assert "<svg" in html_text
    assert "data:image/png;base64," in html_text

    switcher_obj = mock_store.objects["pub-proj/__switcher.json"]
    assert switcher_obj.content_type == "application/json; charset=utf-8"
    payload = json.loads(switcher_obj.data.decode("utf-8"))
    versions = [entry["version"] for entry in payload]
    assert versions == ["__main", "v1.0.0"]

    error_obj = mock_store.objects["pub-proj/__404.html"]
    assert error_obj.content_type == "text/html; charset=utf-8"
    error_text = error_obj.data.decode("utf-8")
    assert "404" in error_text
    assert "Pub Project" in error_text

    # rendered_at is shared across artifacts (single context)
    assert context.rendered_at.isoformat() in html_obj.data.decode("utf-8")
    assert context.rendered_at.isoformat() in error_obj.data.decode("utf-8")


@pytest.mark.asyncio
async def test_publisher_writes_per_edition_json_files(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    """One ``__editions/{slug}.json`` per non-deleted edition is uploaded."""
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)

    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="per-ed-org",
                title="Per Ed Org",
                base_domain="per-ed.example.com",
            )
        )
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="per-ed-proj",
                title="Per Ed Project",
                source_url="https://github.com/example/per-ed",
            ),
        )
        await edition_store.create_internal(
            project_id=project.id,
            slug="__main",
            title="Latest",
            kind=EditionKind.main,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "main"},
        )
        await edition_store.create_internal(
            project_id=project.id,
            slug="v1.0.0",
            title="v1.0.0",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "v1.0.0"},
        )
        await db_session.commit()

    publisher = _make_publisher(db_session, discovery_client)
    mock_store = MockObjectStore()

    async def _provider() -> MockObjectStore:
        return mock_store

    async with db_session.begin():
        await publisher.publish(
            org_id=org.id,
            project_id=project.id,
            object_store_provider=_provider,
        )

    main_obj = mock_store.objects["per-ed-proj/__editions/__main.json"]
    assert main_obj.content_type == "application/json; charset=utf-8"
    main_meta = json.loads(main_obj.data.decode("utf-8"))
    assert main_meta["is_canonical"] is True
    assert main_meta["edition_slug"] == "__main"
    assert main_meta["canonical_url"] == main_meta["published_url"]

    release_obj = mock_store.objects["per-ed-proj/__editions/v1.0.0.json"]
    assert release_obj.content_type == "application/json; charset=utf-8"
    release_meta = json.loads(release_obj.data.decode("utf-8"))
    assert release_meta["is_canonical"] is False
    assert release_meta["edition_slug"] == "v1.0.0"
    assert release_meta["canonical_url"] == main_meta["published_url"]
    assert release_meta["canonical_url"] != release_meta["published_url"]


@pytest.mark.asyncio
async def test_publisher_handles_empty_project(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)

    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="empty-pub-org",
                title="Empty",
                base_domain="empty.example.com",
            )
        )
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="empty-pub-proj",
                title="Empty Project",
                source_url="https://github.com/example/empty",
            ),
        )
        await db_session.commit()

    publisher = _make_publisher(db_session, discovery_client)
    mock_store = MockObjectStore()

    async def _provider() -> MockObjectStore:
        return mock_store

    async with db_session.begin():
        await publisher.publish(
            org_id=org.id,
            project_id=project.id,
            object_store_provider=_provider,
        )

    assert "empty-pub-proj/__dashboard.html" in mock_store.objects
    switcher = mock_store.objects["empty-pub-proj/__switcher.json"]
    assert json.loads(switcher.data.decode("utf-8")) == []

    error = mock_store.objects["empty-pub-proj/__404.html"]
    assert error.content_type == "text/html; charset=utf-8"
    assert "404" in error.data.decode("utf-8")

    # Zero editions → zero __editions/*.json files.
    assert not any(
        key.startswith("empty-pub-proj/__editions/")
        for key in mock_store.objects
    )


_CUSTOM_TEMPLATE_TOML = b"""\
[dashboard]
template = "dashboard.html.jinja"

[dashboard.assets]
css = []
js = []
images = []

[switcher]
include_kinds = ["main", "release"]
"""

_CUSTOM_DASHBOARD_JINJA = b"""\
<!DOCTYPE html>
<html lang="en">
<head><title>{{ project.title }}</title></head>
<body>
<div id="custom-marker">GITHUB-TEMPLATE-RENDERED-{{ project.slug }}</div>
</body>
</html>
"""


@pytest.mark.asyncio
async def test_publisher_resolve_template_returns_builtin_when_unbound(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    """``resolve_template`` returns a ``builtin`` ResolvedTemplate.

    Exercises the no-binding path: a project without an override or
    org-default binding resolves through to ``BuiltInTemplateSource``.
    """
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)

    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="resolve-builtin-org",
                title="Resolve Builtin Org",
                base_domain="resolve-builtin.example.com",
            )
        )
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="resolve-builtin-proj",
                title="Resolve Builtin Project",
                source_url="https://github.com/example/resolve-builtin",
            ),
        )
        await db_session.commit()

    publisher = _make_publisher(db_session, discovery_client)

    async with db_session.begin():
        resolved = await publisher.resolve_template(
            org_id=org.id, project_id=project.id
        )

    assert isinstance(resolved, ResolvedTemplate)
    assert resolved.origin is ResolvedTemplateOrigin.builtin


@pytest.mark.asyncio
async def test_publisher_render_and_upload_uses_provided_resolved_template(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    """``render_and_upload`` renders from the caller-supplied ``resolved``.

    Pins the contract that the upload loop is a pure consumer of the
    pre-resolved template: no DB reads happen inside ``render_and_upload``
    itself, so the worker can commit its resolve-time transaction before
    entering the object-store context.
    """
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    template_store = DashboardGitHubTemplateStore(
        session=db_session, logger=logger
    )
    binding_store = DashboardGitHubTemplateBindingStore(
        session=db_session, logger=logger
    )

    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="render-provided-org",
                title="Render Provided Org",
                base_domain="render-provided.example.com",
            )
        )
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="render-provided-proj",
                title="Render Provided Project",
                source_url="https://github.com/example/render-provided",
            ),
        )
        template_result = await template_store.upsert(
            key=GitHubTemplateKey(
                github_owner="acme",
                github_repo="dashboard-templates",
                github_ref="main",
                root_path="/",
            ),
            commit_sha="deadbeef",
            etag="etag-1",
            template_toml=_CUSTOM_TEMPLATE_TOML,
            files=[
                GitHubTemplateFileInput(
                    relative_path="dashboard.html.jinja",
                    is_text=True,
                    data=_CUSTOM_DASHBOARD_JINJA,
                ),
            ],
        )
        binding = await binding_store.create(
            DashboardGitHubTemplateBindingCreate(
                org_id=org.id,
                project_id=None,
                github_owner="acme",
                github_repo="dashboard-templates",
                github_ref="main",
                root_path="/",
            )
        )
        await binding_store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="succeeded",
            github_template_id=template_result.template.id,
        )
        await db_session.commit()

    publisher = _make_publisher(db_session, discovery_client)
    mock_store = MockObjectStore()

    async with db_session.begin():
        context = await publisher.build_context(
            org_id=org.id, project_id=project.id
        )
        resolved = await publisher.resolve_template(
            org_id=org.id, project_id=project.id
        )

    # Upload loop runs outside any DB transaction: the preloaded
    # GitHub template source must satisfy every renderer read.
    async with mock_store:
        await publisher.render_and_upload(
            context=context,
            object_store=mock_store,
            resolved=resolved,
        )

    html_obj = mock_store.objects["render-provided-proj/__dashboard.html"]
    html_text = html_obj.data.decode("utf-8")
    assert "GITHUB-TEMPLATE-RENDERED-render-provided-proj" in html_text
    assert resolved.origin is ResolvedTemplateOrigin.org_default


@pytest.mark.asyncio
async def test_publisher_logs_builtin_template_origin(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    """``template_origin='builtin'`` is bound on unbound-project uploads."""
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)

    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="origin-builtin-org",
                title="Origin Builtin Org",
                base_domain="origin-builtin.example.com",
            )
        )
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="origin-builtin-proj",
                title="Origin Builtin Project",
                source_url="https://github.com/example/origin-builtin",
            ),
        )
        await db_session.commit()

    publisher = _make_publisher(db_session, discovery_client)
    mock_store = MockObjectStore()

    async def _provider() -> MockObjectStore:
        return mock_store

    with capture_logs() as captured:
        async with db_session.begin():
            await publisher.publish(
                org_id=org.id,
                project_id=project.id,
                object_store_provider=_provider,
            )

    upload_events = [
        event
        for event in captured
        if event.get("event") == "Uploaded dashboard artifacts"
    ]
    assert len(upload_events) == 1
    assert upload_events[0]["template_origin"] == "builtin"


@pytest.mark.asyncio
async def test_publisher_logs_github_template_origin(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    """A GitHub-bound render logs ``template_origin='org_default'``."""
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    template_store = DashboardGitHubTemplateStore(
        session=db_session, logger=logger
    )
    binding_store = DashboardGitHubTemplateBindingStore(
        session=db_session, logger=logger
    )

    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="origin-github-org",
                title="Origin GitHub Org",
                base_domain="origin-github.example.com",
            )
        )
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="origin-github-proj",
                title="Origin GitHub Project",
                source_url="https://github.com/example/origin-github",
            ),
        )
        template_result = await template_store.upsert(
            key=GitHubTemplateKey(
                github_owner="acme",
                github_repo="dashboard-templates",
                github_ref="main",
                root_path="/",
            ),
            commit_sha="deadbeef",
            etag="etag-1",
            template_toml=_CUSTOM_TEMPLATE_TOML,
            files=[
                GitHubTemplateFileInput(
                    relative_path="dashboard.html.jinja",
                    is_text=True,
                    data=_CUSTOM_DASHBOARD_JINJA,
                ),
            ],
        )
        binding = await binding_store.create(
            DashboardGitHubTemplateBindingCreate(
                org_id=org.id,
                project_id=None,
                github_owner="acme",
                github_repo="dashboard-templates",
                github_ref="main",
                root_path="/",
            )
        )
        await binding_store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="succeeded",
            github_template_id=template_result.template.id,
        )
        await db_session.commit()

    publisher = _make_publisher(db_session, discovery_client)
    mock_store = MockObjectStore()

    async def _provider() -> MockObjectStore:
        return mock_store

    with capture_logs() as captured:
        async with db_session.begin():
            await publisher.publish(
                org_id=org.id,
                project_id=project.id,
                object_store_provider=_provider,
            )

    upload_events = [
        event
        for event in captured
        if event.get("event") == "Uploaded dashboard artifacts"
    ]
    assert len(upload_events) == 1
    assert upload_events[0]["template_origin"] == "org_default"
