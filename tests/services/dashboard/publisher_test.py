"""Integration tests for DashboardPublisher."""

from __future__ import annotations

import asyncio
import json

import pytest
import structlog
from rubin.repertoire import DiscoveryClient
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.models import (
    BuildCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse_server.domain.dashboard_context import DashboardContext
from docverse_server.services.dashboard.publisher import DashboardPublisher
from docverse_server.services.dashboard_templates.resolver import (
    ResolvedTemplate,
    ResolvedTemplateOrigin,
    TemplateResolver,
)
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
    GitHubTemplateFileInput,
    GitHubTemplateKey,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.objectstore import MockObjectStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore

_HASH = "sha256:" + "a" * 64

_UPLOAD_CONCURRENCY = 4
"""Upload window for tests that do not exercise the fan-out itself."""


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
                source_url="https://example.com/example/pub",
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
            upload_concurrency=_UPLOAD_CONCURRENCY,
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
                source_url="https://example.com/example/per-ed",
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
            upload_concurrency=_UPLOAD_CONCURRENCY,
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
                source_url="https://example.com/example/empty",
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
            upload_concurrency=_UPLOAD_CONCURRENCY,
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
                source_url="https://example.com/example/resolve-builtin",
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
                source_url="https://example.com/example/render-provided",
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
            upload_concurrency=_UPLOAD_CONCURRENCY,
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
                source_url="https://example.com/example/origin-builtin",
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
                upload_concurrency=_UPLOAD_CONCURRENCY,
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
                source_url="https://example.com/example/origin-github",
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
                upload_concurrency=_UPLOAD_CONCURRENCY,
            )

    upload_events = [
        event
        for event in captured
        if event.get("event") == "Uploaded dashboard artifacts"
    ]
    assert len(upload_events) == 1
    assert upload_events[0]["template_origin"] == "org_default"


# ── Edition upload fan-out (#863) ──────────────────────────────────────────

_FAN_OUT_CAP = 3
"""Upload window the fan-out tests run ``render_and_upload`` under."""

_FAN_OUT_DRAFTS = _FAN_OUT_CAP * 4
"""Draft editions the fan-out tests seed: several windows' worth."""


async def _seed_fan_out_project(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
    *,
    slug: str,
) -> tuple[DashboardPublisher, DashboardContext, ResolvedTemplate]:
    """Seed a project with more editions than the upload window.

    Returns a publisher with the rendered context and resolved template
    for the project, ready for :meth:`DashboardPublisher.render_and_upload`.
    The project has ``__main`` plus :data:`_FAN_OUT_DRAFTS` drafts.
    """
    logger = _logger()
    edition_store = EditionStore(session=db_session, logger=logger)
    async with db_session.begin():
        org = await OrganizationStore(
            session=db_session, logger=logger
        ).create(
            OrganizationCreate(
                slug=f"{slug}-org",
                title="Fan-out Org",
                base_domain=f"{slug}.example.com",
            )
        )
        project = await ProjectStore(session=db_session, logger=logger).create(
            org_id=org.id,
            data=ProjectCreate(
                slug=slug,
                title="Fan-out Project",
                source_url=f"https://example.com/example/{slug}",
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
        for index in range(_FAN_OUT_DRAFTS):
            await edition_store.create_internal(
                project_id=project.id,
                slug=f"draft-{index:02d}",
                title=f"draft-{index:02d}",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": f"tickets/draft-{index:02d}"},
            )
        await db_session.commit()

    publisher = _make_publisher(db_session, discovery_client)
    async with db_session.begin():
        context = await publisher.build_context(
            org_id=org.id, project_id=project.id
        )
        resolved = await publisher.resolve_template(
            org_id=org.id, project_id=project.id
        )
    return publisher, context, resolved


def _fixed_keys(slug: str) -> list[str]:
    """Return the three project-level artifacts in the order they land."""
    return [
        f"{slug}/__dashboard.html",
        f"{slug}/__404.html",
        f"{slug}/__switcher.json",
    ]


class _UploadTimelineStore(MockObjectStore):
    """``MockObjectStore`` that records when each upload starts and lands.

    Each upload stays in flight across a real ``await``, so uploads the
    caller did not hold back overlap, and pile up in :attr:`peak`.
    """

    def __init__(self) -> None:
        super().__init__()
        self.timeline: list[tuple[str, str]] = []
        self._in_flight = 0
        self.peak = 0

    async def upload_object(
        self, *, key: str, data: bytes, content_type: str
    ) -> int:
        """Store the object, recording its start and end on the timeline."""
        self.timeline.append(("start", key))
        self._in_flight += 1
        self.peak = max(self.peak, self._in_flight)
        try:
            await asyncio.sleep(0.01)
            return await super().upload_object(
                key=key, data=data, content_type=content_type
            )
        finally:
            self._in_flight -= 1
            self.timeline.append(("end", key))


@pytest.mark.asyncio
async def test_render_and_upload_lands_project_artifacts_first(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    """The landing page, 404 and switcher land, in order, before editions.

    The edition JSONs now fan out concurrently, but a reader must never
    see a project's edition files ahead of the pages that link them, so
    the three project-level artifacts still go one at a time, in order,
    and the first edition upload starts only after the switcher landed.
    """
    slug = "fan-order"
    publisher, context, resolved = await _seed_fan_out_project(
        db_session, discovery_client, slug=slug
    )
    store = _UploadTimelineStore()

    async with store:
        await publisher.render_and_upload(
            context=context,
            object_store=store,
            resolved=resolved,
            upload_concurrency=_FAN_OUT_CAP,
        )

    fixed = _fixed_keys(slug)
    assert store.timeline[: 2 * len(fixed)] == [
        (step, key) for key in fixed for step in ("start", "end")
    ]
    rest = store.timeline[2 * len(fixed) :]
    assert len(rest) == 2 * (_FAN_OUT_DRAFTS + 1)
    assert all(key.startswith(f"{slug}/__editions/") for _, key in rest)


@pytest.mark.asyncio
async def test_render_and_upload_fans_editions_out_to_the_window(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    """Edition JSONs upload concurrently, never more than the window.

    One PUT at a time cost ``ts-xml`` (1,200 editions) about 2 s each
    and arq cancelled every rebuild at 300 s (#863). The editions now go
    up ``upload_concurrency`` at a time: with several windows' worth of
    editions, the store sees exactly that many in flight at its peak.
    """
    publisher, context, resolved = await _seed_fan_out_project(
        db_session, discovery_client, slug="fan-window"
    )
    store = _UploadTimelineStore()

    async with store:
        await publisher.render_and_upload(
            context=context,
            object_store=store,
            resolved=resolved,
            upload_concurrency=_FAN_OUT_CAP,
        )

    assert store.peak == _FAN_OUT_CAP


@pytest.mark.asyncio
async def test_render_and_upload_counts_every_artifact(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    """Concurrent edition uploads report the same progress as sequential ones.

    ``object_count`` is every artifact uploaded and ``total_size_bytes``
    their summed size, exactly what the one-at-a-time loop reported, so
    the job's progress and ``dashboard_built`` metric read the same.
    """
    slug = "fan-counts"
    publisher, context, resolved = await _seed_fan_out_project(
        db_session, discovery_client, slug=slug
    )
    store = _UploadTimelineStore()

    async with store:
        progress = await publisher.render_and_upload(
            context=context,
            object_store=store,
            resolved=resolved,
            upload_concurrency=_FAN_OUT_CAP,
        )

    assert set(store.objects) == {
        *_fixed_keys(slug),
        f"{slug}/__editions/__main.json",
        *(
            f"{slug}/__editions/draft-{index:02d}.json"
            for index in range(_FAN_OUT_DRAFTS)
        ),
    }
    assert progress.object_count == len(store.objects)
    assert progress.total_size_bytes == sum(
        len(stored.data) for stored in store.objects.values()
    )


class _FirstEditionFailsStore(MockObjectStore):
    """``MockObjectStore`` whose first edition upload fails.

    The project-level artifacts land. Every edition upload after the
    first hangs until it is cancelled, and counts the cancellation, so a
    test can tell an upload torn down with the render from one orphaned
    to run on after it.
    """

    def __init__(self) -> None:
        super().__init__()
        self.attempted: list[str] = []
        self.cancelled = 0

    async def upload_object(
        self, *, key: str, data: bytes, content_type: str
    ) -> int:
        """Fail the first edition, hang every later one until cancelled."""
        if "/__editions/" not in key:
            return await super().upload_object(
                key=key, data=data, content_type=content_type
            )
        self.attempted.append(key)
        if len(self.attempted) == 1:
            # Long enough for the rest of the window to start uploading.
            await asyncio.sleep(0.01)
            msg = "Object store went away"
            raise RuntimeError(msg)
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        msg = "unreachable: the upload hangs until it is cancelled"
        raise AssertionError(msg)


@pytest.mark.asyncio
async def test_render_and_upload_edition_failure_cancels_the_rest(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    """The first failed edition upload stops the rest and raises as itself.

    No further edition upload starts, the ones already in flight are
    cancelled rather than left running after the job has failed, and the
    store's own error comes out — not an exception group — so the
    worker's ``except Exception`` records it and Sentry sees the fault.
    """
    publisher, context, resolved = await _seed_fan_out_project(
        db_session, discovery_client, slug="fan-fails"
    )
    store = _FirstEditionFailsStore()

    with pytest.raises(RuntimeError, match=r"^Object store went away$"):
        async with store:
            await publisher.render_and_upload(
                context=context,
                object_store=store,
                resolved=resolved,
                upload_concurrency=_FAN_OUT_CAP,
            )

    assert 1 < len(store.attempted) <= _FAN_OUT_CAP
    assert store.cancelled == len(store.attempted) - 1
