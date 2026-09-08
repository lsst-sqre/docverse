"""Tests for build endpoints."""

from __future__ import annotations

import pytest
import structlog
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher
from sqlalchemy import select

from docverse.models import BuildAnnotations, BuildStatus
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dependencies.context import context_dependency
from docverse_server.domain.base32id import validate_base32_id
from docverse_server.domain.build import Build
from docverse_server.domain.content_hash import PLACEHOLDER_CONTENT_HASH
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from tests.conftest import seed_build, seed_org_with_admin

CONTENT_HASH = (
    "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
)


async def _setup(client: AsyncClient) -> None:
    """Create org, membership, and project."""
    await seed_org_with_admin(client, "build-org", "testuser")
    await client.post(
        "/docverse/orgs/build-org/projects",
        json={
            "slug": "build-proj",
            "title": "Build Project",
            "source_url": "https://example.com/example/build",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )


async def _transition_build(
    org_slug: str,
    project_slug: str,
    build_id: str,
    status: BuildStatus,
) -> None:
    """Step a seeded build from ``pending`` to a terminal status.

    Goes through the store rather than the API because no endpoint hands
    a build to ``superseded`` — only the worker's stale-skip path does,
    and this test is about the read side.
    """
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.get_by_slug(org_slug)
            assert org is not None
            project_store = ProjectStore(session=session, logger=logger)
            project = await project_store.get_by_slug(
                org_id=org.id, slug=project_slug
            )
            assert project is not None
            build_store = BuildStore(session=session, logger=logger)
            build = await build_store.get_by_public_id(
                project_id=project.id,
                public_id=validate_base32_id(build_id),
            )
            assert build is not None
            await build_store.transition_status(
                build_id=build.id, new_status=BuildStatus.processing
            )
            await build_store.transition_status(
                build_id=build.id, new_status=status
            )
            await session.commit()
        return


async def _read_deleted_build(
    org_slug: str,
    project_slug: str,
    build_id: str,
) -> Build:
    """Read a build row that ``BuildStore`` would hide as soft-deleted.

    ``BuildStore.get_by_public_id`` filters ``date_deleted IS NULL``, so
    checking what DELETE left behind means going to the table directly.
    """
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.get_by_slug(org_slug)
            assert org is not None
            project_store = ProjectStore(session=session, logger=logger)
            project = await project_store.get_by_slug(
                org_id=org.id, slug=project_slug
            )
            assert project is not None
            result = await session.execute(
                select(SqlBuild).where(
                    SqlBuild.project_id == project.id,
                    SqlBuild.public_id == validate_base32_id(build_id),
                )
            )
            return Build.model_validate(result.scalar_one())
    raise AssertionError("db_session_dependency yielded no session")


@pytest.mark.asyncio
async def test_create_build(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/build-org/projects/build-proj/builds",
        json={
            "git_ref": "main",
            "content_hash": CONTENT_HASH,
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422
    data = response.json()
    assert data["detail"][0]["type"] == "missing_configuration"
    assert "object store" in data["detail"][0]["msg"].lower()


async def _configure_staging_store(client: AsyncClient) -> None:
    """Give ``build-org`` an aws_s3 staging store so POST build can 201.

    Presigned-URL generation signs locally (no network), so an
    ``aws_s3`` service with placeholder credentials is enough to drive
    the create path end-to-end.
    """
    headers = {"X-Auth-Request-User": "testuser"}
    await client.post(
        "/docverse/orgs/build-org/credentials",
        json={
            "label": "aws-cred",
            "credentials": {
                "provider": "aws",
                "access_key_id": "AKIAEXAMPLE",
                "secret_access_key": "secret",
            },
        },
        headers=headers,
    )
    await client.post(
        "/docverse/orgs/build-org/services",
        json={
            "label": "my-s3",
            "credential_label": "aws-cred",
            "config": {
                "provider": "aws_s3",
                "bucket": "my-bucket",
                "region": "us-east-1",
            },
        },
        headers=headers,
    )
    await client.patch(
        "/docverse/orgs/build-org",
        json={"staging_store_label": "my-s3"},
        headers=headers,
    )


@pytest.mark.asyncio
async def test_create_build_sets_location_header(client: AsyncClient) -> None:
    """POST build returns 201 with ``Location`` == the build's self_url."""
    await _setup(client)
    await _configure_staging_store(client)
    response = await client.post(
        "/docverse/orgs/build-org/projects/build-proj/builds",
        json={
            "git_ref": "main",
            "content_hash": CONTENT_HASH,
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert response.headers["Location"] == data["self_url"]


@pytest.mark.asyncio
async def test_create_build_stores_client_content_hash(
    client: AsyncClient,
) -> None:
    """A client-supplied transport digest is stored on the pending row.

    The field is deprecated but still accepted, so an old client that
    sends its tarball digest sees it round-trip until the worker
    overwrites it with the server-computed content identity.
    """
    await _setup(client)
    await _configure_staging_store(client)
    response = await client.post(
        "/docverse/orgs/build-org/projects/build-proj/builds",
        json={
            "git_ref": "main",
            "content_hash": CONTENT_HASH,
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    assert response.json()["content_hash"] == CONTENT_HASH


@pytest.mark.asyncio
async def test_create_build_without_content_hash(client: AsyncClient) -> None:
    """Omitting the deprecated digest succeeds and stores the placeholder.

    ``builds.content_hash`` is ``NOT NULL``, so a pending row created
    without a client digest holds ``PLACEHOLDER_CONTENT_HASH`` until the
    worker writes the real content identity at completion.
    """
    await _setup(client)
    await _configure_staging_store(client)
    response = await client.post(
        "/docverse/orgs/build-org/projects/build-proj/builds",
        json={"git_ref": "main"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["content_hash"] == PLACEHOLDER_CONTENT_HASH
    assert data["status"] == "pending"

    # The placeholder is persisted, not just synthesized in the response.
    fetched = await client.get(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{data['id']}",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert fetched.status_code == 200
    assert fetched.json()["content_hash"] == PLACEHOLDER_CONTENT_HASH


@pytest.mark.asyncio
async def test_create_build_rejects_malformed_content_hash(
    client: AsyncClient,
) -> None:
    """The ``sha256:`` pattern is still enforced when the field is sent."""
    await _setup(client)
    await _configure_staging_store(client)
    response = await client.post(
        "/docverse/orgs/build-org/projects/build-proj/builds",
        json={"git_ref": "main", "content_hash": "not-a-digest"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_build_with_annotations(client: AsyncClient) -> None:
    """Annotations round-trip via DB seeding + GET (POST needs a store)."""
    await _setup(client)
    build_id = await seed_build(
        "build-org",
        "build-proj",
        annotations=BuildAnnotations.model_validate(
            {
                "commit_sha": "abc123",
                "ci_platform": "github-actions",
                "custom_key": "custom_value",
            }
        ),
    )
    response = await client.get(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["annotations"]["commit_sha"] == "abc123"
    assert data["annotations"]["ci_platform"] == "github-actions"
    assert data["annotations"]["custom_key"] == "custom_value"


@pytest.mark.asyncio
async def test_list_builds(client: AsyncClient) -> None:
    await _setup(client)
    await seed_build("build-org", "build-proj")
    response = await client.get(
        "/docverse/orgs/build-org/projects/build-proj/builds",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert len(response.json()) >= 1
    assert "Link" in response.headers
    assert "X-Total-Count" in response.headers


@pytest.mark.asyncio
async def test_list_builds_filters_by_superseded_status(
    client: AsyncClient,
) -> None:
    """``?status=superseded`` is accepted and selects only those builds.

    The filter is typed as ``BuildStatus``, so widening the enum is what
    opens the query up — an operator chasing a ref that never published
    can now ask for exactly the builds a newer build took over, without
    them being lumped in with ``failed``.
    """
    await _setup(client)
    superseded_id = await seed_build("build-org", "build-proj")
    pending_id = await seed_build("build-org", "build-proj")
    await _transition_build(
        "build-org", "build-proj", superseded_id, BuildStatus.superseded
    )

    response = await client.get(
        "/docverse/orgs/build-org/projects/build-proj/builds",
        params={"status": "superseded"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    ids = [build["id"] for build in response.json()]
    assert ids == [superseded_id]
    assert pending_id not in ids


@pytest.mark.asyncio
async def test_get_build(client: AsyncClient) -> None:
    await _setup(client)
    build_id = await seed_build("build-org", "build-proj")
    response = await client.get(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["id"] == build_id


@pytest.mark.asyncio
async def test_get_build_malformed_id(client: AsyncClient) -> None:
    """GET with a malformed base32 build ID returns 422."""
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/build-org/projects/build-proj/builds/not-a-valid-id",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_patch_build_upload_complete(client: AsyncClient) -> None:
    await _setup(client)
    build_id = await seed_build("build-org", "build-proj")
    response = await client.patch(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        json={"status": "uploaded"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "processing"
    assert data["job_url"] is not None
    assert "/orgs/build-org/jobs/" in data["job_url"]
    # The job_url resolves via the org-scoped GET.
    job_response = await client.get(
        data["job_url"],
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert job_response.status_code == 200


@pytest.mark.asyncio
async def test_patch_build_publishes_build_uploaded(
    client: AsyncClient,
) -> None:
    """PATCH status=uploaded emits one build_uploaded with provenance."""
    await _setup(client)
    build_id = await seed_build(
        "build-org",
        "build-proj",
        uploader="ci-bot",
        annotations=BuildAnnotations.model_validate(
            {
                "commit_sha": "abc123",
                "github_run_id": "42",
                "ci_platform": "github-actions",
            }
        ),
    )
    response = await client.patch(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        json={"status": "uploaded"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    events = context_dependency._events
    assert events is not None
    publisher = events.build_uploaded
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == "build-org"
    assert event.project == "build-proj"
    assert event.uploader == "ci-bot"
    assert event.commit_sha == "abc123"
    assert event.github_run_id == "42"
    assert event.ci_platform == "github-actions"
    # Provenance fields the uploader did not annotate are null.
    assert event.github_repository is None
    assert event.github_actor is None


@pytest.mark.asyncio
async def test_patch_build_noop_does_not_publish_build_uploaded(
    client: AsyncClient,
) -> None:
    """A non-uploaded PATCH must not emit a build_uploaded event."""
    await _setup(client)
    build_id = await seed_build("build-org", "build-proj")
    response = await client.patch(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        json={"status": "pending"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    events = context_dependency._events
    assert events is not None
    publisher = events.build_uploaded
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 0


@pytest.mark.asyncio
async def test_delete_build(client: AsyncClient) -> None:
    await _setup(client)
    build_id = await seed_build("build-org", "build-proj")
    response = await client.delete(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_delete_build_cancels_pending_build(
    client: AsyncClient,
) -> None:
    """DELETE on an unfinished build cancels it as well as deleting it.

    The 204 contract is unchanged; what changes is the row left behind.
    A deleted build must not stay ``pending``, because the worker's
    supersession and reaper logic both read status as a claim about
    whether anyone is still going to publish the build.
    """
    await _setup(client)
    build_id = await seed_build("build-org", "build-proj")
    response = await client.delete(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 204

    row = await _read_deleted_build("build-org", "build-proj", build_id)
    assert row.status == BuildStatus.cancelled
    assert row.date_deleted is not None
    assert row.date_completed is not None
