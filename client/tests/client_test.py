"""Tests for docverse._client."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest
import respx
from pydantic import ValidationError

from docverse._client import DEFAULT_UPDATED_SINCE_OVERLAP, DocverseClient
from docverse._exceptions import BuildProcessingError, DocverseClientError
from docverse.models import (
    KeeperSyncConfigUpdate,
    OrgMembershipUpdate,
    OrgRole,
)
from docverse.models.builds import BuildAnnotations, BuildStatus
from docverse.models.queue_enums import JobKind, JobStatus

BASE_URL = "https://docverse.example.com"
TOKEN = "test-token"

# Valid Crockford Base32 IDs with checksum (base32_lib.encode(N, ...))
BUILD_ID = "000000000195"
JOB_ID = "000000000292"
PROJECT_ID = "1w01-nw5r-173e-35"


def _build_response(**overrides: Any) -> dict[str, Any]:
    """Return a dict matching the Build model shape."""
    data: dict[str, Any] = {
        "self_url": "/orgs/testorg/projects/testproj/builds/" + BUILD_ID,
        "project_url": "/orgs/testorg/projects/testproj",
        "id": BUILD_ID,
        "git_ref": "main",
        "alternate_name": None,
        "content_hash": "sha256:" + "a" * 64,
        "status": "pending",
        "upload_url": "https://storage.example.com/presigned-put",
        "job_url": None,
        "object_count": None,
        "total_size_bytes": None,
        "uploader": "ci-bot",
        "annotations": None,
        "date_created": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        "date_uploaded": None,
        "date_completed": None,
    }
    data.update(overrides)
    return data


def _job_response(**overrides: Any) -> dict[str, Any]:
    """Return a dict matching the QueueJob model shape."""
    data: dict[str, Any] = {
        "self_url": "/orgs/testorg/jobs/" + JOB_ID,
        "id": JOB_ID,
        "kind": "build_processing",
        "status": "completed",
        "phase": None,
        "progress": None,
        "errors": None,
        "date_created": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        "date_started": None,
        "date_completed": None,
    }
    data.update(overrides)
    return data


def _project_response(slug: str, **overrides: Any) -> dict[str, Any]:
    """Return a dict matching the Project model shape."""
    project_url = f"/orgs/myorg/projects/{slug}"
    data: dict[str, Any] = {
        "self_url": project_url,
        "org_url": "/orgs/myorg",
        "editions_url": f"{project_url}/editions",
        "builds_url": f"{project_url}/builds",
        "dashboard_template_url": f"{project_url}/dashboard-template",
        "id": PROJECT_ID,
        "slug": slug,
        "title": slug.capitalize(),
        "date_created": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        "date_updated": datetime(2026, 1, 2, tzinfo=UTC).isoformat(),
        "date_deleted": None,
    }
    data.update(overrides)
    return data


@pytest.mark.asyncio
async def test_create_build() -> None:
    """POST to builds endpoint with correct JSON and auth header."""
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.post("/orgs/myorg/projects/myproj/builds").mock(
            return_value=httpx.Response(201, json=_build_response())
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            build = await client.create_build(
                "myorg",
                "myproj",
                git_ref="main",
                content_hash="sha256:" + "a" * 64,
            )

        assert route.called
        request = route.calls[0].request
        assert request.headers["authorization"] == f"Bearer {TOKEN}"
        assert build.id == BUILD_ID
        assert build.status == BuildStatus.pending


@pytest.mark.asyncio
async def test_create_build_omits_content_hash_by_default() -> None:
    """``content_hash`` is left out of the payload when not supplied.

    The server fills a placeholder for a build whose digest it was not
    given, so the client must omit the key rather than send ``null`` —
    the field's ``sha256:`` pattern applies to whatever is sent.
    """
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.post("/orgs/myorg/projects/myproj/builds").mock(
            return_value=httpx.Response(
                201, json=_build_response(content_hash="sha256:" + "0" * 64)
            )
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            await client.create_build("myorg", "myproj", git_ref="main")

        assert route.called
        body = json.loads(route.calls[0].request.content)
        assert body == {"git_ref": "main"}


@pytest.mark.asyncio
async def test_create_build_with_annotations() -> None:
    """Annotations are included in the POST payload."""
    annotations = BuildAnnotations(
        commit_sha="abc123", ci_platform="github-actions"
    )
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.post("/orgs/myorg/projects/myproj/builds").mock(
            return_value=httpx.Response(
                201,
                json=_build_response(
                    annotations={
                        "commit_sha": "abc123",
                        "ci_platform": "github-actions",
                    }
                ),
            )
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            build = await client.create_build(
                "myorg",
                "myproj",
                git_ref="main",
                content_hash="sha256:" + "a" * 64,
                annotations=annotations,
            )

        assert route.called
        request = route.calls[0].request
        body = json.loads(request.content)
        assert body["annotations"]["commit_sha"] == "abc123"
        assert body["annotations"]["ci_platform"] == "github-actions"
        assert build.annotations is not None
        assert build.annotations.commit_sha == "abc123"


@pytest.mark.asyncio
async def test_upload_tarball(tmp_path: Path) -> None:
    """PUT to presigned URL without Authorization header."""
    tarball = tmp_path / "docs.tar.gz"
    tarball.write_bytes(b"fake-tarball-content")

    presigned_url = "https://storage.example.com/presigned-put"

    with respx.mock() as router:
        route = router.put(presigned_url).mock(
            return_value=httpx.Response(200)
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            await client.upload_tarball(presigned_url, tarball)

        assert route.called
        request = route.calls[0].request
        # Security invariant: no auth token on presigned upload
        assert "authorization" not in request.headers


@pytest.mark.asyncio
async def test_complete_upload() -> None:
    """PATCH to build self_url with uploaded status."""
    build_url = "/orgs/testorg/projects/testproj/builds/" + BUILD_ID
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.patch(build_url).mock(
            return_value=httpx.Response(
                200,
                json=_build_response(
                    status="uploaded",
                    job_url="/orgs/testorg/jobs/" + JOB_ID,
                ),
            )
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            build = await client.complete_upload(build_url)

        assert route.called
        assert build.status == BuildStatus.uploaded
        assert build.job_url == "/orgs/testorg/jobs/" + JOB_ID


@pytest.mark.asyncio
async def test_get_job() -> None:
    """GET to job URL returns parsed QueueJob."""
    job_url = "/orgs/testorg/jobs/" + JOB_ID
    async with respx.mock(base_url=BASE_URL) as router:
        router.get(job_url).mock(
            return_value=httpx.Response(
                200,
                json=_job_response(status="completed"),
            )
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            job = await client.get_job(job_url)

    assert job.id == JOB_ID
    assert job.status == JobStatus.completed
    assert job.kind == JobKind.build_processing


@pytest.mark.asyncio
async def test_wait_for_job_completed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Polls twice then returns completed job."""
    monkeypatch.setattr("docverse._client._BACKOFF_INITIAL", 0)

    queue_url = "/queue/jobs/" + JOB_ID
    responses = [
        httpx.Response(200, json=_job_response(status="in_progress")),
        httpx.Response(200, json=_job_response(status="completed")),
    ]
    async with respx.mock(base_url=BASE_URL) as router:
        router.get(queue_url).mock(side_effect=responses)

        with patch("docverse._client.asyncio.sleep"):
            async with DocverseClient(BASE_URL, TOKEN) as client:
                job = await client.wait_for_job(queue_url)

    assert job.status == JobStatus.completed


@pytest.mark.asyncio
async def test_wait_for_job_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Failed status raises BuildProcessingError with the job attached."""
    monkeypatch.setattr("docverse._client._BACKOFF_INITIAL", 0)

    queue_url = "/queue/jobs/" + JOB_ID
    async with respx.mock(base_url=BASE_URL) as router:
        router.get(queue_url).mock(
            return_value=httpx.Response(
                200,
                json=_job_response(status="failed", phase="inventory"),
            )
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            with pytest.raises(BuildProcessingError) as exc_info:
                await client.wait_for_job(queue_url)

    assert exc_info.value.job.status == JobStatus.failed
    assert exc_info.value.job.phase == "inventory"


@pytest.mark.asyncio
async def test_list_organizations() -> None:
    """GET /orgs returns parsed OrganizationSummary entries."""
    payload = [
        {
            "self_url": "/orgs/org-a",
            "id": "1vzr-eky0-0vvf-43",
            "slug": "org-a",
            "title": "Org A",
            "role": "admin",
        },
        {
            "self_url": "/orgs/org-b",
            "id": "1w01-nw5r-173e-35",
            "slug": "org-b",
            "title": "Org B",
            "role": "reader",
        },
    ]
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.get("/orgs").mock(
            return_value=httpx.Response(200, json=payload)
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            orgs = await client.list_organizations()

    assert route.called
    assert [o.slug for o in orgs] == ["org-a", "org-b"]
    assert orgs[0].role == OrgRole.admin
    assert orgs[1].role == OrgRole.reader
    assert orgs[0].title == "Org A"


@pytest.mark.asyncio
async def test_list_organizations_empty() -> None:
    """GET /orgs with no memberships returns an empty list."""
    async with respx.mock(base_url=BASE_URL) as router:
        router.get("/orgs").mock(return_value=httpx.Response(200, json=[]))
        async with DocverseClient(BASE_URL, TOKEN) as client:
            orgs = await client.list_organizations()

    assert orgs == []


@pytest.mark.asyncio
async def test_update_member() -> None:
    """PATCH a member's role returns the updated OrgMembership."""
    payload = {
        "self_url": "/orgs/myorg/members/user:jdoe",
        "org_url": "/orgs/myorg",
        "id": "user:jdoe",
        "principal": "jdoe",
        "principal_type": "user",
        "role": "admin",
    }
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.patch("/orgs/myorg/members/user:jdoe").mock(
            return_value=httpx.Response(200, json=payload)
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            member = await client.update_member(
                "myorg", "user:jdoe", role=OrgRole.admin
            )

    assert route.called
    assert json.loads(route.calls[0].request.content) == {"role": "admin"}
    assert member.role == OrgRole.admin
    assert member.principal == "jdoe"


@pytest.mark.asyncio
async def test_update_keeper_sync_config() -> None:
    """PATCH keeper-sync sends only set fields and returns the config."""
    payload = {
        "enabled": False,
        "ltd_base_url": "https://keeper.lsst.codes/",
        "project_slugs": ["dmtn-001"],
    }
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.patch("/orgs/myorg/keeper-sync").mock(
            return_value=httpx.Response(200, json=payload)
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            config = await client.update_keeper_sync_config(
                "myorg", KeeperSyncConfigUpdate(enabled=False)
            )

    assert route.called
    assert json.loads(route.calls[0].request.content) == {"enabled": False}
    assert config.enabled is False
    assert config.project_slugs == ["dmtn-001"]


@pytest.mark.asyncio
async def test_update_keeper_sync_config_ignores_unknown_response_field() -> (
    None
):
    """A response field this client release predates does not break parsing.

    The write has already applied by the time the response is parsed, so
    failing here would report an error for a change that did land. The
    unknown field is dropped and the known ones come back intact.
    """
    payload = {
        "enabled": True,
        "ltd_base_url": "https://keeper.lsst.codes/",
        "project_slugs": ["dmtn-001"],
        "future_scope_field": ["not-yet-a-thing"],
    }
    async with respx.mock(base_url=BASE_URL) as router:
        router.patch("/orgs/myorg/keeper-sync").mock(
            return_value=httpx.Response(200, json=payload)
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            config = await client.update_keeper_sync_config(
                "myorg", KeeperSyncConfigUpdate(enabled=True)
            )

    assert config.enabled is True
    assert config.project_slugs == ["dmtn-001"]
    assert "future_scope_field" not in config.model_dump()


def test_org_membership_update_forbids_identity_fields() -> None:
    """OrgMembershipUpdate rejects principal/principal_type fields."""
    with pytest.raises(ValidationError):
        OrgMembershipUpdate.model_validate({"principal": "someone-else"})
    with pytest.raises(ValidationError):
        OrgMembershipUpdate.model_validate({"principal_type": "group"})


@pytest.mark.asyncio
async def test_http_error() -> None:
    """Non-2xx response raises DocverseClientError with status_code."""
    async with respx.mock(base_url=BASE_URL) as router:
        router.post("/orgs/x/projects/y/builds").mock(
            return_value=httpx.Response(403, text="Forbidden")
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            with pytest.raises(DocverseClientError) as exc_info:
                await client.create_build(
                    "x",
                    "y",
                    git_ref="main",
                    content_hash="sha256:" + "a" * 64,
                )

    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_restore_build() -> None:
    """POST to the restore sub-resource returns the revived build.

    The restored build keeps whatever status the delete left it with —
    the server does not re-run it — so the round-trip has to carry the
    status through rather than assume the build is live again.
    """
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.post(
            f"/orgs/myorg/projects/myproj/builds/{BUILD_ID}/restore"
        ).mock(
            return_value=httpx.Response(
                200,
                json=_build_response(
                    status="cancelled",
                    upload_url=None,
                    date_purged=None,
                ),
            )
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            build = await client.restore_build("myorg", "myproj", BUILD_ID)

        assert route.called
        assert route.calls[0].request.headers["Authorization"] == (
            f"Bearer {TOKEN}"
        )
        assert build.id == BUILD_ID
        assert build.status == BuildStatus.cancelled
        assert build.date_purged is None


@pytest.mark.asyncio
async def test_restore_build_raises_on_purged_build() -> None:
    """A 409 becomes a client error rather than a silent miss.

    The server answers 409 when the build's content has already been
    reclaimed. Swallowing that would tell an operator their restore
    worked when the files are gone for good.
    """
    async with respx.mock(base_url=BASE_URL) as router:
        router.post(
            f"/orgs/myorg/projects/myproj/builds/{BUILD_ID}/restore"
        ).mock(return_value=httpx.Response(409, text="already purged"))
        async with DocverseClient(BASE_URL, TOKEN) as client:
            with pytest.raises(DocverseClientError) as excinfo:
                await client.restore_build("myorg", "myproj", BUILD_ID)

    assert excinfo.value.status_code == 409


@pytest.mark.asyncio
async def test_list_projects_follows_next_link() -> None:
    """Paging walks ``Link rel="next"`` to the end of the listing.

    A poller asks for an organization's projects once and gets all of
    them; the page boundary is the client's problem, not the caller's.
    The last page's ``Link`` carries only ``rel="first"``, which is how
    the walk knows to stop.
    """
    page_two_url = f"{BASE_URL}/orgs/myorg/projects?order=slug&cursor=c2"
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.get("/orgs/myorg/projects").mock(
            side_effect=[
                httpx.Response(
                    200,
                    json=[
                        _project_response("alpha"),
                        _project_response("beta"),
                    ],
                    headers={
                        "Link": (
                            f'<{BASE_URL}/orgs/myorg/projects>; rel="first",'
                            f' <{page_two_url}>; rel="next"'
                        )
                    },
                ),
                httpx.Response(
                    200,
                    json=[_project_response("gamma")],
                    headers={
                        "Link": (
                            f'<{BASE_URL}/orgs/myorg/projects>; rel="first"'
                        )
                    },
                ),
            ]
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            result = await client.list_projects("myorg")

    assert [p.slug for p in result.projects] == ["alpha", "beta", "gamma"]
    assert len(route.calls) == 2
    assert str(route.calls[1].request.url) == page_two_url
    assert result.not_modified is False


@pytest.mark.asyncio
async def test_list_projects_forwards_query_parameters() -> None:
    """The poll knobs reach the server as query parameters.

    ``updated_since`` goes out as an ISO 8601 timestamp carrying its
    offset, because the server rejects a naive one with a 422. The
    overlap is switched off here so the timestamp on the wire is the
    one the caller passed; the default backdating has its own test.
    """
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.get("/orgs/myorg/projects").mock(
            return_value=httpx.Response(200, json=[])
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            await client.list_projects(
                "myorg",
                updated_since=datetime(2026, 2, 1, 12, 30, tzinfo=UTC),
                updated_since_overlap=timedelta(0),
                include_deleted=True,
                order="date_updated",
            )

    params = route.calls[0].request.url.params
    assert params["updated_since"] == "2026-02-01T12:30:00+00:00"
    assert params["include_deleted"] == "true"
    assert params["order"] == "date_updated"


@pytest.mark.asyncio
async def test_list_projects_backdates_updated_since_by_the_overlap() -> None:
    """``updated_since`` goes out backdated by the overlap window.

    A project's ``date_updated`` is PostgreSQL's transaction *start*
    clock, and commit order is not start order, so a write that began
    before the caller's last poll can become visible after it, wearing
    a timestamp the caller has already passed. Asking from slightly
    earlier than the caller believes it needs is what keeps that row
    from being skipped forever.
    """
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.get("/orgs/myorg/projects").mock(
            return_value=httpx.Response(200, json=[])
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            await client.list_projects(
                "myorg",
                updated_since=datetime(2026, 2, 1, 12, 30, tzinfo=UTC),
            )

    params = route.calls[0].request.url.params
    expected = datetime(2026, 2, 1, 12, 30, tzinfo=UTC) - (
        DEFAULT_UPDATED_SINCE_OVERLAP
    )
    assert params["updated_since"] == expected.isoformat()
    assert timedelta(seconds=60) == DEFAULT_UPDATED_SINCE_OVERLAP


@pytest.mark.asyncio
async def test_list_projects_sends_preconditions_on_first_page_only() -> None:
    """The validator guards the first request and no other.

    The caller's ``ETag`` describes the page it already holds, so it is
    meaningless against the cursor URLs that follow — a 304 there would
    silently truncate the listing. The first page's tag comes back on
    the result so the next poll can send it again.
    """
    page_two_url = f"{BASE_URL}/orgs/myorg/projects?order=slug&cursor=c2"
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.get("/orgs/myorg/projects").mock(
            side_effect=[
                httpx.Response(
                    200,
                    json=[_project_response("alpha")],
                    headers={
                        "ETag": 'W/"abc123"',
                        "Link": f'<{page_two_url}>; rel="next"',
                    },
                ),
                httpx.Response(
                    200,
                    json=[_project_response("beta")],
                    headers={"ETag": 'W/"def456"'},
                ),
            ]
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            result = await client.list_projects(
                "myorg", if_none_match='W/"stale"'
            )

    first, second = (call.request for call in route.calls)
    assert first.headers["If-None-Match"] == 'W/"stale"'
    assert "if-modified-since" not in first.headers
    assert "if-none-match" not in second.headers
    assert result.etag == 'W/"abc123"'


@pytest.mark.asyncio
async def test_list_projects_not_modified() -> None:
    """A 304 on the first page ends the poll before any paging.

    The listing is unchanged, so there is nothing to fetch and nothing
    to page through; the empty ``projects`` list is explained by
    ``not_modified`` rather than mistaken for an empty organization.
    """
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.get("/orgs/myorg/projects").mock(
            return_value=httpx.Response(304, headers={"ETag": 'W/"abc123"'})
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            result = await client.list_projects(
                "myorg", if_none_match='W/"abc123"'
            )

    assert result.not_modified is True
    assert result.projects == []
    assert result.etag == 'W/"abc123"'
    assert len(route.calls) == 1


@pytest.mark.asyncio
async def test_list_projects_raises_on_error_status() -> None:
    """A non-2xx page is an error, not an empty listing."""
    async with respx.mock(base_url=BASE_URL) as router:
        router.get("/orgs/myorg/projects").mock(
            return_value=httpx.Response(403, text="Forbidden")
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            with pytest.raises(DocverseClientError) as excinfo:
                await client.list_projects("myorg")

    assert excinfo.value.status_code == 403
