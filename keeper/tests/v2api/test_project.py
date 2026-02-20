"""Test /v2/ APIs for projects."""

from __future__ import annotations

from typing import TYPE_CHECKING

from mock import MagicMock

from keeper.testutils import MockTaskQueue

if TYPE_CHECKING:
    from unittest.mock import Mock

    from keeper.testutils import TestClient


def test_projects(client: TestClient, mocker: Mock) -> None:
    task_queue = mocker.patch(
        "keeper.taskrunner.inspect_task_queue", return_value=None
    )
    task_queue = MockTaskQueue(mocker)  # noqa

    mock_presigned_url = {
        "url": "https://example.com",
        "fields": {"key": "a/b/${filename}"},
    }
    presign_post_mock = mocker.patch(
        "keeper.services.createbuild.presign_post_url_for_prefix",
        new=MagicMock(return_value=mock_presigned_url),
    )
    presign_post_mock = mocker.patch(
        "keeper.services.createbuild.presign_post_url_for_directory_object",
        new=MagicMock(return_value=mock_presigned_url),
    )
    s3_resource_mock = mocker.patch(
        "keeper.services.createbuild.open_s3_resource"
    )

    # Create a default organization ===========================================
    mocker.resetall()

    request_data = {
        "slug": "test1",
        "title": "Test 1",
        "layout": "subdomain",
        "domain": "example.org",
        "path_prefix": "/",
        "s3_bucket": "test-bucket",
        "fastly_support": True,
        "fastly_domain": "fastly.example.org",
        "fastly_service_id": "abc",
        "fastly_api_key": "123",
    }
    r = client.post("/v2/orgs", request_data)
    task_queue.apply_task_side_effects()
    assert r.status == 201
    org1_url = r.headers["Location"]
    org1_projects_url = r.json["projects_url"]

    # Get project list (should be empty) ======================================
    mocker.resetall()

    r = client.get(org1_projects_url)
    assert r.status == 200
    assert len(r.json) == 0

    # Create a project ========================================================
    mocker.resetall()

    request_data = {
        "slug": "alpha",
        "title": "Alpha",
        "source_repo_url": "https://github.com/example/alpha",
        "default_edition_mode": "git_ref",
    }
    r = client.post(org1_projects_url, request_data)
    task_queue.apply_task_side_effects()
    assert r.status == 201
    project1_url = r.headers["Location"]
    project1_data = r.json

    assert project1_data["organization_url"] == org1_url
    assert project1_data["self_url"] == project1_url
    assert project1_data["published_url"] == "https://alpha.example.org"
    project1_builds_url = project1_data["builds_url"]
    project1_default_edition_url = project1_data["default_edition"]["self_url"]

    # Get project list again ==================================================
    mocker.resetall()

    r = client.get(org1_projects_url)
    assert r.status == 200
    assert r.json[0] == project1_data

    # Update title ============================================================
    mocker.resetall()

    request_data = {"title": "Alpha Docs"}
    r = client.patch(project1_url, request_data)
    task_queue.apply_task_side_effects()
    assert r.status == 200
    assert r.json["title"] == "Alpha Docs"

    # Create a build ==========================================================
    mocker.resetall()

    build1_request = {"git_ref": "main"}
    r = client.post(project1_builds_url, build1_request)
    task_queue.apply_task_side_effects()
    s3_resource_mock.assert_called_once()
    presign_post_mock.assert_called_once()
    assert r.status == 201
    assert r.json["project_url"] == project1_url
    assert r.json["date_created"] is not None
    assert r.json["date_ended"] is None
    assert r.json["uploaded"] is False
    assert r.json["published_url"] == "https://alpha.example.org/builds/1"
    assert "post_prefix_urls" in r.json
    assert "post_dir_urls" in r.json
    assert len(r.json["surrogate_key"]) == 32  # should be a uuid4 -> hex
    build1_url = r.headers["Location"]

    # ========================================================================
    # List builds
    mocker.resetall()

    r = client.get(project1_builds_url)
    assert len(r.json) == 1

    # ========================================================================
    # Register upload
    mocker.resetall()

    r = client.patch(build1_url, {"uploaded": True})
    task_queue.apply_task_side_effects()
    assert r.status == 202

    task_queue.assert_launched_once()
    task_queue.assert_edition_build_v2(
        project1_default_edition_url,
        build1_url,
    )
    task_queue.assert_dashboard_build_v2(project1_url)

    r = client.get(build1_url)
    assert r.json["uploaded"] is True

    # =========================================================================
    # Get the default edition
    mocker.resetall()
    r = client.get(project1_default_edition_url)
    assert r.status == 200
    data = r.json

    # It should be tracking build 1 because it's of the main branch
    assert data["build_url"] == build1_url
