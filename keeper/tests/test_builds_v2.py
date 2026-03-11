"""Test v2 APIs for build resources."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pydantic
import pytest
from mock import MagicMock
from werkzeug.exceptions import NotFound

from keeper.exceptions import ValidationError
from keeper.mediatypes import v2_json_type
from keeper.testutils import MockTaskQueue

if TYPE_CHECKING:
    from unittest.mock import Mock

    from keeper.testutils import TestClient


def test_builds_v2(client: TestClient, mocker: Mock) -> None:
    task_queue = mocker.patch(
        "keeper.taskrunner.inspect_task_queue", return_value=None
    )
    task_queue = MockTaskQueue(mocker)

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

    # Create default organization
    from keeper.models import Organization, db

    org = Organization(
        slug="test",
        title="Test",
        root_domain="lsst.io",
        fastly_domain="global.ssl.fastly.net",
        bucket_name="bucket-name",
    )
    db.session.add(org)
    db.session.commit()

    # ========================================================================
    # Add product /products/pipelines
    mocker.resetall()

    p = {
        "slug": "pipelines",
        "doc_repo": "https://github.com/lsst/pipelines_docs.git",
        "title": "LSST Science Pipelines",
        "root_domain": "lsst.io",
        "root_fastly_domain": "global.ssl.fastly.net",
        "bucket_name": "bucket-name",
    }
    r = client.post("/products/", p)
    task_queue.apply_task_side_effects()
    product_url = r.headers["Location"]

    assert r.status == 201

    # ========================================================================
    # Add a sample edition
    mocker.resetall()

    e = {
        "tracked_refs": ["main"],
        "slug": "latest",
        "title": "Latest",
        "published_url": "pipelines.lsst.io",
    }
    r = client.post(product_url + "/editions/", e)
    task_queue.apply_task_side_effects()

    # Initially no builds
    r = client.get("/products/pipelines/builds/")
    assert r.status == 200
    assert len(r.json["builds"]) == 0

    # ========================================================================
    # Add a build (using v2 api)
    mocker.resetall()

    b1 = {
        "slug": "b1",
        "github_requester": "jonathansick",
        "git_refs": ["main"],
    }
    r = client.post(
        "/products/pipelines/builds/", b1, headers={"Accept": v2_json_type}
    )
    task_queue.apply_task_side_effects()
    s3_resource_mock.assert_called_once()
    presign_post_mock.assert_called_once()
    assert r.status == 201
    assert r.json["product_url"] == product_url
    assert r.json["slug"] == b1["slug"]
    assert r.json["date_created"] is not None
    assert r.json["date_ended"] is None
    assert r.json["uploaded"] is False
    assert r.json["published_url"] == "https://pipelines.lsst.io/builds/b1"
    assert "post_prefix_urls" in r.json
    assert "post_dir_urls" in r.json
    assert len(r.json["surrogate_key"]) == 32  # should be a uuid4 -> hex
    build_url = r.headers["Location"]

    # ========================================================================
    # Re-add build with same slug; should fail
    mocker.resetall()

    with pytest.raises(ValidationError):
        r = client.post(
            "/products/pipelines/builds/", b1, headers={"Accept": v2_json_type}
        )

    # ========================================================================
    # List builds
    mocker.resetall()

    r = client.get("/products/pipelines/builds/")
    assert r.status == 200
    assert len(r.json["builds"]) == 1

    # ========================================================================
    # Get build
    mocker.resetall()

    r = client.get(build_url)
    assert r.status == 200
    assert r.json["bucket_name"] == "bucket-name"
    assert r.json["bucket_root_dir"] == "pipelines/builds/b1"

    # ========================================================================
    # Register upload
    mocker.resetall()

    r = client.patch(build_url, {"uploaded": True})
    task_queue.apply_task_side_effects()
    assert r.status == 200

    task_queue.assert_launched_once()
    task_queue.assert_edition_build_v1(
        "http://example.test/editions/1",
        build_url,
    )
    task_queue.assert_edition_build_v1(
        "http://example.test/editions/2",
        build_url,
    )
    task_queue.assert_dashboard_build_v1(product_url)

    r = client.get(build_url)
    assert r.json["uploaded"] is True

    # ========================================================================
    # Check that the edition was rebuilt
    mocker.resetall()

    edition_data = client.get("http://example.test/editions/2")
    assert edition_data.json["build_url"] == build_url

    # ========================================================================
    # Deprecate build
    mocker.resetall()

    r = client.delete("/builds/1")
    task_queue.apply_task_side_effects()
    assert r.status == 200

    mocker.resetall()

    r = client.get("/builds/1")
    assert r.json["product_url"] == product_url
    assert r.json["slug"] == b1["slug"]
    assert r.json["date_created"] is not None
    assert r.json["date_ended"] is not None

    # Build no longer in listing
    r = client.get("/products/pipelines/builds/")
    assert r.status == 200
    assert len(r.json["builds"]) == 0

    # ========================================================================
    # Add an auto-slugged build
    mocker.resetall()

    b2 = {"git_refs": ["main"]}
    r = client.post("/products/pipelines/builds/", b2)
    task_queue.apply_task_side_effects()

    assert r.status == 201
    assert r.json["slug"] == "1"

    # ========================================================================
    # Add an auto-slugged build
    mocker.resetall()

    b3 = {"git_refs": ["main"]}
    r = client.post(
        "/products/pipelines/builds/", b3, headers={"Accept": v2_json_type}
    )
    task_queue.apply_task_side_effects()

    assert r.status == 201
    assert r.json["slug"] == "2"

    # ========================================================================
    # Add a build missing 'git_refs'
    mocker.resetall()

    b4 = {"slug": "bad-build"}
    with pytest.raises(pydantic.ValidationError):
        r = client.post("/products/pipelines/builds/", b4)

    # ========================================================================
    # Add a build with a badly formatted git_refs
    mocker.resetall()

    b5 = {"slug": "another-bad-build", "git_refs": "main"}
    with pytest.raises(pydantic.ValidationError):
        r = client.post(
            "/products/pipelines/builds/", b5, headers={"Accept": v2_json_type}
        )

    # ========================================================================
    # Add a build and see if an edition was automatically created
    mocker.resetall()

    b6 = {"git_refs": ["tickets/DM-1234"]}
    r = client.post(
        "/products/pipelines/builds/", b6, headers={"Accept": v2_json_type}
    )
    task_queue.apply_task_side_effects()
    assert r.status == 201
    r = client.get("/products/pipelines/editions/")
    assert len(r.json["editions"]) == 3
    editions = sorted(r.json["editions"])  # postgres and sqlite differ orders
    auto_edition_url = editions[-1]
    r = client.get(auto_edition_url)
    assert r.json["slug"] == "DM-1234"


# Authorizion tests: POST /products/<slug>/builds/ (v2) ======================
# Only the build-upload auth'd client should get in


def test_post_build_auth_anon(anon_client: TestClient) -> None:
    r = anon_client.post(
        "/products/test/builds/",
        {"foo": "bar"},
        headers={"Accept": v2_json_type},
    )
    assert r.status == 401


def test_post_build_auth_product_client(product_client: TestClient) -> None:
    r = product_client.post(
        "/products/test/builds/",
        {"foo": "bar"},
        headers={"Accept": v2_json_type},
    )
    assert r.status == 403


def test_post_build_auth_edition_client(edition_client: TestClient) -> None:
    r = edition_client.post(
        "/products/test/builds/",
        {"foo": "bar"},
        headers={"Accept": v2_json_type},
    )
    assert r.status == 403


def test_post_build_auth_builduploader_client(
    upload_build_client: TestClient,
) -> None:
    with pytest.raises(NotFound):
        upload_build_client.post(
            "/products/test/builds/",
            {"foo": "bar"},
            headers={"Accept": v2_json_type},
        )


def test_post_build_auth_builddeprecator_client(
    deprecate_build_client: TestClient,
) -> None:
    r = deprecate_build_client.post(
        "/products/test/builds/",
        {"foo": "bar"},
        headers={"Accept": v2_json_type},
    )
    assert r.status == 403
