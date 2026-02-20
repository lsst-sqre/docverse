"""Tests for the builds API."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pydantic
import pytest
from werkzeug.exceptions import NotFound

from keeper.exceptions import ValidationError
from keeper.testutils import MockTaskQueue

if TYPE_CHECKING:
    from unittest.mock import Mock

    from keeper.testutils import TestClient


def test_builds(client: TestClient, mocker: Mock) -> None:
    task_queue = mocker.patch(
        "keeper.taskrunner.inspect_task_queue", return_value=None
    )
    task_queue = MockTaskQueue(mocker)

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

    task_queue.assert_launched_once()
    task_queue.assert_dashboard_build_v1(product_url)

    # Check that the default edition was made
    r = client.get("/products/pipelines/editions/")
    assert len(r.json["editions"]) == 1

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
    # Add a build
    mocker.resetall()

    b1 = {
        "slug": "b1",
        "github_requester": "jonathansick",
        "git_refs": ["main"],
    }
    r = client.post("/products/pipelines/builds/", b1)
    task_queue.apply_task_side_effects()
    assert r.status == 201
    assert r.json["product_url"] == product_url
    assert r.json["slug"] == b1["slug"]
    assert r.json["date_created"] is not None
    assert r.json["date_ended"] is None
    assert r.json["uploaded"] is False
    assert r.json["published_url"] == "https://pipelines.lsst.io/builds/b1"
    assert len(r.json["surrogate_key"]) == 32  # should be a uuid4 -> hex
    build_url = r.headers["Location"]

    # ========================================================================
    # Re-add build with same slug; should fail
    mocker.resetall()

    with pytest.raises(ValidationError):
        r = client.post("/products/pipelines/builds/", b1)

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
    assert r.status == 200
    task_queue.apply_task_side_effects()

    e1_url = "http://example.test/editions/1"
    e2_url = "http://example.test/editions/2"

    task_queue.assert_launched_once()
    task_queue.assert_dashboard_build_v1(product_url)
    task_queue.assert_edition_build_v1(e1_url, build_url)
    task_queue.assert_edition_build_v1(e2_url, build_url)

    r = client.get(build_url)
    assert r.json["uploaded"] is True

    # ========================================================================
    # Check that the edition was rebuilt
    mocker.resetall()

    edition_data = client.get(e2_url)
    assert edition_data.json["build_url"] == build_url

    # ========================================================================
    # Deprecate build
    mocker.resetall()

    r = client.delete("/builds/1")
    task_queue.apply_task_side_effects()
    assert r.status == 200

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
    # Add another auto-slugged build
    mocker.resetall()

    b3 = {"git_refs": ["main"]}
    r = client.post("/products/pipelines/builds/", b3)
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
        r = client.post("/products/pipelines/builds/", b5)

    # ========================================================================
    # Add a build and see if an edition was automatically created
    mocker.resetall()

    b6 = {"git_refs": ["tickets/DM-1234"]}
    r = client.post("/products/pipelines/builds/", b6)
    task_queue.apply_task_side_effects()
    assert r.status == 201
    r = client.get("/products/pipelines/editions/")
    assert len(r.json["editions"]) == 3
    editions = sorted(r.json["editions"])  # postgres and sqlite differ orders
    auto_edition_url = editions[-1]
    r = client.get(auto_edition_url)
    assert r.json["slug"] == "DM-1234"


# Authorizion tests: POST /products/<slug>/builds/ ===========================
# Only the build-upload auth'd client should get in


def test_post_build_auth_anon(anon_client: TestClient) -> None:
    r = anon_client.post("/products/test/builds/", {"foo": "bar"})
    assert r.status == 401


def test_post_build_auth_product_client(product_client: TestClient) -> None:
    r = product_client.post("/products/test/builds/", {"foo": "bar"})
    assert r.status == 403


def test_post_build_auth_edition_client(edition_client: TestClient) -> None:
    r = edition_client.post("/products/test/builds/", {"foo": "bar"})
    assert r.status == 403


def test_post_build_auth_builduploader_client(
    upload_build_client: TestClient,
) -> None:
    with pytest.raises(NotFound):
        upload_build_client.post("/products/test/builds/", {"foo": "bar"})


def test_post_build_auth_builddeprecator_client(
    deprecate_build_client: TestClient,
) -> None:
    r = deprecate_build_client.post("/products/test/builds/", {"foo": "bar"})
    assert r.status == 403


# Authorizion tests: PATCH /products/<slug>/builds/ ==========================
# Only the build-upload auth'd client should get in


def test_patch_build_auth_anon(anon_client: TestClient) -> None:
    r = anon_client.patch("/builds/1", {"foo": "bar"})
    assert r.status == 401


def test_patch_build_auth_product_client(product_client: TestClient) -> None:
    r = product_client.patch("/builds/1", {"foo": "bar"})
    assert r.status == 403


def test_patch_build_auth_edition_client(edition_client: TestClient) -> None:
    r = edition_client.patch("/builds/1", {"foo": "bar"})
    assert r.status == 403


def test_patch_build_auth_builduploader_client(
    upload_build_client: TestClient,
) -> None:
    with pytest.raises(NotFound):
        upload_build_client.patch("/builds/1", {"foo": "bar"})


def test_patch_build_auth_builddeprecator_client(
    deprecate_build_client: TestClient,
) -> None:
    r = deprecate_build_client.patch("/builds/1", {"foo": "bar"})
    assert r.status == 403


# Authorizion tests: DELETE /products/<slug>/builds/ =========================
# Only the build-deprecator auth'd client should get in


def test_delete_build_auth_anon(anon_client: TestClient) -> None:
    r = anon_client.delete("/builds/1")
    assert r.status == 401


def test_delete_build_auth_product_client(product_client: TestClient) -> None:
    r = product_client.delete("/builds/1")
    assert r.status == 403


def test_delete_build_auth_edition_client(edition_client: TestClient) -> None:
    r = edition_client.delete("/builds/1")
    assert r.status == 403


def test_delete_build_auth_builduploader_client(
    upload_build_client: TestClient,
) -> None:
    r = upload_build_client.delete("/builds/1")
    assert r.status == 403


def test_delete_build_auth_builddeprecator_client(
    deprecate_build_client: TestClient,
) -> None:
    with pytest.raises(NotFound):
        deprecate_build_client.delete("/builds/1", {"foo": "bar"})
