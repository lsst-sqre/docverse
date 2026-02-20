"""Tests for the product API."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pydantic
import pytest
import werkzeug.exceptions

from keeper.testutils import MockTaskQueue

if TYPE_CHECKING:
    from unittest.mock import Mock

    from keeper.testutils import TestClient


def test_products(client: TestClient, mocker: Mock) -> None:
    task_queue = mocker.patch(
        "keeper.taskrunner.inspect_task_queue", return_value=None
    )
    task_queue = MockTaskQueue(mocker)

    """Test various API operations against Product resources."""
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
    # Add product /products/ldm-151
    mocker.resetall()

    r = client.get("/products/")

    assert r.status == 200
    assert len(r.json["products"]) == 0

    # ========================================================================
    # Add first product /products/pipelines
    mocker.resetall()

    p1 = {
        "slug": "pipelines",
        "doc_repo": "https://github.com/lsst/pipelines_docs.git",
        "title": "LSST Science Pipelines",
        "root_domain": "lsst.io",
        "root_fastly_domain": "global.ssl.fastly.net",
        "bucket_name": "bucket-name",
    }
    r = client.post("/products/", p1)
    task_queue.apply_task_side_effects()
    p1_url = r.headers["Location"]

    assert r.status == 201

    task_queue.assert_launched_once()
    task_queue.assert_dashboard_build_v1(p1_url, once=False)  # FIXME

    # ========================================================================
    # Validate that default edition was made
    mocker.resetall()

    r = client.get("/products/pipelines/editions/")
    assert r.status == 200
    default_ed_url = r.json["editions"][0]

    r = client.get(default_ed_url)
    assert r.json["slug"] == "__main"
    assert r.json["title"] == "Latest"
    assert r.json["tracked_refs"] == ["main"]
    assert r.json["published_url"] == "https://pipelines.lsst.io"

    # ========================================================================
    # Add second product
    mocker.resetall()

    p2 = {
        "slug": "qserv",
        "doc_repo": "https://github.com/lsst/qserv_docs.git",
        "title": "Qserv",
        "root_domain": "lsst.io",
        "root_fastly_domain": "global.ssl.fastly.net",
        "bucket_name": "bucket-name",
    }
    r = client.post("/products/", p2)
    task_queue.apply_task_side_effects()
    p2_url = r.headers["Location"]

    assert r.status == 201

    task_queue.assert_launched_once()
    task_queue.assert_dashboard_build_v1(p2_url)

    # ========================================================================
    # Add product with slug that will fail validation
    mocker.resetall()

    with pytest.raises(pydantic.ValidationError):
        client.post(
            "/products/",
            {
                "slug": "0qserv",
                "doc_repo": "https://github.com/lsst/qserv_docs.git",
                "title": "Qserv",
                "root_domain": "lsst.io",
                "root_fastly_domain": "global.ssl.fastly.net",
                "bucket_name": "bucket-name",
            },
        )
    with pytest.raises(pydantic.ValidationError):
        client.post(
            "/products/",
            {
                "slug": "qserv_distrib",
                "doc_repo": "https://github.com/lsst/qserv_docs.git",
                "title": "Qserv",
                "root_domain": "lsst.io",
                "root_fastly_domain": "global.ssl.fastly.net",
                "bucket_name": "bucket-name",
            },
        )
    with pytest.raises(pydantic.ValidationError):
        client.post(
            "/products/",
            {
                "slug": "qserv.distrib",
                "doc_repo": "https://github.com/lsst/qserv_docs.git",
                "title": "Qserv",
                "root_domain": "lsst.io",
                "root_fastly_domain": "global.ssl.fastly.net",
                "bucket_name": "bucket-name",
            },
        )

    # ========================================================================
    # Test listing of products
    mocker.resetall()

    r = client.get("/products/")
    assert r.status == 200
    assert r.json["products"] == [p1_url, p2_url]

    # ========================================================================
    # Test getting first product
    mocker.resetall()

    r = client.get(p1_url)
    for k, v in p1.items():
        assert r.json[k] == v
    # Test domain
    assert r.json["domain"] == "pipelines.lsst.io"
    assert r.json["fastly_domain"] == "global.ssl.fastly.net"
    assert r.json["published_url"] == "https://pipelines.lsst.io"
    # Test surrogate key
    assert len(r.json["surrogate_key"]) == 32

    # ========================================================================
    # Test getting second product
    mocker.resetall()

    r = client.get(p2_url)
    for k, v in p2.items():
        assert r.json[k] == v
    # Test domain
    assert r.json["domain"] == "qserv.lsst.io"
    assert r.json["fastly_domain"] == "global.ssl.fastly.net"
    assert r.json["published_url"] == "https://qserv.lsst.io"
    # Test surrogate key
    assert len(r.json["surrogate_key"]) == 32

    # ========================================================================
    # Try modifying non-existent product
    mocker.resetall()

    p2v2 = dict(p2)
    p2v2 = {"title": "Qserve Data Access"}

    # Throws werkzeug.exceptions.NotFound rather than emitting 404 response
    with pytest.raises(werkzeug.exceptions.NotFound):
        r = client.patch("/products/3", p2v2)

    # ========================================================================
    # Modify existing product
    mocker.resetall()

    r = client.patch("/products/qserv", p2v2)
    task_queue.apply_task_side_effects()
    assert r.status == 200

    r = client.get("/products/qserv")
    assert r.status == 200
    for k, v in p2v2.items():
        assert r.json[k] == v

    task_queue.assert_launched_once()
    task_queue.assert_dashboard_build_v1(p2_url)


# Authorizion tests: POST /products/ =========================================
# Only the full admin client and the product-authorized client should get in


def test_post_product_auth_anon(anon_client: TestClient) -> None:
    r = anon_client.post("/products/", {"foo": "bar"})
    assert r.status == 401


def test_post_product_auth_product_client(product_client: TestClient) -> None:
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

    with pytest.raises(pydantic.ValidationError):
        product_client.post("/products/", {"foo": "bar"})


def test_post_product_auth_edition_client(edition_client: TestClient) -> None:
    r = edition_client.post("/products/", {"foo": "bar"})
    assert r.status == 403


def test_post_product_auth_builduploader_client(
    upload_build_client: TestClient,
) -> None:
    r = upload_build_client.post("/products/", {"foo": "bar"})
    assert r.status == 403


def test_post_product_auth_builddeprecator_client(
    upload_build_client: TestClient,
) -> None:
    r = upload_build_client.post("/products/", {"foo": "bar"})
    assert r.status == 403


# Authorizion tests: PATCH /products/<slug> ==================================
# Only the full admin client and the product-authorized client should get in


def test_patch_product_auth_anon(anon_client: TestClient) -> None:
    r = anon_client.patch("/products/test", {"foo": "bar"})
    assert r.status == 401


def test_patch_product_auth_product_client(product_client: TestClient) -> None:
    with pytest.raises(werkzeug.exceptions.NotFound):
        product_client.patch("/products/test", {"foo": "bar"})


def test_patch_product_auth_edition_client(edition_client: TestClient) -> None:
    r = edition_client.patch("/products/test", {"foo": "bar"})
    assert r.status == 403


def test_patch_product_auth_builduploader_client(
    upload_build_client: TestClient,
) -> None:
    r = upload_build_client.patch("/products/test", {"foo": "bar"})
    assert r.status == 403


def test_patch_product_auth_builddeprecator_client(
    upload_build_client: TestClient,
) -> None:
    r = upload_build_client.patch("/products/test", {"foo": "bar"})
    assert r.status == 403


# Authorizion tests: POST /products/<slug>/dashboard =========================
# Only the full admin client and the product-authorized client should get in


def test_post_dashboard_auth_anon(anon_client: TestClient) -> None:
    r = anon_client.post("/products/test/dashboard", {})
    assert r.status == 401


def test_post_dashboard_auth_product_client(
    product_client: TestClient,
) -> None:
    with pytest.raises(werkzeug.exceptions.NotFound):
        product_client.post("/products/test/dashboard", {})


def test_post_dashboard_auth_edition_client(
    edition_client: TestClient,
) -> None:
    r = edition_client.post("/products/test/dashboard", {})
    assert r.status == 403


def test_post_dashboard_auth_builduploader_client(
    upload_build_client: TestClient,
) -> None:
    r = upload_build_client.post("/products/test/dashboard", {})
    assert r.status == 403
