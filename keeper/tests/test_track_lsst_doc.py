"""Test an Edition that tracks LSST document releases (``lsst_doc``)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from keeper.testutils import MockTaskQueue

if TYPE_CHECKING:
    from unittest.mock import Mock

    from keeper.testutils import TestClient


def test_lsst_doc_edition(client: TestClient, mocker: Mock) -> None:
    """Test an edition that tracks LSST Doc semantic versions.

    1. Create a build on `main`; it should be tracked because the LSST_DOC
       mode tracks main if a semantic version tag hasn't been pushed yet.
    2. Create a ticket branch; it isn't tracked.
    3. Create a v1.0 build; it is tracked.
    4. Create another build on `main`; it isn't tracked because we already
       have the v1.0 build.
    5. Create a v0.9 build that is not tracked because it's older.
    6. Create a v1.1 build that **is** tracked because it's newer.
    """
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
    # Add product /products/ldm-151
    mocker.resetall()

    p1_data = {
        "slug": "ldm-151",
        "doc_repo": "https://github.com/lsst/LDM-151",
        "main_mode": "lsst_doc",
        "title": "Applications Design",
        "root_domain": "lsst.io",
        "root_fastly_domain": "global.ssl.fastly.net",
        "bucket_name": "bucket-name",
    }
    r = client.post("/products/", p1_data)
    task_queue.apply_task_side_effects()
    p1_url = r.headers["Location"]

    assert r.status == 201

    # ========================================================================
    # Get the URL for the default edition
    r = client.get(p1_url + "/editions/")
    default_edition_url = sorted(r.json["editions"])[0]
    assert default_edition_url == "http://example.test/editions/1"

    # ========================================================================
    # Create a build on 'main'
    mocker.resetall()

    b1_data = {
        "slug": "b1",
        "github_requester": "jonathansick",
        "git_refs": ["main"],
    }
    r = client.post("/products/ldm-151/builds/", b1_data)
    task_queue.apply_task_side_effects()
    b1_url = r.headers["Location"]

    # ========================================================================
    # Confirm build on 'main'
    mocker.resetall()

    r = client.patch(b1_url, {"uploaded": True})
    task_queue.apply_task_side_effects()
    task_queue.assert_edition_build_v1(default_edition_url, b1_url)

    # The 'main' edition was also automatically created to track main.
    r = client.get(p1_url + "/editions/")
    main_edition_url = sorted(r.json["editions"])[1]
    task_queue.assert_edition_build_v1(main_edition_url, b1_url)

    # Check that it's tracking the main branch
    r = client.get(main_edition_url)
    assert r.json["mode"] == "git_ref"
    assert r.json["slug"] == "main"
    assert r.json["title"] == "main"
    assert r.json["tracked_refs"] == ["main"]

    # Test that the default edition updated because there are no builds yet
    # with semantic versions
    r = client.get(default_edition_url)
    assert r.json["build_url"] == b1_url
    assert r.json["pending_rebuild"] is False

    # ========================================================================
    # Create a ticket branch build
    mocker.resetall()

    b2_data = {
        "slug": "b2",
        "github_requester": "jonathansick",
        "git_refs": ["tickets/DM-1"],
    }
    r = client.post("/products/ldm-151/builds/", b2_data)
    task_queue.apply_task_side_effects()
    b2_url = r.headers["Location"]

    # ========================================================================
    # Confirm ticket branch build
    mocker.resetall()

    r = client.patch(b2_url, {"uploaded": True})
    task_queue.apply_task_side_effects()
    task_queue.assert_edition_build_v1(
        "http://example.test/editions/3", b2_url
    )

    # Test that the default edition *did not* update because this build is
    # neither for main nor a semantic version.
    # with semantic versions
    r = client.get(default_edition_url)
    assert r.json["build_url"] == b1_url

    # ========================================================================
    # Create a build with a semantic version tag.
    mocker.resetall()

    b3_data = {
        "slug": "b3",
        "github_requester": "jonathansick",
        "git_refs": ["v1.0"],
    }
    r = client.post("/products/ldm-151/builds/", b3_data)
    task_queue.apply_task_side_effects()
    b3_url = r.headers["Location"]

    # ========================================================================
    # Confirm v1.0 build
    mocker.resetall()

    r = client.patch(b3_url, {"uploaded": True})
    task_queue.apply_task_side_effects()
    task_queue.assert_edition_build_v1(
        "http://example.test/editions/1", b3_url
    )
    task_queue.assert_edition_build_v1(
        "http://example.test/editions/4", b3_url
    )

    # Test that the default edition updated
    r = client.get(default_edition_url)
    assert r.json["build_url"] == b3_url

    # Test that the v1-0 edition updated
    r = client.get("http://example.test/editions/4")
    assert r.json["build_url"] == b3_url

    # ========================================================================
    # Create another build on 'main'
    mocker.resetall()

    b4_data = {
        "slug": "b4",
        "github_requester": "jonathansick",
        "git_refs": ["main"],
    }
    r = client.post("/products/ldm-151/builds/", b4_data)
    task_queue.apply_task_side_effects()
    b4_url = r.headers["Location"]

    # ========================================================================
    # Confirm main build
    mocker.resetall()

    r = client.patch(b4_url, {"uploaded": True})
    task_queue.apply_task_side_effects()

    # Test that the default edition *did not* update because now it's sticking
    # to only show semantic versions.
    r = client.get(default_edition_url)
    assert r.json["build_url"] == b3_url

    # Test that the **main** edition did update, though
    r = client.get(main_edition_url)
    assert r.json["build_url"] == b4_url

    # ========================================================================
    # Create a build with a **older** semantic version tag.
    mocker.resetall()

    b5_data = {
        "slug": "b5",
        "github_requester": "jonathansick",
        "git_refs": ["v0.9"],
    }
    r = client.post("/products/ldm-151/builds/", b5_data)
    task_queue.apply_task_side_effects()
    b5_url = r.headers["Location"]

    # ========================================================================
    # Confirm v0.9 build
    mocker.resetall()

    r = client.patch(b5_url, {"uploaded": True})
    task_queue.apply_task_side_effects()

    # Test that the default edition *did not* update b/c it's older
    r = client.get(default_edition_url)
    assert r.json["build_url"] == b3_url

    # ========================================================================
    # Create a build with a **newer** semantic version tag.
    mocker.resetall()

    b6_data = {
        "slug": "b6",
        "github_requester": "jonathansick",
        "git_refs": ["v1.1"],
    }
    r = client.post("/products/ldm-151/builds/", b6_data)
    task_queue.apply_task_side_effects()
    b6_url = r.headers["Location"]

    mocker.resetall()
    r = client.patch(b6_url, {"uploaded": True})
    task_queue.apply_task_side_effects()

    # Test that the default edition updated
    r = client.get(default_edition_url)
    assert r.json["build_url"] == b6_url

    task_queue.assert_edition_build_v1(
        "http://example.test/editions/1", b6_url
    )
    task_queue.assert_edition_build_v1(
        "http://example.test/editions/6", b6_url
    )
