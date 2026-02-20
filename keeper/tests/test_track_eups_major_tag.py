"""Test an Edition that tracks eups major releases (`eups_major_release`)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from keeper.testutils import MockTaskQueue

if TYPE_CHECKING:
    from unittest.mock import Mock

    from keeper.testutils import TestClient


def test_eups_major_release_edition(client: TestClient, mocker: Mock) -> None:
    """Test an edition that tracks the most recent EUPS major release."""
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

    p1_data = {
        "slug": "pipelines",
        "doc_repo": "https://github.com/lsst/pipelines",
        "main_mode": "eups_major_release",
        "title": "LSST Science Pipelines",
        "root_domain": "lsst.io",
        "root_fastly_domain": "global.ssl.fastly.net",
        "bucket_name": "bucket-name",
    }
    r = client.post("/products/", p1_data)
    task_queue.apply_task_side_effects()
    p1_url = r.headers["Location"]

    # Get the URL for the default edition
    r = client.get(p1_url + "/editions/")
    edition_url = r.json["editions"][0]

    # ========================================================================
    # Test tracking mode of default edition
    r = client.get(edition_url)
    assert r.json["mode"] == "eups_major_release"

    # ========================================================================
    # Create a build for 'v1_0'
    mocker.resetall()

    b1_data = {
        "slug": "b1",
        "github_requester": "jonathansick",
        "git_refs": ["v1_0"],
    }
    r = client.post("/products/pipelines/builds/", b1_data)
    task_queue.apply_task_side_effects()
    b1_url = r.headers["Location"]

    mocker.resetall()
    r = client.patch(b1_url, {"uploaded": True})
    task_queue.apply_task_side_effects()

    # Test that the main edition updated
    r = client.get(edition_url)
    assert r.json["build_url"] == b1_url
    assert r.json["pending_rebuild"] is False

    # ========================================================================
    # Create a build for the 'min' branch that is not tracked
    mocker.resetall()

    b2_data = {
        "slug": "b2",
        "github_requester": "jonathansick",
        "git_refs": ["main"],
    }
    r = client.post("/products/pipelines/builds/", b2_data)
    task_queue.apply_task_side_effects()
    b2_url = r.headers["Location"]

    mocker.resetall()
    r = client.patch(b2_url, {"uploaded": True})
    task_queue.apply_task_side_effects()

    # Test that the default edition *did not* update because this build is
    # neither for main nor a semantic version.
    # with semantic versions
    r = client.get(edition_url)
    assert r.json["build_url"] == b1_url

    # ========================================================================
    # Create a build with a newer semantic release tag that is tracked
    mocker.resetall()

    b3_data = {
        "slug": "b3",
        "github_requester": "jonathansick",
        "git_refs": ["v2_0"],
    }
    r = client.post("/products/pipelines/builds/", b3_data)
    task_queue.apply_task_side_effects()
    b3_url = r.headers["Location"]

    mocker.resetall()
    r = client.patch(b3_url, {"uploaded": True})
    task_queue.apply_task_side_effects()

    # Test that the main edition updated
    r = client.get(edition_url)
    assert r.json["build_url"] == b3_url
    assert r.json["pending_rebuild"] is False

    # ========================================================================
    # Create a build with a older semantic release tag that is not tracked
    mocker.resetall()

    b4_data = {
        "slug": "b4",
        "github_requester": "jonathansick",
        "git_refs": ["v1_5"],
    }
    r = client.post("/products/pipelines/builds/", b4_data)
    task_queue.apply_task_side_effects()
    b4_url = r.headers["Location"]

    mocker.resetall()
    r = client.patch(b4_url, {"uploaded": True})
    task_queue.apply_task_side_effects()

    # Test that the default edition *did not* update because this build is
    # neither for main nor a semantic version.
    # with semantic versions
    r = client.get(edition_url)
    assert r.json["build_url"] == b3_url
