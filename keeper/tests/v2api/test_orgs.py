"""Test /v2/ APIs for managing organizations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from keeper.models import Organization, db
from keeper.testutils import MockTaskQueue

if TYPE_CHECKING:
    from unittest.mock import Mock

    from keeper.testutils import TestClient


def test_get_orgs(client: TestClient, mocker: Mock) -> None:
    task_queue = mocker.patch(
        "keeper.taskrunner.inspect_task_queue", return_value=None
    )
    task_queue = MockTaskQueue(mocker)  # noqa

    r = client.get("/v2/orgs")
    assert len(r.json) == 0

    # Create default organization
    org1 = Organization(
        slug="test1",
        title="Test1",
        root_domain="lsst.io",
        fastly_domain="global.ssl.fastly.net",
        bucket_name="bucket-name",
        bucket_public_read=False,
    )
    db.session.add(org1)
    db.session.commit()

    r = client.get("/v2/orgs")
    data = r.json
    assert len(data) == 1
    assert data[0]["slug"] == "test1"

    # Add another organization
    org2 = Organization(
        slug="test2",
        title="Test2",
        root_domain="lsst.io",
        fastly_domain="global.ssl.fastly.net",
        bucket_name="bucket-name",
    )
    db.session.add(org2)
    db.session.commit()

    r = client.get("/v2/orgs")
    data = r.json
    assert len(data) == 2

    # Try to get an organization resource from an item's self_url
    org_url = data[0]["self_url"]
    r2 = client.get(org_url)
    assert r2.json == data[0]


def test_create_fastly_org(client: TestClient, mocker: Mock) -> None:
    """Test creating an Organization with Fastly support."""
    task_queue = mocker.patch(
        "keeper.taskrunner.inspect_task_queue", return_value=None
    )
    task_queue = MockTaskQueue(mocker)  # noqa

    request_data = {
        "slug": "test",
        "title": "Test",
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
    assert r.status == 201
    org_url = r.headers["Location"]
    data = r.json

    r2 = client.get("/v2/orgs/test")
    data2 = r2.json
    assert data == data2
    assert data2["self_url"] == org_url
