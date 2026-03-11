"""Tests for keeper.dashboard and keeper.services.dashboard."""

from __future__ import annotations

from unittest.mock import Mock

from structlog import get_logger

from keeper.models import OrganizationLayoutMode, db
from keeper.services.createbuild import create_build
from keeper.services.createorg import create_organization
from keeper.services.createproduct import create_product
from keeper.services.dashboard import build_dashboard
from keeper.testutils import MockTaskQueue, TestClient


def test_builtin_template(client: TestClient, mocker: Mock) -> None:
    logger = get_logger("keeper")

    task_queue = mocker.patch(
        "keeper.taskrunner.inspect_task_queue", return_value=None
    )
    task_queue = MockTaskQueue(mocker)  # noqa

    org = create_organization(
        slug="test",
        title="Test",
        layout=OrganizationLayoutMode.path,
        domain="example.org",
        path_prefix="/",
        bucket_name="example",
        s3_public_read=False,
        fastly_support=False,
        aws_id=None,
        aws_region=None,
        aws_secret=None,
        fastly_domain=None,
        fastly_service_id=None,
        fastly_api_key=None,
    )
    db.session.add(org)
    db.session.commit()

    # This print is somehow required; not exactly sure why.
    print(f"test {org.root_domain}")

    product, default_edition = create_product(
        org=org,
        slug="myproject",
        doc_repo="https://git.example.org/myproject",
        title="My Project",
    )
    print(product)
    build, _ = create_build(
        product=product,
        git_ref="main",
    )
    default_edition.build = build
    db.session.add(build)
    db.session.add(default_edition)
    db.session.commit()

    build_dashboard(product, logger)
