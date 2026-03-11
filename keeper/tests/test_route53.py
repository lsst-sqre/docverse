"""Integration tests for ltd-keeper's Route 53 interactions.

`ltdtest.local.` is a test Route 53 hosted zone created in the lsst-sqre AWS
account.
"""

from __future__ import annotations

import logging
import os

import boto3
import pytest

from keeper.route53 import (
    _find_cname_record,
    _get_zone_id,
    create_cname,
    delete_cname,
)

logging.basicConfig(level=logging.INFO)
logging.getLogger("keeper.route53").level = logging.DEBUG


@pytest.mark.skipif(
    os.getenv("LTD_KEEPER_TEST_AWS_ID") is None
    or os.getenv("LTD_KEEPER_TEST_AWS_SECRET") is None,
    reason="Set LTD_KEEPER_TEST_AWS_ID and " "LTD_KEEPER_TEST_AWS_SECRET",
)
def test_route53() -> None:
    AWS_ID = os.getenv("LTD_KEEPER_TEST_AWS_ID", "")
    AWS_SECRET = os.getenv("LTD_KEEPER_TEST_AWS_SECRET", "")

    session = boto3.session.Session(
        aws_access_key_id=AWS_ID, aws_secret_access_key=AWS_SECRET
    )
    route53 = session.client("route53")

    zone_id = _get_zone_id(route53, "ltdtest.local.")
    print("Creating CNAME")
    create_cname(
        "docs.ltdtest.local.", "daringfireball.net", AWS_ID, AWS_SECRET
    )
    record = _find_cname_record(route53, zone_id, "docs.ltdtest.local.")
    assert record is not None
    print(record)

    print("Changing CNAME")
    create_cname("docs.ltdtest.local.", "www.lsst.org", AWS_ID, AWS_SECRET)
    record = _find_cname_record(route53, zone_id, "docs.ltdtest.local.")
    assert record is not None
    print(record)

    print("Deleting CNAME")
    delete_cname("docs.ltdtest.local.", AWS_ID, AWS_SECRET)
    record = _find_cname_record(route53, zone_id, "docs.ltdtest.local.")
    assert record is None
    print(record)
