"""Test DB models."""

from __future__ import annotations

from typing import TYPE_CHECKING

from keeper.models import Organization, Product, Tag, db

if TYPE_CHECKING:
    from flask import Flask


def test_tags(empty_app: Flask) -> None:
    org = Organization(
        slug="test-org",
        title="Test Org",
        fastly_support=True,
        root_domain="example.org",
        fastly_domain="fastly.example.org",
        bucket_name="example",
    )
    productA = Product(
        slug="productA",
        doc_repo="src.example.org/productA",
        title="productA",
        root_domain="example.org",
        root_fastly_domain="fastly.example.org",
        bucket_name="example",
        surrogate_key="123",
        organization=org,
    )
    tagA = Tag(
        organization=org,
        slug="a",
        title="a",
        comment="This tag is for testing.",
    )
    productA.tags.append(tagA)

    db.session.add(org)
    db.session.add(productA)
    db.session.add(tagA)
    db.session.commit()
