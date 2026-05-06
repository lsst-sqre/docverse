"""Tests for ``docverse.storage.ltd.models``.

Round-trip captured ``keeper.lsst.codes`` JSON fixtures through the
Pydantic models so a future schema drift on the LTD side surfaces here
first.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from docverse.storage.ltd import (
    LtdBuild,
    LtdEdition,
    LtdEditionMode,
    LtdProduct,
    LtdProductsListing,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict[str, object]:
    payload: dict[str, object] = json.loads((FIXTURES_DIR / name).read_text())
    return payload


def test_products_listing_round_trip() -> None:
    listing = LtdProductsListing.model_validate(_load("products_listing.json"))
    assert len(listing.products) == 3
    assert str(listing.products[0]).endswith("/products/pipelines")


def test_product_round_trip() -> None:
    product = LtdProduct.model_validate(_load("product_pipelines.json"))
    assert product.slug == "pipelines"
    assert product.title == "LSST Science Pipelines"
    assert str(product.doc_repo).startswith("https://github.com/lsst/")
    assert product.bucket_name == "lsst-the-docs"


def test_edition_round_trip_main() -> None:
    edition = LtdEdition.model_validate(_load("edition_main_git_refs.json"))
    assert edition.slug == "main"
    assert edition.mode == LtdEditionMode.git_refs
    assert edition.tracked_refs == ["main"]
    assert edition.ltd_id == 1
    assert edition.build_id == 42


def test_edition_round_trip_branch() -> None:
    edition = LtdEdition.model_validate(_load("edition_branch_git_refs.json"))
    assert edition.slug == "u-jsick-feature"
    assert edition.tracked_refs == ["u/jsick/feature"]
    assert edition.ltd_id == 2


def test_build_round_trip() -> None:
    build = LtdBuild.model_validate(_load("build.json"))
    assert build.bucket_name == "lsst-the-docs"
    assert build.bucket_root_dir == "pipelines/builds/42"
    assert build.uploaded is True
    assert build.git_refs == ["main"]
    assert build.ltd_id == 42


def test_extra_fields_are_ignored() -> None:
    """Schema drift: an unexpected key must not fail validation."""
    payload = _load("product_pipelines.json")
    payload["new_lts_field"] = "future-feature"
    LtdProduct.model_validate(payload)


def test_edition_mode_enum_covers_documented_ltd_modes() -> None:
    """Lock the mode set documented in PRD #275."""
    assert {m.value for m in LtdEditionMode} == {
        "git_refs",
        "lsst_doc",
        "eups_major_release",
        "eups_weekly_release",
        "eups_daily_release",
        "manual",
    }


@pytest.mark.parametrize(
    ("self_url", "build_url", "expected_ltd_id", "expected_build_id"),
    [
        (
            "https://keeper.lsst.codes/editions/1",
            "https://keeper.lsst.codes/builds/42",
            1,
            42,
        ),
        (
            "https://keeper.lsst.codes/editions/9999/",
            "https://keeper.lsst.codes/builds/123/",
            9999,
            123,
        ),
    ],
)
def test_edition_ids_parse_from_urls(
    self_url: str,
    build_url: str,
    expected_ltd_id: int,
    expected_build_id: int,
) -> None:
    """Round-trip URLs of varying shape through the public id properties."""
    payload = _load("edition_main_git_refs.json")
    payload["self_url"] = self_url
    payload["build_url"] = build_url
    edition = LtdEdition.model_validate(payload)
    assert edition.ltd_id == expected_ltd_id
    assert edition.build_id == expected_build_id


def test_build_ltd_id_parses_from_self_url() -> None:
    """``LtdBuild.ltd_id`` reads the trailing segment of ``self_url``."""
    payload = _load("build.json")
    payload["self_url"] = "https://keeper.lsst.codes/builds/777/"
    assert LtdBuild.model_validate(payload).ltd_id == 777
