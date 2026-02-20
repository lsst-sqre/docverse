"""Unit tests for keeper.editiontracking.eupsmajormode."""

import pytest

from keeper.editiontracking.eupsmajormode import MajorReleaseTag


def test_parsing() -> None:
    tag = MajorReleaseTag("v1_0")
    assert tag.major == 1
    assert tag.minor == 0

    tag = MajorReleaseTag("v16_1")
    assert tag.major == 16
    assert tag.minor == 1

    # Git variant
    tag = MajorReleaseTag("1.0")
    assert tag.major == 1
    assert tag.minor == 0

    # Git variant
    tag = MajorReleaseTag("16.1")
    assert tag.major == 16
    assert tag.minor == 1

    with pytest.raises(ValueError):
        MajorReleaseTag("2_0")

    with pytest.raises(ValueError):
        MajorReleaseTag("v2")

    with pytest.raises(ValueError):
        MajorReleaseTag("2.0.0")


def test_comparisons() -> None:
    assert MajorReleaseTag("v1_0") > MajorReleaseTag("v0_1")
    assert MajorReleaseTag("v1_0") != MajorReleaseTag("v0_1")

    assert MajorReleaseTag("v1_0") == MajorReleaseTag("v1_0")
    assert MajorReleaseTag("v1_0") >= MajorReleaseTag("v1_0")
