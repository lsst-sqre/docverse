"""Tests for `keeper.editiontracking.eupsdailymode`."""

import pytest

from keeper.editiontracking.eupsdailymode import DailyReleaseTag


def test_parsing() -> None:
    tag = DailyReleaseTag("d_2018_02_01")
    assert tag.year == 2018
    assert tag.month == 2
    assert tag.day == 1

    # Git variant
    tag = DailyReleaseTag("d.2018.02.01")
    assert tag.year == 2018
    assert tag.month == 2
    assert tag.day == 1

    with pytest.raises(ValueError):
        DailyReleaseTag("2018_02_01")

    with pytest.raises(ValueError):
        DailyReleaseTag("d_2018_02")


def test_comparisons() -> None:
    assert DailyReleaseTag("d_2018_02_01") == DailyReleaseTag("d_2018_02_01")
    assert DailyReleaseTag("d_2018_02_02") >= DailyReleaseTag("d_2018_02_01")
    assert DailyReleaseTag("d_2018_02_02") > DailyReleaseTag("d_2018_02_01")
