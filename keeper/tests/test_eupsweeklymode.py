"""Tests for `keeper.editiontracking.eupsweeklymode`."""

import pytest

from keeper.editiontracking.eupsweeklymode import WeeklyReleaseTag


def test_parsing() -> None:
    tag = WeeklyReleaseTag("w_2018_01")
    assert tag.year == 2018
    assert tag.week == 1

    tag = WeeklyReleaseTag("w_2018_26")
    assert tag.year == 2018
    assert tag.week == 26

    # Git variant
    tag = WeeklyReleaseTag("w.2018.01")
    assert tag.year == 2018
    assert tag.week == 1

    # Git variant
    tag = WeeklyReleaseTag("w.2018.26")
    assert tag.year == 2018
    assert tag.week == 26

    with pytest.raises(ValueError):
        WeeklyReleaseTag("v1_0")
    with pytest.raises(ValueError):
        WeeklyReleaseTag("w_2018")
    with pytest.raises(ValueError):
        WeeklyReleaseTag("w_2018_01rc1")


def test_comparisons() -> None:
    assert WeeklyReleaseTag("w_2018_01") > WeeklyReleaseTag("w_2017_01")
    assert not WeeklyReleaseTag("w_2018_01") < WeeklyReleaseTag("w_2017_01")

    assert WeeklyReleaseTag("w_2018_20") >= WeeklyReleaseTag("w_2018_20")
    assert WeeklyReleaseTag("w_2018_20") >= WeeklyReleaseTag("w_2018_01")
    assert WeeklyReleaseTag("w_2018_20") <= WeeklyReleaseTag("w_2018_20")
    assert not WeeklyReleaseTag("w_2018_20") == WeeklyReleaseTag("w_2018_01")
