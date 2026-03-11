"""Test the manual tracking mode."""

from __future__ import annotations

from keeper.editiontracking.trackingmodes import EditionTrackingModes


def test_manual_mode() -> None:
    """Test the ManualTrackingMode."""
    modes = EditionTrackingModes()

    mode = modes["manual"]

    assert mode.name == "manual"

    # Never returns True, by definition
    assert mode.should_update(None, None) is False
