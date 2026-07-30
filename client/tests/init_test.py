"""Smoke tests for the docverse package."""

from __future__ import annotations

from docverse import __version__


def test_version() -> None:
    assert isinstance(__version__, str)
    assert __version__ != ""
