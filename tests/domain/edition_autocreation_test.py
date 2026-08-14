"""Tests for edition-autocreation config resolution."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from docverse.models import EditionAutocreationConfig
from docverse_server.domain.edition_autocreation import (
    DEFAULT_EDITION_AUTOCREATION,
    resolve_edition_autocreation,
)

__all__ = []


def test_project_config_wins_over_org() -> None:
    """A project-level config overrides the org-level one whole-object."""
    resolved = resolve_edition_autocreation(
        project=EditionAutocreationConfig(semver_aggregates=True),
        org=EditionAutocreationConfig(semver_aggregates=False),
    )
    assert resolved.semver_aggregates is True


def test_org_config_applies_when_project_is_null() -> None:
    """A project with no config inherits the org's."""
    resolved = resolve_edition_autocreation(
        project=None,
        org=EditionAutocreationConfig(semver_aggregates=False),
    )
    assert resolved.semver_aggregates is False


def test_all_null_resolves_to_defaults() -> None:
    """No config anywhere leaves the built-in defaults in force."""
    resolved = resolve_edition_autocreation(project=None, org=None)
    assert resolved.semver_aggregates is True


def test_defaults_are_shared_and_immutable() -> None:
    """The default is handed out by reference, so it must be frozen.

    Every caller with no org- or project-level config gets the same
    process-wide object; a mutable one would let a single consumer
    rewrite the defaults for the rest of the process.
    """
    resolved = resolve_edition_autocreation(project=None, org=None)
    assert resolved is DEFAULT_EDITION_AUTOCREATION
    with pytest.raises(ValidationError):
        # mypy already rejects this; the assertion covers untyped callers.
        resolved.semver_aggregates = False  # type: ignore[misc]
    assert DEFAULT_EDITION_AUTOCREATION.semver_aggregates is True
