"""Tests for edition-autocreation config resolution."""

from __future__ import annotations

from docverse.models import EditionAutocreationConfig
from docverse_server.domain.edition_autocreation import (
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
