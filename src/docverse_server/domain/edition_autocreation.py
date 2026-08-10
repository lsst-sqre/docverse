"""Resolution of the effective edition-autocreation config.

The config is stored as a nullable JSONB column on both
``organizations`` and ``projects``. Resolution mirrors
``slug_rewrite_rules``: the project value wins whole-object when
present, the org value is the fallback, and an all-``None`` pair yields
the built-in defaults.
"""

from __future__ import annotations

from docverse.models import EditionAutocreationConfig

__all__ = ["DEFAULT_EDITION_AUTOCREATION", "resolve_edition_autocreation"]


DEFAULT_EDITION_AUTOCREATION = EditionAutocreationConfig()
"""Config applied when neither the project nor its org sets one."""


def resolve_edition_autocreation(
    *,
    project: EditionAutocreationConfig | None,
    org: EditionAutocreationConfig | None,
) -> EditionAutocreationConfig:
    """Resolve the effective autocreation config for a project.

    Parameters
    ----------
    project
        The project's own config, or ``None`` when unset.
    org
        The parent organization's config, or ``None`` when unset.

    Returns
    -------
    EditionAutocreationConfig
        The project config when set, else the org config, else the
        defaults. Resolution is whole-object, not per-field: a project
        config with a single knob set does not inherit the org's value
        for the others.
    """
    if project is not None:
        return project
    if org is not None:
        return org
    return DEFAULT_EDITION_AUTOCREATION
