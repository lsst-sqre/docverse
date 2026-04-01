"""Edition slug derivation from git refs using rewrite rules.

This module is pure logic with no database or I/O dependencies.
"""

from __future__ import annotations

import fnmatch
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, TypeAdapter, field_validator

from docverse.client.models import EditionKind, TrackingMode

__all__ = [
    "ALTERNATE_SEPARATOR",
    "MAX_SLUG_LENGTH",
    "IgnoreRule",
    "InvalidSlugError",
    "PrefixStripRule",
    "RegexRule",
    "SlugDerivationResult",
    "SlugRewriteRule",
    "derive_edition_slug",
    "parse_slug_rewrite_rules",
    "validate_slug",
]

MAX_SLUG_LENGTH = 128
"""Maximum allowed length for an edition slug."""

ALTERNATE_SEPARATOR = "--"
"""Separator between alternate name and base slug in compound slugs."""

_SLUG_CHAR_PATTERN = re.compile(r"^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$")
"""Allowed characters in a slug.

Single-character slugs are allowed. Multi-character slugs must start and end
with alphanumeric characters.
"""

_ALLOWED_SLASH_REPLACEMENTS = frozenset({"-", "_", "."})


# --- Exceptions ---


class InvalidSlugError(Exception):
    """The derived slug fails validation."""

    def __init__(self, slug: str, reason: str) -> None:
        self.slug = slug
        self.reason = reason
        super().__init__(f"Invalid slug {slug!r}: {reason}")


# --- Rule models (Pydantic discriminated union) ---


class IgnoreRule(BaseModel):
    """Suppress edition auto-creation for refs matching a glob pattern."""

    type: Literal["ignore"]
    glob: str


class PrefixStripRule(BaseModel):
    """Strip a literal prefix from the git ref to produce the slug."""

    type: Literal["prefix_strip"]
    prefix: str
    edition_kind: EditionKind = EditionKind.draft
    slash_replacement: str = "-"

    @field_validator("slash_replacement")
    @classmethod
    def _check_slash_replacement(cls, v: str) -> str:
        if v not in _ALLOWED_SLASH_REPLACEMENTS:
            msg = (
                f"slash_replacement must be one of"
                f" {sorted(_ALLOWED_SLASH_REPLACEMENTS)}"
            )
            raise ValueError(msg)
        return v


class RegexRule(BaseModel):
    """Use a regex with a named ``slug`` capture group."""

    type: Literal["regex"]
    pattern: str
    edition_kind: EditionKind = EditionKind.draft
    slash_replacement: str = "-"

    @field_validator("slash_replacement")
    @classmethod
    def _check_slash_replacement(cls, v: str) -> str:
        if v not in _ALLOWED_SLASH_REPLACEMENTS:
            msg = (
                f"slash_replacement must be one of"
                f" {sorted(_ALLOWED_SLASH_REPLACEMENTS)}"
            )
            raise ValueError(msg)
        return v

    @field_validator("pattern")
    @classmethod
    def _check_pattern(cls, v: str) -> str:
        try:
            compiled = re.compile(v)
        except re.error as exc:
            msg = f"Invalid regex pattern: {exc}"
            raise ValueError(msg) from exc
        if "slug" not in compiled.groupindex:
            msg = "Regex pattern must contain a named group 'slug'"
            raise ValueError(msg)
        return v


SlugRewriteRule = Annotated[
    IgnoreRule | PrefixStripRule | RegexRule,
    Field(discriminator="type"),
]
"""A single slug rewrite rule (discriminated union on ``type``)."""

_rule_list_adapter: TypeAdapter[list[SlugRewriteRule]] = TypeAdapter(
    list[SlugRewriteRule]
)


# --- Result dataclass ---


@dataclass(slots=True)
class SlugDerivationResult:
    """The outcome of deriving an edition slug from a git ref."""

    slug: str
    """The derived edition slug."""

    edition_kind: EditionKind
    """The kind of edition to create."""

    tracking_mode: TrackingMode
    """How the edition tracks builds."""

    tracking_params: dict[str, str]
    """Parameters for the tracking mode."""


# --- Public functions ---


def parse_slug_rewrite_rules(
    raw: list[dict[str, Any]] | None,
) -> list[IgnoreRule | PrefixStripRule | RegexRule]:
    """Parse JSONB slug rewrite rules into typed rule objects.

    Parameters
    ----------
    raw
        The raw rule list from ``Organization.slug_rewrite_rules`` or
        ``Project.slug_rewrite_rules``. ``None`` is treated as an empty list.

    Returns
    -------
    list
        Typed rule objects.

    Raises
    ------
    pydantic.ValidationError
        If any rule dict is invalid.
    """
    if raw is None:
        return []
    return _rule_list_adapter.validate_python(raw)


def validate_slug(slug: str) -> str:
    """Validate an edition slug.

    Parameters
    ----------
    slug
        The slug to validate.

    Returns
    -------
    str
        The validated slug (case preserved).

    Raises
    ------
    InvalidSlugError
        If the slug is invalid.
    """
    if not slug:
        raise InvalidSlugError(slug, "slug is empty")
    if slug.startswith("__"):
        raise InvalidSlugError(
            slug, "slug must not start with '__' (reserved)"
        )
    if len(slug) > MAX_SLUG_LENGTH:
        raise InvalidSlugError(
            slug, f"slug exceeds {MAX_SLUG_LENGTH} characters"
        )
    if not _SLUG_CHAR_PATTERN.match(slug):
        raise InvalidSlugError(slug, "slug contains invalid characters")
    return slug


def derive_edition_slug(
    git_ref: str,
    rules: Sequence[IgnoreRule | PrefixStripRule | RegexRule],
    *,
    alternate_name: str | None = None,
) -> SlugDerivationResult | None:
    """Derive an edition slug from a git ref using rewrite rules.

    Parameters
    ----------
    git_ref
        The git ref (branch or tag name) from the build.
    rules
        Ordered rewrite rules; first match wins.
    alternate_name
        If set, produces a compound slug for an alternate edition.

    Returns
    -------
    SlugDerivationResult or None
        The derivation result, or ``None`` if the ref is suppressed by an
        ignore rule.

    Raises
    ------
    InvalidSlugError
        If the derived slug fails validation.
    """
    base_slug: str | None = None
    edition_kind = EditionKind.draft

    for rule in rules:
        if isinstance(rule, IgnoreRule):
            if fnmatch.fnmatchcase(git_ref, rule.glob):
                return None
        elif isinstance(rule, PrefixStripRule):
            if git_ref.startswith(rule.prefix):
                remainder = git_ref[len(rule.prefix) :]
                base_slug = remainder.replace("/", rule.slash_replacement)
                edition_kind = rule.edition_kind
                break
        elif isinstance(rule, RegexRule):
            m = re.match(rule.pattern, git_ref)
            if m:
                base_slug = m.group("slug")
                base_slug = base_slug.replace("/", rule.slash_replacement)
                edition_kind = rule.edition_kind
                break

    # Default fallback: replace slashes with hyphens
    if base_slug is None:
        base_slug = git_ref.replace("/", "-")
        edition_kind = EditionKind.draft

    # Compound slug for alternates
    if alternate_name is not None:
        slug = f"{alternate_name}{ALTERNATE_SEPARATOR}{base_slug}"
        tracking_mode = TrackingMode.alternate_git_ref
        tracking_params = {
            "git_ref": git_ref,
            "alternate_name": alternate_name,
        }
    else:
        slug = base_slug
        tracking_mode = TrackingMode.git_ref
        tracking_params = {"git_ref": git_ref}

    slug = validate_slug(slug)

    return SlugDerivationResult(
        slug=slug,
        edition_kind=edition_kind,
        tracking_mode=tracking_mode,
        tracking_params=tracking_params,
    )
