"""Edition slug derivation from git refs using rewrite rules.

This module is pure logic with no database or I/O dependencies.
"""

from __future__ import annotations

import fnmatch
import re
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import chain
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator

from docverse.models import EditionKind, TrackingMode
from docverse_server.domain.version import (
    EupsMajorVersion,
    EupsWeeklyVersion,
    LsstDocVersion,
    SemverVersion,
)
from docverse_server.exceptions import DocverseSlackException

__all__ = [
    "ALTERNATE_SEPARATOR",
    "BUILTIN_SLUG_REWRITE_RULES",
    "MAX_SLUG_LENGTH",
    "AnySlugRewriteRule",
    "EupsMajorRule",
    "EupsWeeklyRule",
    "IgnoreRule",
    "InvalidSlugError",
    "LsstDocRule",
    "PrefixStripRule",
    "RefKindDerivation",
    "RegexRule",
    "SemverRule",
    "SlugDerivationResult",
    "SlugRewriteRule",
    "VersionRule",
    "derive_edition_kind_from_ref",
    "derive_edition_slug",
    "parse_slug_rewrite_rules",
    "resolve_slug_rewrite_rules",
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


class InvalidSlugError(DocverseSlackException):
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


class VersionRule(BaseModel):
    """Base for rules that match a ref against a version grammar.

    Version rules are *kind-only*: the slug stays verbatim (version refs
    never contain slashes) and tracking stays pinned to the ref, so the
    only thing a match contributes is ``edition_kind``.
    """

    model_config = ConfigDict(frozen=True)

    edition_kind: EditionKind = EditionKind.release

    def matches(self, git_ref: str) -> bool:
        """Report whether *git_ref* satisfies this rule's grammar."""
        raise NotImplementedError


class SemverRule(VersionRule):
    """Match stable semantic-version refs (``1.2.3``, ``v1.2.3``).

    Prereleases such as ``1.0.0-rc.1`` deliberately do not match so they
    fall through to the draft fallback.
    """

    type: Literal["semver"] = "semver"

    def matches(self, git_ref: str) -> bool:
        version = SemverVersion.parse(git_ref)
        return version is not None and version.prerelease is None


class LsstDocRule(VersionRule):
    """Match LSST document version refs (``v1.0``, ``1.0``, ``1.0.1``)."""

    type: Literal["lsst_doc"] = "lsst_doc"

    def matches(self, git_ref: str) -> bool:
        return LsstDocVersion.parse(git_ref) is not None


class EupsMajorRule(VersionRule):
    """Match EUPS major release refs (``v27_0``, ``27.0``)."""

    type: Literal["eups_major"] = "eups_major"

    def matches(self, git_ref: str) -> bool:
        return EupsMajorVersion.parse(git_ref) is not None


class EupsWeeklyRule(VersionRule):
    """Match EUPS weekly release refs (``w_2026_10``)."""

    type: Literal["eups_weekly"] = "eups_weekly"

    def matches(self, git_ref: str) -> bool:
        return EupsWeeklyVersion.parse(git_ref) is not None


AnySlugRewriteRule = (
    IgnoreRule
    | PrefixStripRule
    | RegexRule
    | SemverRule
    | LsstDocRule
    | EupsMajorRule
    | EupsWeeklyRule
)
"""Any concrete slug rewrite rule, as an undiscriminated union.

Use this in function signatures; use `SlugRewriteRule` for validation.
"""

SlugRewriteRule = Annotated[
    AnySlugRewriteRule,
    Field(discriminator="type"),
]
"""A single slug rewrite rule (discriminated union on ``type``)."""

_rule_list_adapter: TypeAdapter[list[SlugRewriteRule]] = TypeAdapter(
    list[SlugRewriteRule]
)

BUILTIN_SLUG_REWRITE_RULES: tuple[AnySlugRewriteRule, ...] = (
    SemverRule(),
    LsstDocRule(),
    EupsMajorRule(),
    EupsWeeklyRule(),
)
"""Version-heuristic rules applied to every project.

`derive_edition_slug` consults these *after* the org/project-configured
rules and *before* the draft fallback, so an explicit user rule always
wins. EUPS dailies and ticket branches match nothing here and therefore
fall through to the draft fallback.
"""


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

    matched_rule_type: str | None = None
    """``type`` of the rule that matched, or ``None`` for the fallback."""


@dataclass(frozen=True, slots=True)
class RefKindDerivation:
    """The edition kind a git ref resolves to under the rule chain."""

    edition_kind: EditionKind
    """The kind the first matching rule assigns (``draft`` if none do)."""

    matched_rule_type: str | None = None
    """``type`` of the rule that matched, or ``None`` for the fallback."""


@dataclass(frozen=True, slots=True)
class _RuleMatch:
    """A rewrite rule's contribution when it matches a git ref."""

    base_slug: str
    edition_kind: EditionKind


@dataclass(frozen=True, slots=True)
class _ChainOutcome:
    """What the ordered rule chain decided for one git ref.

    The all-defaults instance is the "no rule matched" verdict: the
    caller supplies its own fallback slug and keeps ``draft``.
    """

    base_slug: str | None = None
    edition_kind: EditionKind = EditionKind.draft
    matched_rule_type: str | None = None
    suppressed: bool = False


# --- Public functions ---


def parse_slug_rewrite_rules(
    raw: list[dict[str, Any]] | None,
) -> list[AnySlugRewriteRule]:
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


def resolve_slug_rewrite_rules(
    *,
    project: list[dict[str, Any]] | None,
    org: list[dict[str, Any]] | None,
) -> list[AnySlugRewriteRule]:
    """Resolve a project's effective slug-rewrite rules.

    Implements the SQR-112 inheritance rule: a project's rule list,
    **when set**, entirely replaces the org's — there is no merging.
    Resolution keys on ``None`` (unset), not on falsiness, so an
    explicitly empty ``[]`` is a deliberate opt-out of the org's rules
    rather than a fallback trigger. A project that PATCHes
    ``slug_rewrite_rules`` to ``[]`` therefore escapes an org-level
    ignore rule or kind override and is governed only by the built-in
    rule chain.

    Parameters
    ----------
    project
        The project's own raw rule list, or ``None`` when unset.
    org
        The parent organization's raw rule list, or ``None`` when unset.

    Returns
    -------
    list
        Typed rule objects: the project's rules when the project sets
        any list at all (including an empty one), else the org's, else
        empty.

    Raises
    ------
    pydantic.ValidationError
        If any rule dict is invalid.
    """
    raw = project if project is not None else org
    return parse_slug_rewrite_rules(raw)


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


def _match_rewrite_rule(
    rule: PrefixStripRule | RegexRule | VersionRule, git_ref: str
) -> _RuleMatch | None:
    """Apply a non-ignore rule, returning ``None`` when it does not match."""
    if isinstance(rule, PrefixStripRule):
        if not git_ref.startswith(rule.prefix):
            return None
        remainder = git_ref[len(rule.prefix) :]
        return _RuleMatch(
            base_slug=remainder.replace("/", rule.slash_replacement),
            edition_kind=rule.edition_kind,
        )
    if isinstance(rule, RegexRule):
        m = re.match(rule.pattern, git_ref)
        if m is None:
            return None
        return _RuleMatch(
            base_slug=m.group("slug").replace("/", rule.slash_replacement),
            edition_kind=rule.edition_kind,
        )
    if not rule.matches(git_ref):
        return None
    # Version rules are kind-only: the ref becomes the slug verbatim.
    return _RuleMatch(base_slug=git_ref, edition_kind=rule.edition_kind)


def _run_rule_chain(
    git_ref: str, rules: Sequence[AnySlugRewriteRule]
) -> _ChainOutcome:
    """Walk *rules* then the built-ins; the first match wins.

    Shared by `derive_edition_slug` and `derive_edition_kind_from_ref`
    so both agree on precedence: org/project-configured rules first,
    then `BUILTIN_SLUG_REWRITE_RULES`, then "nothing matched".
    """
    for rule in chain(rules, BUILTIN_SLUG_REWRITE_RULES):
        if isinstance(rule, IgnoreRule):
            if fnmatch.fnmatchcase(git_ref, rule.glob):
                return _ChainOutcome(suppressed=True)
            continue
        match = _match_rewrite_rule(rule, git_ref)
        if match is not None:
            return _ChainOutcome(
                base_slug=match.base_slug,
                edition_kind=match.edition_kind,
                matched_rule_type=rule.type,
            )
    return _ChainOutcome()


def derive_edition_kind_from_ref(
    git_ref: str, rules: Sequence[AnySlugRewriteRule] = ()
) -> RefKindDerivation:
    """Classify a git ref's edition kind without deriving a slug.

    Runs the same first-match-wins chain as `derive_edition_slug`
    (caller-supplied *rules*, then `BUILTIN_SLUG_REWRITE_RULES`) but
    reports only the kind. Callers that already own the slug — notably
    keeper-sync, which preserves LTD's slug verbatim — use this instead
    of `derive_edition_slug` so ref shapes that would not survive
    `validate_slug` still get classified rather than raising.

    An ignore rule reports ``draft`` rather than suppressing: ignore
    rules gate *auto-creation*, and a caller asking only for a kind has
    already decided the edition exists.

    Parameters
    ----------
    git_ref
        The git ref (branch or tag name) to classify.
    rules
        Ordered org/project-configured rewrite rules.

    Returns
    -------
    RefKindDerivation
        The derived kind and the ``type`` of the rule that produced it.
    """
    outcome = _run_rule_chain(git_ref, rules)
    if outcome.suppressed:
        return RefKindDerivation(edition_kind=EditionKind.draft)
    return RefKindDerivation(
        edition_kind=outcome.edition_kind,
        matched_rule_type=outcome.matched_rule_type,
    )


def derive_edition_slug(
    git_ref: str,
    rules: Sequence[AnySlugRewriteRule],
    *,
    alternate_name: str | None = None,
) -> SlugDerivationResult | None:
    """Derive an edition slug from a git ref using rewrite rules.

    Rules are consulted in order and the first match wins:
    the caller-supplied *rules*, then `BUILTIN_SLUG_REWRITE_RULES`, then
    a draft fallback that only replaces slashes with hyphens.

    Parameters
    ----------
    git_ref
        The git ref (branch or tag name) from the build.
    rules
        Ordered org/project-configured rewrite rules.
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
    outcome = _run_rule_chain(git_ref, rules)
    if outcome.suppressed:
        return None
    edition_kind = outcome.edition_kind
    matched_rule_type = outcome.matched_rule_type

    # Default fallback: replace slashes with hyphens
    base_slug = outcome.base_slug
    if base_slug is None:
        base_slug = git_ref.replace("/", "-")

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
        matched_rule_type=matched_rule_type,
    )
