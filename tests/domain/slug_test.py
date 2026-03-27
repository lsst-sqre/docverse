"""Tests for docverse.domain.slug."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from docverse.client.models import EditionKind, TrackingMode
from docverse.domain.slug import (
    ALTERNATE_SEPARATOR,
    IgnoreRule,
    InvalidSlugError,
    PrefixStripRule,
    RegexRule,
    derive_edition_slug,
    parse_slug_rewrite_rules,
    validate_slug,
)

# ---- Rubin example rules (reused across tests) ----

RUBIN_RULES: list[IgnoreRule | PrefixStripRule | RegexRule] = (
    parse_slug_rewrite_rules(
        [
            {"type": "ignore", "glob": "dependabot/**"},
            {"type": "ignore", "glob": "renovate/**"},
            {
                "type": "prefix_strip",
                "prefix": "tickets/",
                "edition_kind": "draft",
            },
            {
                "type": "regex",
                "pattern": r"^v?(?P<slug>\d+\.\d+\.\d+)$",
                "edition_kind": "release",
            },
        ]
    )
)


# ---- parse_slug_rewrite_rules ----


class TestParseSlugRewriteRules:
    def test_none_returns_empty(self) -> None:
        assert parse_slug_rewrite_rules(None) == []

    def test_valid_rules(self) -> None:
        rules = parse_slug_rewrite_rules(
            [
                {"type": "ignore", "glob": "bot/**"},
                {
                    "type": "prefix_strip",
                    "prefix": "tickets/",
                    "edition_kind": "draft",
                },
                {
                    "type": "regex",
                    "pattern": r"^v?(?P<slug>\d+\.\d+\.\d+)$",
                    "edition_kind": "release",
                },
            ]
        )
        assert len(rules) == 3
        assert isinstance(rules[0], IgnoreRule)
        assert isinstance(rules[1], PrefixStripRule)
        assert isinstance(rules[2], RegexRule)

    def test_invalid_rule_type(self) -> None:
        with pytest.raises(ValidationError):
            parse_slug_rewrite_rules([{"type": "unknown", "glob": "*"}])

    def test_missing_required_field(self) -> None:
        with pytest.raises(ValidationError):
            parse_slug_rewrite_rules([{"type": "ignore"}])

    def test_invalid_slash_replacement(self) -> None:
        with pytest.raises(ValidationError, match="slash_replacement"):
            parse_slug_rewrite_rules(
                [
                    {
                        "type": "prefix_strip",
                        "prefix": "x/",
                        "slash_replacement": "/",
                    }
                ]
            )

    def test_regex_without_slug_group(self) -> None:
        with pytest.raises(ValidationError, match="slug"):
            parse_slug_rewrite_rules(
                [{"type": "regex", "pattern": r"^(\d+)$"}]
            )

    def test_regex_invalid_pattern(self) -> None:
        with pytest.raises(ValidationError, match="Invalid regex"):
            parse_slug_rewrite_rules(
                [{"type": "regex", "pattern": r"^(?P<slug>[unclosed$"}]
            )

    def test_defaults(self) -> None:
        """Default edition_kind is draft and slash_replacement is '-'."""
        rules = parse_slug_rewrite_rules(
            [{"type": "prefix_strip", "prefix": "x/"}]
        )
        rule = rules[0]
        assert isinstance(rule, PrefixStripRule)
        assert rule.edition_kind == EditionKind.draft
        assert rule.slash_replacement == "-"


# ---- validate_slug ----


class TestValidateSlug:
    def test_valid_lowercase(self) -> None:
        assert validate_slug("my-slug") == "my-slug"

    def test_valid_uppercase_preserved(self) -> None:
        assert validate_slug("DM-12345") == "DM-12345"

    def test_single_char(self) -> None:
        assert validate_slug("a") == "a"

    def test_dots_and_underscores(self) -> None:
        assert validate_slug("v2.3.0") == "v2.3.0"
        assert validate_slug("foo_bar") == "foo_bar"

    def test_empty_raises(self) -> None:
        with pytest.raises(InvalidSlugError, match="empty"):
            validate_slug("")

    def test_reserved_prefix_raises(self) -> None:
        with pytest.raises(InvalidSlugError, match="reserved"):
            validate_slug("__main")

    def test_max_length_ok(self) -> None:
        slug = "a" * 128
        assert validate_slug(slug) == slug

    def test_exceeds_max_length_raises(self) -> None:
        with pytest.raises(InvalidSlugError, match="exceeds"):
            validate_slug("a" * 129)

    def test_invalid_chars_raises(self) -> None:
        with pytest.raises(InvalidSlugError, match="invalid characters"):
            validate_slug("has spaces")

    def test_leading_hyphen_raises(self) -> None:
        with pytest.raises(InvalidSlugError, match="invalid characters"):
            validate_slug("-leading")

    def test_trailing_hyphen_raises(self) -> None:
        with pytest.raises(InvalidSlugError, match="invalid characters"):
            validate_slug("trailing-")


# ---- IgnoreRule ----


class TestIgnoreRule:
    def test_matching_glob_suppresses(self) -> None:
        rules = [IgnoreRule(type="ignore", glob="dependabot/**")]
        result = derive_edition_slug("dependabot/npm/lodash-4.17.21", rules)
        assert result is None

    def test_non_matching_falls_through(self) -> None:
        rules = [IgnoreRule(type="ignore", glob="dependabot/**")]
        result = derive_edition_slug("main", rules)
        assert result is not None
        assert result.slug == "main"

    def test_recursive_glob(self) -> None:
        rules = [IgnoreRule(type="ignore", glob="bot/**")]
        assert derive_edition_slug("bot/a/b/c", rules) is None

    def test_exact_glob(self) -> None:
        rules = [IgnoreRule(type="ignore", glob="skip-me")]
        assert derive_edition_slug("skip-me", rules) is None
        assert derive_edition_slug("skip-me-not", rules) is not None


# ---- PrefixStripRule ----


class TestPrefixStripRule:
    def test_strips_prefix(self) -> None:
        rules = [
            PrefixStripRule(
                type="prefix_strip",
                prefix="tickets/",
                edition_kind=EditionKind.draft,
            )
        ]
        result = derive_edition_slug("tickets/DM-12345", rules)
        assert result is not None
        assert result.slug == "DM-12345"
        assert result.edition_kind == EditionKind.draft

    def test_nested_slashes_replaced(self) -> None:
        rules = [PrefixStripRule(type="prefix_strip", prefix="tickets/")]
        result = derive_edition_slug("tickets/foo/bar", rules)
        assert result is not None
        assert result.slug == "foo-bar"

    def test_custom_slash_replacement(self) -> None:
        rules = [
            PrefixStripRule(
                type="prefix_strip",
                prefix="tickets/",
                slash_replacement="_",
            )
        ]
        result = derive_edition_slug("tickets/foo/bar", rules)
        assert result is not None
        assert result.slug == "foo_bar"

    def test_non_matching_falls_through(self) -> None:
        rules = [PrefixStripRule(type="prefix_strip", prefix="tickets/")]
        result = derive_edition_slug("main", rules)
        assert result is not None
        assert result.slug == "main"
        # Falls through to default, so kind is draft
        assert result.edition_kind == EditionKind.draft

    def test_edition_kind_propagated(self) -> None:
        rules = [
            PrefixStripRule(
                type="prefix_strip",
                prefix="release/",
                edition_kind=EditionKind.release,
            )
        ]
        result = derive_edition_slug("release/v2.0", rules)
        assert result is not None
        assert result.slug == "v2.0"
        assert result.edition_kind == EditionKind.release

    def test_empty_remainder_raises(self) -> None:
        """A prefix that consumes the entire ref produces an empty slug."""
        rules = [PrefixStripRule(type="prefix_strip", prefix="tickets/")]
        with pytest.raises(InvalidSlugError, match="empty"):
            derive_edition_slug("tickets/", rules)


# ---- RegexRule ----


class TestRegexRule:
    def test_captures_slug_group(self) -> None:
        rules = [
            RegexRule(
                type="regex",
                pattern=r"^v?(?P<slug>\d+\.\d+\.\d+)$",
                edition_kind=EditionKind.release,
            )
        ]
        result = derive_edition_slug("v2.3.0", rules)
        assert result is not None
        assert result.slug == "2.3.0"
        assert result.edition_kind == EditionKind.release

    def test_without_v_prefix(self) -> None:
        rules = [
            RegexRule(
                type="regex",
                pattern=r"^v?(?P<slug>\d+\.\d+\.\d+)$",
                edition_kind=EditionKind.release,
            )
        ]
        result = derive_edition_slug("2.3.0", rules)
        assert result is not None
        assert result.slug == "2.3.0"

    def test_non_matching_falls_through(self) -> None:
        rules = [
            RegexRule(
                type="regex",
                pattern=r"^v?(?P<slug>\d+\.\d+\.\d+)$",
                edition_kind=EditionKind.release,
            )
        ]
        result = derive_edition_slug("main", rules)
        assert result is not None
        assert result.slug == "main"
        assert result.edition_kind == EditionKind.draft

    def test_slashes_in_captured_group_replaced(self) -> None:
        rules = [
            RegexRule(
                type="regex",
                pattern=r"^rel/(?P<slug>.+)$",
                edition_kind=EditionKind.release,
            )
        ]
        result = derive_edition_slug("rel/2/3", rules)
        assert result is not None
        assert result.slug == "2-3"

    def test_custom_slash_replacement(self) -> None:
        rules = [
            RegexRule(
                type="regex",
                pattern=r"^rel/(?P<slug>.+)$",
                slash_replacement=".",
            )
        ]
        result = derive_edition_slug("rel/2/3", rules)
        assert result is not None
        assert result.slug == "2.3"


# ---- Rule ordering ----


class TestRuleOrdering:
    def test_first_match_wins(self) -> None:
        rules = [
            PrefixStripRule(
                type="prefix_strip",
                prefix="tickets/",
                edition_kind=EditionKind.draft,
            ),
            PrefixStripRule(
                type="prefix_strip",
                prefix="tickets/",
                edition_kind=EditionKind.release,
            ),
        ]
        result = derive_edition_slug("tickets/DM-1", rules)
        assert result is not None
        assert result.edition_kind == EditionKind.draft

    def test_ignore_before_prefix_strip_suppresses(self) -> None:
        rules: list[IgnoreRule | PrefixStripRule | RegexRule] = [
            IgnoreRule(type="ignore", glob="tickets/*"),
            PrefixStripRule(type="prefix_strip", prefix="tickets/"),
        ]
        result = derive_edition_slug("tickets/DM-1", rules)
        assert result is None

    def test_ignore_after_prefix_strip_does_not_suppress(self) -> None:
        rules: list[IgnoreRule | PrefixStripRule | RegexRule] = [
            PrefixStripRule(type="prefix_strip", prefix="tickets/"),
            IgnoreRule(type="ignore", glob="tickets/*"),
        ]
        result = derive_edition_slug("tickets/DM-1", rules)
        assert result is not None
        assert result.slug == "DM-1"


# ---- Default fallback ----


class TestDefaultFallback:
    def test_slashes_replaced_with_hyphens(self) -> None:
        result = derive_edition_slug("feature/dark-mode", [])
        assert result is not None
        assert result.slug == "feature-dark-mode"
        assert result.edition_kind == EditionKind.draft

    def test_simple_ref(self) -> None:
        result = derive_edition_slug("main", [])
        assert result is not None
        assert result.slug == "main"
        assert result.edition_kind == EditionKind.draft

    def test_tracking_mode_is_git_ref(self) -> None:
        result = derive_edition_slug("main", [])
        assert result is not None
        assert result.tracking_mode == TrackingMode.git_ref
        assert result.tracking_params == {"git_ref": "main"}


# ---- Alternate slugs ----


class TestAlternateSlug:
    def test_compound_slug(self) -> None:
        rules = [
            PrefixStripRule(
                type="prefix_strip",
                prefix="tickets/",
                edition_kind=EditionKind.draft,
            )
        ]
        result = derive_edition_slug(
            "tickets/DM-12345", rules, alternate_name="usdf-dev"
        )
        assert result is not None
        assert result.slug == f"usdf-dev{ALTERNATE_SEPARATOR}DM-12345"
        assert result.edition_kind == EditionKind.draft
        assert result.tracking_mode == TrackingMode.alternate_git_ref
        assert result.tracking_params == {
            "git_ref": "tickets/DM-12345",
            "alternate_name": "usdf-dev",
        }

    def test_alternate_with_default_fallback(self) -> None:
        result = derive_edition_slug("main", [], alternate_name="usdf-dev")
        assert result is not None
        assert result.slug == "usdf-dev--main"
        assert result.tracking_mode == TrackingMode.alternate_git_ref

    def test_alternate_with_ignore_suppresses(self) -> None:
        rules = [IgnoreRule(type="ignore", glob="dependabot/**")]
        result = derive_edition_slug(
            "dependabot/npm/foo",
            rules,
            alternate_name="usdf-dev",
        )
        assert result is None


# ---- Tracking params ----


class TestTrackingParams:
    def test_non_alternate_tracking(self) -> None:
        result = derive_edition_slug("tickets/DM-1", RUBIN_RULES)
        assert result is not None
        assert result.tracking_mode == TrackingMode.git_ref
        assert result.tracking_params == {"git_ref": "tickets/DM-1"}

    def test_alternate_tracking(self) -> None:
        result = derive_edition_slug(
            "tickets/DM-1", RUBIN_RULES, alternate_name="usdf-dev"
        )
        assert result is not None
        assert result.tracking_mode == TrackingMode.alternate_git_ref
        assert result.tracking_params == {
            "git_ref": "tickets/DM-1",
            "alternate_name": "usdf-dev",
        }


# ---- Rubin example (full integration) ----


@pytest.mark.parametrize(
    ("git_ref", "expected_slug", "expected_kind"),
    [
        ("dependabot/npm/lodash-4.17.21", None, None),
        ("renovate/typescript-5.x", None, None),
        ("tickets/DM-12345", "DM-12345", EditionKind.draft),
        ("tickets/DM-99999", "DM-99999", EditionKind.draft),
        ("v2.3.0", "2.3.0", EditionKind.release),
        ("2.3.0", "2.3.0", EditionKind.release),
        ("feature/dark-mode", "feature-dark-mode", EditionKind.draft),
        ("main", "main", EditionKind.draft),
    ],
    ids=[
        "dependabot-ignored",
        "renovate-ignored",
        "ticket-branch",
        "ticket-branch-2",
        "semver-with-v",
        "semver-without-v",
        "feature-branch-fallback",
        "main-fallback",
    ],
)
def test_rubin_example(
    git_ref: str,
    expected_slug: str | None,
    expected_kind: EditionKind | None,
) -> None:
    result = derive_edition_slug(git_ref, RUBIN_RULES)
    if expected_slug is None:
        assert result is None
    else:
        assert result is not None
        assert result.slug == expected_slug
        assert result.edition_kind == expected_kind


def test_rubin_alternate() -> None:
    result = derive_edition_slug(
        "tickets/DM-12345", RUBIN_RULES, alternate_name="usdf-dev"
    )
    assert result is not None
    assert result.slug == "usdf-dev--DM-12345"
    assert result.edition_kind == EditionKind.draft
    assert result.tracking_mode == TrackingMode.alternate_git_ref
    assert result.tracking_params == {
        "git_ref": "tickets/DM-12345",
        "alternate_name": "usdf-dev",
    }
