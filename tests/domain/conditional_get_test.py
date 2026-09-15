"""Tests for the conditional-GET domain module."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from docverse_server.domain.conditional_get import (
    PreconditionKind,
    datetime_to_microseconds,
    evaluate_preconditions,
    make_weak_etag,
)


def test_weak_etag_is_deterministic_for_the_same_material() -> None:
    """The same canonical tuple always hashes to the same weak tag."""
    material = ("projects_list", 42, 1_759_000_000_000_000, "limit=25")

    first = make_weak_etag(material)
    second = make_weak_etag(material)

    assert first == second
    assert first.startswith('W/"')
    assert first.endswith('"')


def test_weak_etag_separates_adjacent_parts() -> None:
    """Regrouping the same characters across parts changes the tag.

    Plain concatenation would hash ``("ab", "c")`` and ``("a", "bc")``
    identically, which on the listing would mean two different pages
    could share an ETag.
    """
    assert make_weak_etag(("ab", "c")) != make_weak_etag(("a", "bc"))


ETAG = 'W/"0123456789abcdef0123456789abcdef"'
"""Stand-in current entity-tag for the precondition tests."""


def test_matching_if_none_match_is_not_modified() -> None:
    """Echoing back the current tag earns a 304 decided by the ETag."""
    result = evaluate_preconditions(if_none_match=ETAG, etag=ETAG)

    assert result.not_modified is True
    assert result.precondition is PreconditionKind.etag


def test_star_if_none_match_is_not_modified() -> None:
    """``*`` matches any current representation."""
    result = evaluate_preconditions(if_none_match="*", etag=ETAG)

    assert result.not_modified is True
    assert result.precondition is PreconditionKind.etag


def test_if_none_match_compares_weakly() -> None:
    """A strong tag echoed back matches the weak tag that was sent."""
    result = evaluate_preconditions(
        if_none_match='"nomatch", "0123456789abcdef0123456789abcdef"',
        etag=ETAG,
    )

    assert result.not_modified is True


def test_stale_if_none_match_is_modified() -> None:
    """A tag the server has retired earns the full representation."""
    result = evaluate_preconditions(if_none_match='W/"stale"', etag=ETAG)

    assert result.not_modified is False
    assert result.precondition is PreconditionKind.etag


def test_request_without_preconditions_decides_nothing() -> None:
    """An unconditional GET reports no deciding header."""
    result = evaluate_preconditions(if_none_match=None, etag=ETAG)

    assert result.not_modified is False
    assert result.precondition is None


def test_etag_is_the_only_precondition_kind() -> None:
    """There is no date validator to decide a conditional GET.

    ``Last-Modified`` can only name the maximum of the clocks behind a
    representation, truncated to the second, and neither the maximum
    nor the truncation is sound under transaction-start clocks. Task
    #650 dropped the date validator, so the enum the metrics event
    mirrors has exactly one member.
    """
    assert [kind.value for kind in PreconditionKind] == ["etag"]


def test_microseconds_are_exact_for_a_present_day_instant() -> None:
    """A clock reaches the ETag without float rounding.

    ``datetime.timestamp()`` returns a float, and a present-day instant
    in microseconds needs 16 significant digits — right at the edge of
    what a float64 holds exactly. Integer arithmetic keeps two clocks a
    microsecond apart distinguishable.
    """
    base = datetime(2026, 9, 14, 19, 15, 29, 654_321, tzinfo=UTC)

    assert datetime_to_microseconds(base) == 1_789_413_329_654_321
    assert (
        datetime_to_microseconds(base + timedelta(microseconds=1))
        - datetime_to_microseconds(base)
        == 1
    )
