"""Tests for the conditional-GET domain module."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from docverse_server.domain.conditional_get import (
    PreconditionKind,
    datetime_to_microseconds,
    evaluate_preconditions,
    format_http_date,
    make_weak_etag,
    parse_http_date,
    truncate_to_second,
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


def test_http_date_round_trips_through_second_truncation() -> None:
    """Formatting drops sub-second precision; parsing recovers the rest."""
    stamp = datetime(2026, 9, 14, 19, 15, 29, 654_321, tzinfo=UTC)

    formatted = format_http_date(stamp)

    assert formatted == "Mon, 14 Sep 2026 19:15:29 GMT"
    assert truncate_to_second(stamp) == datetime(
        2026, 9, 14, 19, 15, 29, tzinfo=UTC
    )
    assert parse_http_date(formatted) == truncate_to_second(stamp)


WATERMARK = datetime(2026, 9, 14, 19, 15, 29, 654_321, tzinfo=UTC)
"""Stand-in resource watermark for the precondition tests."""

ETAG = 'W/"0123456789abcdef0123456789abcdef"'
"""Stand-in current entity-tag for the precondition tests."""


def test_matching_if_none_match_is_not_modified() -> None:
    """Echoing back the current tag earns a 304 decided by the ETag."""
    result = evaluate_preconditions(
        if_none_match=ETAG,
        if_modified_since=None,
        etag=ETAG,
        last_modified=WATERMARK,
    )

    assert result.not_modified is True
    assert result.precondition is PreconditionKind.etag


def test_star_if_none_match_is_not_modified() -> None:
    """``*`` matches any current representation."""
    result = evaluate_preconditions(
        if_none_match="*",
        if_modified_since=None,
        etag=ETAG,
        last_modified=WATERMARK,
    )

    assert result.not_modified is True
    assert result.precondition is PreconditionKind.etag


def test_if_none_match_compares_weakly() -> None:
    """A strong tag echoed back matches the weak tag that was sent."""
    result = evaluate_preconditions(
        if_none_match='"nomatch", "0123456789abcdef0123456789abcdef"',
        if_modified_since=None,
        etag=ETAG,
        last_modified=WATERMARK,
    )

    assert result.not_modified is True


def test_if_none_match_outranks_if_modified_since() -> None:
    """A stale tag wins over a date that would have said 304.

    RFC 7232 evaluates ``If-Modified-Since`` only when there is no
    ``If-None-Match`` at all, so a client holding an out-of-date tag
    gets the new representation even if it also sends a recent date.
    """
    result = evaluate_preconditions(
        if_none_match='W/"stale"',
        if_modified_since=format_http_date(WATERMARK),
        etag=ETAG,
        last_modified=WATERMARK,
    )

    assert result.not_modified is False
    assert result.precondition is PreconditionKind.etag


def test_unparsable_if_modified_since_is_ignored() -> None:
    """A malformed date leaves the request effectively unconditional."""
    result = evaluate_preconditions(
        if_none_match=None,
        if_modified_since="not a date",
        etag=ETAG,
        last_modified=WATERMARK,
    )

    assert result.not_modified is False
    assert result.precondition is None


def test_if_modified_since_alone_is_not_modified_when_clock_is_still() -> None:
    """Echoing back the ``Last-Modified`` we sent earns a 304."""
    result = evaluate_preconditions(
        if_none_match=None,
        if_modified_since=format_http_date(WATERMARK),
        etag=ETAG,
        last_modified=WATERMARK,
    )

    assert result.not_modified is True
    assert result.precondition is PreconditionKind.last_modified


def test_if_modified_since_alone_is_modified_once_the_clock_moves() -> None:
    """A watermark past the client's date is a fresh representation."""
    result = evaluate_preconditions(
        if_none_match=None,
        if_modified_since=format_http_date(WATERMARK - timedelta(seconds=30)),
        etag=ETAG,
        last_modified=WATERMARK,
    )

    assert result.not_modified is False
    assert result.precondition is PreconditionKind.last_modified


def test_request_without_preconditions_decides_nothing() -> None:
    """An unconditional GET reports no deciding header."""
    result = evaluate_preconditions(
        if_none_match=None,
        if_modified_since=None,
        etag=ETAG,
        last_modified=WATERMARK,
    )

    assert result.not_modified is False
    assert result.precondition is None


def test_microseconds_are_exact_for_a_present_day_instant() -> None:
    """The watermark reaches the ETag without float rounding.

    ``datetime.timestamp()`` returns a float, and a present-day instant
    in microseconds needs 16 significant digits — right at the edge of
    what a float64 holds exactly. Integer arithmetic keeps two
    watermarks a microsecond apart distinguishable.
    """
    base = datetime(2026, 9, 14, 19, 15, 29, 654_321, tzinfo=UTC)

    assert datetime_to_microseconds(base) == 1_789_413_329_654_321
    assert (
        datetime_to_microseconds(base + timedelta(microseconds=1))
        - datetime_to_microseconds(base)
        == 1
    )
