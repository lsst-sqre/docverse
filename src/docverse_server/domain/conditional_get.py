"""Conditional-GET primitives: weak ETags, HTTP dates, preconditions.

HTTP conditional requests (:rfc:`7232`) let a poller ask "has this
changed?" and be answered with an empty 304 instead of a full
representation. Docverse uses that on the read endpoints Ook polls, so
a pass that finds nothing new costs a watermark query rather than a
serialized page of projects.

The rules are fiddly and easy to get subtly wrong — weak comparison,
``If-None-Match`` outranking ``If-Modified-Since``, second-granularity
HTTP dates — so they live here as pure functions over plain values. No
handler, no request object, no database: a caller supplies the
validator material it computed and the two request headers, and gets
back a decision it can act on. That keeps the semantics testable
without a server and keeps every endpoint answering the same way.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime, parsedate_to_datetime
from enum import StrEnum

__all__ = [
    "ETAG_DIGEST_LENGTH",
    "ConditionalGetResult",
    "PreconditionKind",
    "datetime_to_microseconds",
    "evaluate_preconditions",
    "format_http_date",
    "make_weak_etag",
    "parse_http_date",
    "truncate_to_second",
]

ETAG_DIGEST_LENGTH = 32
"""Number of hex characters of the SHA-256 digest kept in an ETag.

128 bits is far past the point where two distinct representations
collide by accident, and a shorter tag keeps the header small on a
listing that is polled continuously.
"""

_POSIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
"""Origin for :func:`datetime_to_microseconds`."""

_FIELD_SEPARATOR = "\x1f"
"""ASCII unit separator joining the parts of an ETag's material.

A character that cannot appear in any part we hash (org ids and
microsecond counts are integers; the query string is percent-encoded),
so ``("ab", "c")`` and ``("a", "bc")`` cannot hash to the same tag the
way plain concatenation would let them.
"""


def make_weak_etag(parts: Sequence[object]) -> str:
    """Build a weak entity-tag over a canonical tuple of validator parts.

    Parameters
    ----------
    parts
        The values that identify this representation — typically the
        endpoint name, the resource's public id, its watermark, and the
        request's canonical query string. Order is significant, and
        each part is rendered with :func:`str`, so callers must pass
        values with a stable textual form (ints, not floats).

    Returns
    -------
    str
        A weak entity-tag of the form ``W/"<hex digest>"``, ready to be
        sent as an ``ETag`` header.

    Notes
    -----
    The tag is **weak** because it marks semantic equivalence rather
    than byte-for-byte identity: two responses with the same material
    describe the same resource state even if, say, a serializer tweak
    reordered a JSON key.
    """
    material = _FIELD_SEPARATOR.join(str(part) for part in parts)
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return f'W/"{digest[:ETAG_DIGEST_LENGTH]}"'


def datetime_to_microseconds(value: datetime) -> int:
    """Render an instant as whole microseconds since the POSIX epoch.

    ETag material has to be textually stable, and
    :meth:`datetime.datetime.timestamp` is not: it returns a float, and
    a present-day instant expressed in microseconds sits at the limit
    of float64's exact-integer range, so two watermarks a microsecond
    apart can render identically. Subtracting two datetimes and
    dividing the :class:`~datetime.timedelta` is exact integer
    arithmetic.

    A naive value is read as UTC.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return (value - _POSIX_EPOCH) // timedelta(microseconds=1)


def truncate_to_second(value: datetime) -> datetime:
    """Drop a timestamp's sub-second precision.

    HTTP dates carry whole seconds only, so a ``Last-Modified`` header
    is necessarily a truncation of the watermark behind it. Comparing
    an ``If-Modified-Since`` against the *untruncated* watermark would
    therefore report "modified" forever: the client echoes back the
    second it was told, which is always fractionally older than the
    real instant.
    """
    return value.replace(microsecond=0)


def format_http_date(value: datetime) -> str:
    """Render an instant as an :rfc:`7231` IMF-fixdate in GMT.

    Parameters
    ----------
    value
        The instant to render. A naive value is read as UTC; an aware
        one is converted to UTC first, because the fixdate form admits
        no offset other than ``GMT``.

    Returns
    -------
    str
        For example ``"Mon, 14 Sep 2026 19:15:29 GMT"``, with
        sub-second precision truncated away.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return format_datetime(
        truncate_to_second(value.astimezone(UTC)), usegmt=True
    )


def parse_http_date(value: str) -> datetime | None:
    """Parse an HTTP-date header value, or ``None`` if it is malformed.

    :rfc:`7232` says a recipient must ignore an ``If-Modified-Since``
    it cannot parse rather than reject the request, so the failure is
    reported as ``None`` instead of an exception — there is nothing for
    a caller to handle beyond "carry on unconditionally".

    A parsed value with no timezone is read as UTC: HTTP dates are GMT
    by definition, and :func:`email.utils.parsedate_to_datetime` leaves
    an obsolete ``-0000`` offset naive.
    """
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class PreconditionKind(StrEnum):
    """Which request header decided a conditional GET."""

    etag = "etag"
    """``If-None-Match`` was present and was evaluated."""

    last_modified = "last_modified"
    """``If-Modified-Since`` was present, parsable, and was evaluated."""


@dataclass(frozen=True, slots=True)
class ConditionalGetResult:
    """The outcome of evaluating a request's preconditions."""

    not_modified: bool
    """Whether the caller already holds the current representation."""

    precondition: PreconditionKind | None
    """Which header decided, or ``None`` if the request carried none.

    ``None`` also covers an ``If-Modified-Since`` that could not be
    parsed: :rfc:`7232` says to ignore one, and a header that was
    ignored decided nothing, so the request is indistinguishable from
    an unconditional one.
    """


_ENTITY_TAG_RE = re.compile(r'(?:W/)?"[^"]*"|\*')
"""Matches one entity-tag, or ``*``, in an ``If-None-Match`` list.

Scanning for tags rather than splitting on commas keeps a comma inside
an opaque tag — legal per :rfc:`7232` — from splitting it in two.
"""


def _opaque_tag(entity_tag: str) -> str:
    """Strip a ``W/`` prefix, leaving the quoted opaque tag.

    ``If-None-Match`` uses the *weak* comparison function, under which
    ``W/"x"`` and ``"x"`` are the same tag.
    """
    return entity_tag.removeprefix("W/")


def evaluate_preconditions(
    *,
    if_none_match: str | None,
    if_modified_since: str | None,
    etag: str,
    last_modified: datetime,
) -> ConditionalGetResult:
    """Decide whether a conditional GET may be answered with a 304.

    Parameters
    ----------
    if_none_match
        The request's ``If-None-Match`` header, or ``None``.
    if_modified_since
        The request's ``If-Modified-Since`` header, or ``None``.
    etag
        The entity-tag of the representation the server would send.
    last_modified
        The watermark behind that representation, at full precision;
        it is truncated to the second here so the comparison matches
        what the ``Last-Modified`` header actually told the client.

    Returns
    -------
    ConditionalGetResult
        Whether to answer 304, and which header decided.

    Notes
    -----
    :rfc:`7232` §6 fixes the precedence: when ``If-None-Match`` is
    present it is evaluated and ``If-Modified-Since`` is not consulted
    at all — *even when the tags do not match*. The date is a fallback
    for clients that have no tag to echo, never a second opinion.
    """
    if if_none_match is not None:
        return ConditionalGetResult(
            not_modified=_if_none_match_matches(if_none_match, etag),
            precondition=PreconditionKind.etag,
        )
    if if_modified_since is not None:
        since = parse_http_date(if_modified_since)
        if since is not None:
            return ConditionalGetResult(
                not_modified=truncate_to_second(last_modified) <= since,
                precondition=PreconditionKind.last_modified,
            )
    return ConditionalGetResult(not_modified=False, precondition=None)


def _if_none_match_matches(header: str, etag: str) -> bool:
    """Evaluate an ``If-None-Match`` list against the current tag."""
    candidates = _ENTITY_TAG_RE.findall(header)
    if "*" in candidates:
        return True
    current = _opaque_tag(etag)
    return any(_opaque_tag(candidate) == current for candidate in candidates)
