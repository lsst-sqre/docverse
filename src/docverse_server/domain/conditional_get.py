"""Conditional-GET primitives: weak ETags and precondition evaluation.

HTTP conditional requests (:rfc:`9110`) let a poller ask "has this
changed?" and be answered with an empty 304 instead of a full
representation. Docverse uses that on the read endpoints Ook polls, so
a pass that finds nothing new costs a watermark query rather than a
serialized page of projects.

The entity-tag is the **only** validator Docverse offers. A
``Last-Modified`` can only name an instant, which forces it to be the
maximum of the clocks behind a representation truncated to the second —
and neither the maximum nor the truncation is sound here. Every row is
stamped with PostgreSQL's transaction *start* time, so a writer that
waited on a lock can commit a clock below a maximum a poller already
holds; and a write landing later in the second a client was told about
is invisible to a comparison of truncated dates. An opaque tag has
neither constraint: it hashes each clock separately at full precision,
so any of them moving in either direction retires it. Offering only the
tag is :rfc:`9110`-conformant, and it is why ``Last-Modified`` and
``If-Modified-Since`` appear nowhere in this module.

The remaining rules — weak comparison, ``*``, a comma inside an opaque
tag — are fiddly enough to be worth isolating, so they live here as
pure functions over plain values. No handler, no request object, no
database: a caller supplies the validator material it computed and the
request header, and gets back a decision it can act on. That keeps the
semantics testable without a server and keeps every endpoint answering
the same way.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

__all__ = [
    "ETAG_DIGEST_LENGTH",
    "ConditionalGetResult",
    "PreconditionKind",
    "datetime_to_microseconds",
    "evaluate_preconditions",
    "make_weak_etag",
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
        endpoint name, the resource's public id, every clock behind it,
        and the request's canonical query string. Order is significant,
        and each part is rendered with :func:`str`, so callers must
        pass values with a stable textual form (ints, not floats).

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


class PreconditionKind(StrEnum):
    """Which request header decided a conditional GET."""

    etag = "etag"
    """``If-None-Match`` was present and was evaluated."""


@dataclass(frozen=True, slots=True)
class ConditionalGetResult:
    """The outcome of evaluating a request's preconditions."""

    not_modified: bool
    """Whether the caller already holds the current representation."""

    precondition: PreconditionKind | None
    """Which header decided, or ``None`` if the request carried none.

    ``None`` also covers a request that carried only an
    ``If-Modified-Since``. Docverse publishes no date validator, so
    there is nothing for such a header to be compared against and it is
    ignored — which leaves the request indistinguishable from an
    unconditional one.
    """


_ENTITY_TAG_RE = re.compile(r'(?:W/)?"[^"]*"|\*')
"""Matches one entity-tag, or ``*``, in an ``If-None-Match`` list.

Scanning for tags rather than splitting on commas keeps a comma inside
an opaque tag — legal per :rfc:`9110` — from splitting it in two.
"""


def _opaque_tag(entity_tag: str) -> str:
    """Strip a ``W/`` prefix, leaving the quoted opaque tag.

    ``If-None-Match`` uses the *weak* comparison function, under which
    ``W/"x"`` and ``"x"`` are the same tag.
    """
    return entity_tag.removeprefix("W/")


def evaluate_preconditions(
    *, if_none_match: str | None, etag: str
) -> ConditionalGetResult:
    """Decide whether a conditional GET may be answered with a 304.

    Parameters
    ----------
    if_none_match
        The request's ``If-None-Match`` header, or ``None``.
    etag
        The entity-tag of the representation the server would send.

    Returns
    -------
    ConditionalGetResult
        Whether to answer 304, and which header decided.

    Notes
    -----
    ``If-Modified-Since`` is deliberately not a parameter. :rfc:`9110`
    §13.1.3 makes the date validator optional, and Docverse sends no
    ``Last-Modified`` for one to be compared against — see this
    module's docstring for why a date cannot express these watermarks
    soundly. A request carrying one alone is therefore answered in full
    and reports no deciding header, exactly as an unparsable date
    always was.
    """
    if if_none_match is None:
        return ConditionalGetResult(not_modified=False, precondition=None)
    return ConditionalGetResult(
        not_modified=_if_none_match_matches(if_none_match, etag),
        precondition=PreconditionKind.etag,
    )


def _if_none_match_matches(header: str, etag: str) -> bool:
    """Evaluate an ``If-None-Match`` list against the current tag."""
    candidates = _ENTITY_TAG_RE.findall(header)
    if "*" in candidates:
        return True
    current = _opaque_tag(etag)
    return any(_opaque_tag(candidate) == current for candidate in candidates)
