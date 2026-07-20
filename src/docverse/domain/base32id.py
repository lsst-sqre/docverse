"""Pydantic helpers for Crockford Base32 identifiers with checksum support.

This module provides utilities to create Pydantic fields that validate and
serialize Crockford Base32 identifiers using the base32-lib package. The
identifiers are stored internally as integers but validated from and
serialized to base32 strings with checksums.

References
----------
- base32-lib: https://base32-lib.readthedocs.io/en/latest/
- Ported from ook: https://github.com/lsst-sqre/ook
"""

from __future__ import annotations

import secrets
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, TypeAlias

import base32_lib
from pydantic import PlainSerializer, PlainValidator

__all__ = [
    "BASE32_ID_LENGTH",
    "BASE32_ID_SPLIT_EVERY",
    "RESOURCE_ID_EPOCH",
    "RESOURCE_ID_RANDOM_BITS",
    "RESOURCE_ID_TIMESTAMP_BITS",
    "Base32Id",
    "generate_base32_id",
    "generate_resource_id",
    "mint_resource_id_for_timestamp",
    "mint_time_ordered_resource_ids",
    "serialize_base32_id",
    "validate_base32_id",
]

BASE32_ID_LENGTH = 12
"""Default length for Base32Id before checksum."""

BASE32_ID_SPLIT_EVERY = 4
"""Default number of characters between hyphens for Base32Id."""

RESOURCE_ID_EPOCH = datetime(2010, 1, 1, tzinfo=UTC)
"""Fixed epoch (2010-01-01T00:00:00Z) for time-ordered resource IDs.

This epoch is deliberately fixed for all time and is **not** configurable. It
predates Rubin Observatory project records, so any table ported to this ID
scheme can re-mint IDs from each row's ``date_created`` without ever hitting a
pre-epoch timestamp. A single org-wide constant also avoids the silent
misordering that per-service epoch configuration would invite.
"""

RESOURCE_ID_TIMESTAMP_BITS = 43
"""High-order bits encoding milliseconds since `RESOURCE_ID_EPOCH`.

43 bits of milliseconds gives roughly 278 years of runway from the epoch
(to roughly the year 2288).
"""

RESOURCE_ID_RANDOM_BITS = 17
"""Low-order random bits (~131k distinct values per millisecond).

`RESOURCE_ID_TIMESTAMP_BITS` + `RESOURCE_ID_RANDOM_BITS` == 60, which is the
Crockford Base32 envelope (12 characters at 5 bits each) that `Base32Id`
serializes.
"""


def validate_base32_id(value: Any) -> int:
    """Validate a base32 identifier and return the integer value.

    Parameters
    ----------
    value
        The identifier value as an integer or base32 string with checksum.

    Returns
    -------
    int
        The decoded integer value.

    Raises
    ------
    ValueError
        If the value is not a valid integer or base32 string with checksum.
    """
    if isinstance(value, int):
        if value < 0:
            msg = "Base32 ID must be non-negative integer"
            raise ValueError(msg)
        return value
    if isinstance(value, str):
        try:
            clean_value = value.replace("-", "")
            decoded: int = base32_lib.decode(clean_value, checksum=True)
        except (ValueError, TypeError) as e:
            msg = f"Invalid base32 identifier: {e}"
            raise ValueError(msg) from e
        else:
            return decoded
    msg = f"Base32 ID must be int or str, got {type(value).__name__}"
    raise ValueError(msg)


def serialize_base32_id(
    value: int, *, split_every: int = 4, length: int = 12
) -> str:
    """Serialize an integer to a base32 string with checksum and hyphens.

    Parameters
    ----------
    value
        The integer value to encode.
    split_every
        Number of characters between hyphens (default: 4).
    length
        Minimum length of the base32 string before checksum (default: 12).

    Returns
    -------
    str
        Base32 string with checksum and hyphens.
    """
    encoded: str = base32_lib.encode(
        value, min_length=length, checksum=True, split_every=0
    )
    if split_every > 0:
        parts = [
            encoded[i : i + split_every]
            for i in range(0, len(encoded), split_every)
        ]
        return "-".join(parts)
    return encoded


def generate_base32_id(*, length: int = 12, split_every: int = 4) -> str:
    """Generate a new random base32 identifier with checksum.

    Parameters
    ----------
    length
        Length of the identifier before checksum (default: 12).
    split_every
        Number of characters between hyphens (default: 4).

    Returns
    -------
    str
        A new base32 identifier string with checksum and hyphens.
    """
    result: str = base32_lib.generate(
        length=length + 2, split_every=split_every, checksum=True
    )
    return result


def mint_resource_id_for_timestamp(timestamp: datetime) -> int:
    """Mint a time-ordered resource ID for a specific timestamp.

    The ID is a Snowflake-style integer inside the 60-bit Crockford Base32
    envelope: the high `RESOURCE_ID_TIMESTAMP_BITS` bits hold milliseconds
    since `RESOURCE_ID_EPOCH` and the low `RESOURCE_ID_RANDOM_BITS` bits are
    random. IDs minted for later timestamps therefore sort after earlier ones
    under plain integer (and thus Base32 keyset) ordering. This is
    deliberately *not* a truncated UUIDv7 (a 60-bit envelope cannot hold one,
    and its 1970-anchored 48-bit timestamp would leave only 12 random bits).

    Parameters
    ----------
    timestamp
        A timezone-aware timestamp at or after `RESOURCE_ID_EPOCH`. The
        re-mint migration reuses this helper to derive IDs from a row's
        ``date_created``.

    Returns
    -------
    int
        An integer that fits the 60-bit envelope, suitable for the
        ``public_id`` column and round-trippable through
        `serialize_base32_id` / `validate_base32_id`.

    Raises
    ------
    ValueError
        If the timestamp is timezone-naive, precedes `RESOURCE_ID_EPOCH`, or
        is far enough in the future to overflow the timestamp bits.
    """
    if timestamp.tzinfo is None:
        msg = "timestamp must be timezone-aware"
        raise ValueError(msg)

    milliseconds = (timestamp - RESOURCE_ID_EPOCH) // timedelta(milliseconds=1)
    if milliseconds < 0:
        msg = (
            "timestamp must not precede the resource ID epoch "
            f"({RESOURCE_ID_EPOCH.isoformat()})"
        )
        raise ValueError(msg)
    if milliseconds >= (1 << RESOURCE_ID_TIMESTAMP_BITS):
        msg = "timestamp exceeds the 60-bit resource ID timestamp envelope"
        raise ValueError(msg)

    random_bits = secrets.randbits(RESOURCE_ID_RANDOM_BITS)
    return (milliseconds << RESOURCE_ID_RANDOM_BITS) | random_bits


def mint_time_ordered_resource_ids(
    timestamps: Sequence[datetime],
) -> list[int]:
    """Mint strictly increasing, time-ordered IDs for a timestamp sequence.

    Each ID is derived from its timestamp with
    `mint_resource_id_for_timestamp`, so the sequence tracks wall-clock order.
    When consecutive timestamps fall in the same millisecond (or the random
    low bits would otherwise regress), the ID is bumped to ``previous + 1`` so
    the returned sequence is *strictly* increasing regardless of tie density.
    This is the generator a one-time backfill migration uses to reassign
    ``public_id`` values in ``date_created`` order.

    Parameters
    ----------
    timestamps
        Timezone-aware timestamps in ascending order (typically each row's
        ``date_created``). Ties are resolved by the caller's ordering.

    Returns
    -------
    list[int]
        One strictly increasing integer ID per input timestamp, each within
        the 60-bit Crockford Base32 envelope.

    Raises
    ------
    ValueError
        If any timestamp is rejected by `mint_resource_id_for_timestamp`.
    """
    ids: list[int] = []
    previous: int | None = None
    for timestamp in timestamps:
        candidate = mint_resource_id_for_timestamp(timestamp)
        if previous is not None and candidate <= previous:
            candidate = previous + 1
        ids.append(candidate)
        previous = candidate
    return ids


def generate_resource_id() -> int:
    """Mint a time-ordered resource ID for the current time.

    Returns
    -------
    int
        A time-ordered integer for a ``public_id`` column. See
        `mint_resource_id_for_timestamp` for the bit layout.
    """
    return mint_resource_id_for_timestamp(datetime.now(tz=UTC))


Base32Id: TypeAlias = Annotated[
    int,
    PlainValidator(validate_base32_id),
    PlainSerializer(
        lambda value: serialize_base32_id(
            value,
            split_every=BASE32_ID_SPLIT_EVERY,
            length=BASE32_ID_LENGTH,
        ),
        return_type=str,
        when_used="json",
    ),
]
"""Base32 identifier with checksum and hyphens (length of 12+2)."""
