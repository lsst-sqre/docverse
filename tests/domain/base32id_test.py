"""Tests for the Crockford Base32 identifier helpers."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from itertools import pairwise

import pytest

from docverse.domain.base32id import (
    RESOURCE_ID_EPOCH,
    RESOURCE_ID_RANDOM_BITS,
    RESOURCE_ID_TIMESTAMP_BITS,
    generate_resource_id,
    mint_resource_id_for_timestamp,
    mint_time_ordered_resource_ids,
    serialize_base32_id,
    validate_base32_id,
)

# 12 payload characters plus a 2-character checksum, hyphenated every 4.
_ENVELOPE_RE = re.compile(
    r"^[0-9a-hjkmnp-tv-z]{4}(?:-[0-9a-hjkmnp-tv-z]{2,4})+$"
)


def test_bit_layout_sums_to_envelope() -> None:
    """The timestamp and random bits fill exactly the 60-bit envelope."""
    assert RESOURCE_ID_TIMESTAMP_BITS + RESOURCE_ID_RANDOM_BITS == 60


def test_resource_id_fits_envelope_length_and_format() -> None:
    """A minted ID serializes to a 12+2 hyphenated Crockford string."""
    resource_id = mint_resource_id_for_timestamp(datetime.now(tz=UTC))
    assert resource_id < (1 << 60)

    serialized = serialize_base32_id(resource_id)
    # 14 payload characters (12 + 2 checksum) plus hyphens.
    assert len(serialized.replace("-", "")) == 14
    assert _ENVELOPE_RE.match(serialized)


def test_checksum_round_trip() -> None:
    """Serialize -> validate returns the original integer."""
    resource_id = mint_resource_id_for_timestamp(datetime.now(tz=UTC))
    serialized = serialize_base32_id(resource_id)
    assert validate_base32_id(serialized) == resource_id


def test_high_bits_decode_to_milliseconds_since_epoch() -> None:
    """The high 43 bits decode to milliseconds since 2010-01-01 UTC."""
    timestamp = datetime(2026, 7, 20, 12, 34, 56, 789000, tzinfo=UTC)
    resource_id = mint_resource_id_for_timestamp(timestamp)

    milliseconds = resource_id >> RESOURCE_ID_RANDOM_BITS
    expected_ms = (timestamp - RESOURCE_ID_EPOCH) // timedelta(milliseconds=1)
    assert milliseconds == expected_ms

    recovered = RESOURCE_ID_EPOCH + timedelta(milliseconds=milliseconds)
    # Millisecond truncation drops sub-millisecond precision only.
    assert recovered == timestamp.replace(microsecond=789000)


def test_creation_order_monotonicity() -> None:
    """Later timestamps mint strictly larger IDs."""
    base = datetime(2026, 1, 1, tzinfo=UTC)
    ids = [
        mint_resource_id_for_timestamp(base + timedelta(milliseconds=n))
        for n in range(50)
    ]
    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)


def test_mint_for_timestamp_deterministic_modulo_random_bits() -> None:
    """A fixed timestamp yields a stable high-bit prefix across calls."""
    timestamp = datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)
    highs = {
        mint_resource_id_for_timestamp(timestamp) >> RESOURCE_ID_RANDOM_BITS
        for _ in range(20)
    }
    assert len(highs) == 1


def test_mint_time_ordered_strictly_increasing_same_millisecond() -> None:
    """Identical timestamps still yield a strictly increasing sequence."""
    timestamp = datetime(2025, 3, 3, 3, 3, 3, tzinfo=UTC)
    ids = mint_time_ordered_resource_ids([timestamp] * 100)
    assert all(later > earlier for earlier, later in pairwise(ids))


def test_mint_time_ordered_tracks_wall_clock() -> None:
    """Ascending timestamps produce a strictly increasing sequence."""
    base = datetime(2025, 3, 3, tzinfo=UTC)
    timestamps = [base + timedelta(seconds=n) for n in range(10)]
    ids = mint_time_ordered_resource_ids(timestamps)
    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)


def test_generate_resource_id_is_time_ordered() -> None:
    """generate_resource_id mints within the envelope for the current time."""
    before = datetime.now(tz=UTC) - timedelta(seconds=1)
    resource_id = generate_resource_id()
    after = datetime.now(tz=UTC) + timedelta(seconds=1)

    milliseconds = resource_id >> RESOURCE_ID_RANDOM_BITS
    minted_at = RESOURCE_ID_EPOCH + timedelta(milliseconds=milliseconds)
    assert before <= minted_at <= after


def test_mint_rejects_naive_timestamp() -> None:
    """A timezone-naive timestamp is rejected."""
    with pytest.raises(ValueError, match="timezone-aware"):
        mint_resource_id_for_timestamp(datetime(2026, 1, 1))  # noqa: DTZ001


def test_mint_rejects_pre_epoch_timestamp() -> None:
    """A timestamp before the epoch is rejected."""
    with pytest.raises(ValueError, match="epoch"):
        mint_resource_id_for_timestamp(datetime(2009, 12, 31, tzinfo=UTC))
