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

from typing import Annotated, Any, TypeAlias

import base32_lib
from pydantic import PlainSerializer, PlainValidator

__all__ = [
    "BASE32_ID_LENGTH",
    "BASE32_ID_SPLIT_EVERY",
    "Base32Id",
    "generate_base32_id",
    "serialize_base32_id",
    "validate_base32_id",
]

BASE32_ID_LENGTH = 12
"""Default length for Base32Id before checksum."""

BASE32_ID_SPLIT_EVERY = 4
"""Default number of characters between hyphens for Base32Id."""


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


Base32Id: TypeAlias = Annotated[  # noqa: UP040
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
