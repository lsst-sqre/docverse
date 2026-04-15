"""Request-input validation helpers that raise HTTP exceptions."""

from __future__ import annotations

from docverse.domain.base32id import validate_base32_id
from docverse.exceptions import InvalidBase32IdError

__all__ = ["parse_base32_id"]


def parse_base32_id(value: str, *, resource: str) -> int:
    """Validate a base32 ID or raise InvalidBase32IdError (HTTP 422)."""
    try:
        return validate_base32_id(value)
    except ValueError as exc:
        msg = f"Invalid {resource} ID {value!r}"
        raise InvalidBase32IdError(msg) from exc
