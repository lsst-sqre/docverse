"""Shared path parameter type aliases for handler functions."""

from __future__ import annotations

from typing import Annotated

from fastapi import Path

__all__ = [
    "LTD_SLUG_MAX_LENGTH",
    "LTD_SLUG_PATTERN",
    "BuildIdParam",
    "CredentialLabelParam",
    "EditionSlugParam",
    "JobIdParam",
    "LtdSlugParam",
    "MemberIdParam",
    "OrgSlugParam",
    "ProjectSlugParam",
    "RunIdParam",
    "ServiceLabelParam",
    "TombstoneIdParam",
]

LTD_SLUG_MAX_LENGTH = 255
"""Longest LTD product slug accepted in a path.

LTD Keeper stores product slugs in a ``Unicode(255)`` column, so nothing
longer can name a product that exists. The cap is load-bearing rather
than cosmetic: the segment is matched against the org's
operator-supplied scope patterns, whose worst-case cost grows with the
length of the subject string, and the length and count caps are what
PRD #667 chose over a match timeout or an alternative regex engine.
"""

LTD_SLUG_PATTERN = r"^[a-z][-a-z0-9]*[a-z0-9]$"
"""What LTD Keeper accepts for a product slug.

Equivalent to LTD's own ``^[a-z]+[-a-z0-9]*[a-z0-9]+$``: lowercase
letters, digits and hyphens, opening on a letter and closing on a letter
or digit, never shorter than two characters. A segment outside this
class names no product LTD could have issued, so the keeper-sync scope
never needs to be consulted about it.
"""

CredentialLabelParam = Annotated[
    str,
    Path(alias="credential", description="Credential label."),
]

OrgSlugParam = Annotated[
    str, Path(alias="org", description="Organization slug.")
]
ProjectSlugParam = Annotated[
    str, Path(alias="project", description="Project slug.")
]
EditionSlugParam = Annotated[
    str, Path(alias="edition", description="Edition slug.")
]
BuildIdParam = Annotated[
    str,
    Path(alias="build", description="Base32-encoded build identifier."),
]
MemberIdParam = Annotated[
    str,
    Path(
        alias="member",
        description=(
            "Member identifier in ``{type}:{principal}`` format"
            " (e.g., ``user:someuser``)."
        ),
    ),
]
JobIdParam = Annotated[
    str,
    Path(alias="job", description="Base32-encoded queue job identifier."),
]
RunIdParam = Annotated[
    str,
    Path(
        alias="run", description="Base32-encoded keeper-sync run identifier."
    ),
]
LtdSlugParam = Annotated[
    str,
    Path(
        max_length=LTD_SLUG_MAX_LENGTH,
        pattern=LTD_SLUG_PATTERN,
        description=(
            "LTD Keeper product slug: lowercase letters, digits and"
            " hyphens, starting with a letter and ending with a letter or"
            f" digit, at most {LTD_SLUG_MAX_LENGTH} characters."
        ),
        examples=["sqr-112"],
    ),
]
ServiceLabelParam = Annotated[
    str,
    Path(alias="service", description="Service label."),
]
TombstoneIdParam = Annotated[
    str,
    Path(
        alias="tombstone",
        description="Base32-encoded keeper-sync tombstone identifier.",
    ),
]
