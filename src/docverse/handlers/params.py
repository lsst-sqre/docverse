"""Shared path parameter type aliases for handler functions."""

from __future__ import annotations

from typing import Annotated

from fastapi import Path

__all__ = [
    "BuildIdParam",
    "CredentialLabelParam",
    "EditionSlugParam",
    "JobIdParam",
    "MemberIdParam",
    "OrgSlugParam",
    "ProjectSlugParam",
    "RunIdParam",
    "ServiceLabelParam",
    "TombstoneIdParam",
]

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
