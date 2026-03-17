"""Shared path parameter type aliases for handler functions."""

from __future__ import annotations

from typing import Annotated

from fastapi import Path

__all__ = [
    "BuildIdParam",
    "EditionSlugParam",
    "JobIdParam",
    "MemberIdParam",
    "OrgSlugParam",
    "ProjectSlugParam",
]

OrgSlugParam = Annotated[str, Path(alias="org")]
ProjectSlugParam = Annotated[str, Path(alias="project")]
EditionSlugParam = Annotated[str, Path(alias="edition")]
BuildIdParam = Annotated[str, Path(alias="build")]
MemberIdParam = Annotated[str, Path(alias="member")]
JobIdParam = Annotated[str, Path(alias="job")]
