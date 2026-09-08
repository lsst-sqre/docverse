"""Domain models for the daily resource-inventory census (SQR-112).

The census is a read-only snapshot of every organization's and
non-deleted project's active resource counts, taken once a day by the
``inventory_census`` worker and published as the ``resource_inventory``
Sasquatch gauge. The models are deliberately flat scalar counters so the
worker can map each row straight onto an event payload.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "InventoryCensus",
    "OrgInventoryCensus",
    "ProjectInventoryCensus",
]


class ProjectInventoryCensus(BaseModel):
    """Active-resource counts for one non-deleted project.

    Soft-deleted editions and builds are excluded from the counts, and a
    project only produces one of these rows when it is itself
    non-deleted, so a soft-deleted project's editions and builds never
    appear on a project row — its reap-pending builds still reach the
    org roll-up, which is where the bytes a purge owes back are visible.
    """

    model_config = ConfigDict(frozen=True)

    org_id: int = Field(description="ID of the owning organization.")
    org_slug: str = Field(description="Slug of the owning organization.")
    project_id: int = Field(description="ID of the project.")
    project_slug: str = Field(description="Slug of the project.")
    edition_count: int = Field(
        description="Number of non-deleted editions in the project."
    )
    build_count: int = Field(
        description="Number of non-deleted builds in the project."
    )
    total_build_bytes: int = Field(
        description=(
            "Summed ``total_size_bytes`` of the project's non-deleted"
            " builds; 0 when there are none or every size is unset."
        )
    )
    purgatory_build_count: int = Field(
        description=(
            "Number of the project's builds that are soft-deleted but not"
            " yet purged."
        )
    )
    purgatory_bytes: int = Field(
        description=(
            "Summed ``total_size_bytes`` of the project's soft-deleted,"
            " not-yet-purged builds."
        )
    )


class OrgInventoryCensus(BaseModel):
    """Active-resource counts for one organization.

    Every organization yields exactly one of these rows, even one with no
    projects (``project_count == 0``). The edition/build counts and byte
    sum are the roll-up of the org's non-deleted projects. The purgatory
    counters are the exception: they span every project of the org,
    deleted or not, because deleting a project is exactly what sends its
    builds' storage into purgatory.
    """

    model_config = ConfigDict(frozen=True)

    org_id: int = Field(description="ID of the organization.")
    org_slug: str = Field(description="Slug of the organization.")
    project_count: int = Field(
        description="Number of non-deleted projects in the organization."
    )
    edition_count: int = Field(
        description="Number of non-deleted editions across the org's projects."
    )
    build_count: int = Field(
        description="Number of non-deleted builds across the org's projects."
    )
    total_build_bytes: int = Field(
        description=(
            "Summed ``total_size_bytes`` of every non-deleted build"
            " across the org's projects."
        )
    )
    purgatory_build_count: int = Field(
        description=(
            "Number of soft-deleted, not-yet-purged builds across all of"
            " the org's projects, including soft-deleted ones."
        )
    )
    purgatory_bytes: int = Field(
        description=(
            "Summed ``total_size_bytes`` of every soft-deleted,"
            " not-yet-purged build across all of the org's projects,"
            " including soft-deleted ones."
        )
    )


class InventoryCensus(BaseModel):
    """A full census snapshot: one org row per org, one per live project."""

    model_config = ConfigDict(frozen=True)

    orgs: list[OrgInventoryCensus] = Field(
        description="One census row per organization."
    )
    projects: list[ProjectInventoryCensus] = Field(
        description="One census row per non-deleted project."
    )
