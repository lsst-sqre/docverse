"""Pydantic models for project / organization lifecycle rules.

Lifecycle rules tell the Docverse ``lifecycle_eval`` background job
which editions and builds should be soft-deleted from a project. The
rule schema is a tagged union discriminated on ``type`` so operators
get an early 422 with a discriminator-aware error message when a
PATCH payload names an unknown rule kind or omits a required field
for a known one.
"""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    RootModel,
    model_validator,
)

__all__ = [
    "BuildHistoryOrphanRule",
    "DraftInactivityRule",
    "LifecycleRule",
    "LifecycleRuleSet",
    "RefDeletedRule",
]


class DraftInactivityRule(BaseModel):
    """Soft-delete ``kind=draft`` editions with no recent builds.

    The ``lifecycle_eval`` evaluator matches draft editions whose
    ``date_updated`` is older than ``max_days_inactive`` days. Other
    edition kinds (``release``, ``main``, ``alternate``) are never
    candidates for this rule, regardless of staleness.
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["draft_inactivity"] = Field(
        default="draft_inactivity",
        description="Discriminator tag for this rule kind.",
    )

    max_days_inactive: int = Field(
        ge=1,
        description=(
            "Maximum number of days a draft edition may remain without a"
            " new build before it becomes a soft-delete candidate."
        ),
    )


class BuildHistoryOrphanRule(BaseModel):
    """Soft-delete builds that have fallen out of every edition's history.

    A build is a candidate when it is not the ``current_build_id`` of
    any active edition, every ``edition_build_history`` row referencing
    it has ``position >= min_position``, and its completion age is at
    least ``min_age_days``.
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["build_history_orphan"] = Field(
        default="build_history_orphan",
        description="Discriminator tag for this rule kind.",
    )

    min_position: int = Field(
        ge=1,
        description=(
            "Minimum rollback-history position above which a build is"
            " considered out of rotation."
        ),
    )

    min_age_days: int = Field(
        ge=0,
        description=(
            "Minimum age in days (from completion) before an"
            " out-of-rotation build is eligible for deletion."
        ),
    )


class RefDeletedRule(BaseModel):
    """Soft-delete editions whose source git ref no longer exists.

    Carries no parameters: the rule's presence in a ``LifecycleRuleSet``
    is the toggle that activates the check. The evaluator branch for
    this rule is owned by DM-54913; the schema is defined here so
    operators can configure the rule in advance and the eventual
    evaluator only needs to swap in the predicate.
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["ref_deleted"] = Field(
        default="ref_deleted",
        description="Discriminator tag for this rule kind.",
    )


LifecycleRule = Annotated[
    DraftInactivityRule | BuildHistoryOrphanRule | RefDeletedRule,
    Discriminator("type"),
]
"""Discriminated union of all lifecycle-rule variants."""


class LifecycleRuleSet(RootModel[list[LifecycleRule]]):
    """Root model wrapping a list of lifecycle rules.

    Enforces that each rule ``type`` appears at most once per set,
    matching the SQR-112 semantics where rules are keyed by kind and
    a second rule of the same kind would be ambiguous.
    """

    root: list[LifecycleRule] = Field(
        default_factory=list,
        description="Lifecycle rules for the enclosing scope.",
    )

    @model_validator(mode="after")
    def _reject_duplicate_rule_types(self) -> Self:
        seen: set[str] = set()
        for rule in self.root:
            if rule.type in seen:
                msg = (
                    f"duplicate lifecycle rule type {rule.type!r};"
                    " each rule type may appear at most once per set"
                )
                raise ValueError(msg)
            seen.add(rule.type)
        return self
