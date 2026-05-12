"""Pure evaluator for lifecycle rules."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta

from pydantic import BaseModel, ConfigDict, Field

from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.edition_build_history import EditionBuildHistory
from docverse.domain.lifecycle import (
    BuildHistoryOrphanRule,
    DraftInactivityRule,
    LifecycleRuleSet,
    RefDeletedRule,
)

__all__ = ["LifecycleDecision", "evaluate_lifecycle", "resolve_rule_set"]


class LifecycleDecision(BaseModel):
    """Decision returned by :func:`evaluate_lifecycle`.

    ``edition_ids`` and ``build_ids`` are the soft-delete candidates.
    ``rule_match_counts`` maps each rule ``type`` present in the rule
    set to the number of entities that rule matched, for structured
    logging by the per-org worker.
    """

    model_config = ConfigDict(frozen=True)

    edition_ids: frozenset[int] = Field(
        default_factory=frozenset,
        description="Edition ids to soft-delete.",
    )

    build_ids: frozenset[int] = Field(
        default_factory=frozenset,
        description="Build ids to soft-delete.",
    )

    rule_match_counts: Mapping[str, int] = Field(
        default_factory=dict,
        description="Per-rule match count keyed on the rule's ``type``.",
    )


def evaluate_lifecycle(
    *,
    rule_set: LifecycleRuleSet,
    editions: Iterable[Edition],
    builds: Iterable[Build],
    edition_build_history: Iterable[EditionBuildHistory],
    now: datetime,
) -> LifecycleDecision:
    """Evaluate ``rule_set`` against pre-fetched project state.

    No database access. The caller hands in already-loaded editions,
    builds, and per-edition build-history rows; the function returns
    a :class:`LifecycleDecision` listing the entities the rule set
    has matched for soft-delete.
    """
    editions_list = list(editions)
    builds_list = list(builds)
    history_list = list(edition_build_history)

    matched_edition_ids: set[int] = set()
    matched_build_ids: set[int] = set()
    rule_match_counts: dict[str, int] = {}

    for rule in rule_set.root:
        if isinstance(rule, DraftInactivityRule):
            matches = _eval_draft_inactivity(
                rule=rule, editions=editions_list, now=now
            )
            matched_edition_ids.update(matches)
            rule_match_counts[rule.type] = len(matches)
        elif isinstance(rule, BuildHistoryOrphanRule):
            matches = _eval_build_history_orphan(
                rule=rule,
                editions=editions_list,
                builds=builds_list,
                history=history_list,
                now=now,
            )
            matched_build_ids.update(matches)
            rule_match_counts[rule.type] = len(matches)
        elif isinstance(rule, RefDeletedRule):
            # DM-54913 will swap in the real predicate; until then the
            # rule is recognized but matches nothing.
            rule_match_counts[rule.type] = 0

    return LifecycleDecision(
        edition_ids=frozenset(matched_edition_ids),
        build_ids=frozenset(matched_build_ids),
        rule_match_counts=rule_match_counts,
    )


def resolve_rule_set(
    *,
    org_rules: LifecycleRuleSet | None,
    project_rules: LifecycleRuleSet | None,
) -> LifecycleRuleSet:
    """Return the rule set that applies to a project.

    Per SQR-112: a project's rule list, when set, **entirely replaces**
    the organization-level rules — there is no merging. An explicitly
    set empty project rule set is the project-level opt-out
    mechanism (user story 3): the project disables every org-level
    rule by writing ``[]``. Only a project rule set of ``None``
    inherits the org-level rules.
    """
    if project_rules is not None:
        return project_rules
    if org_rules is not None:
        return org_rules
    return LifecycleRuleSet(root=[])


def _eval_draft_inactivity(
    *,
    rule: DraftInactivityRule,
    editions: list[Edition],
    now: datetime,
) -> set[int]:
    """Return edition ids that match ``DraftInactivityRule``."""
    threshold = now - timedelta(days=rule.max_days_inactive)
    matched: set[int] = set()
    for edition in editions:
        if edition.kind != "draft":
            continue
        if edition.lifecycle_exempt:
            continue
        if edition.date_deleted is not None:
            continue
        if edition.date_updated < threshold:
            matched.add(edition.id)
    return matched


def _eval_build_history_orphan(
    *,
    rule: BuildHistoryOrphanRule,
    editions: list[Edition],
    builds: list[Build],
    history: list[EditionBuildHistory],
    now: datetime,
) -> set[int]:
    """Return build ids that match ``BuildHistoryOrphanRule``."""
    protected = _protected_build_ids_for_orphan_rule(
        rule=rule, editions=editions, history=history
    )
    age_threshold = now - timedelta(days=rule.min_age_days)
    matched: set[int] = set()
    for build in builds:
        if build.id in protected:
            continue
        if build.date_deleted is not None:
            continue
        # date_completed is None on never-finished builds; fall back
        # to date_created so the age check has something to compare.
        completion = build.date_completed or build.date_created
        if completion > age_threshold:
            continue
        matched.add(build.id)
    return matched


def _protected_build_ids_for_orphan_rule(
    *,
    rule: BuildHistoryOrphanRule,
    editions: list[Edition],
    history: list[EditionBuildHistory],
) -> set[int]:
    """Compute the set of build ids protected from orphan deletion.

    A build is protected when any non-deleted edition holds it as
    ``current_build_id``, when an exempt non-deleted edition has it in
    its rollback history at any position, or when a non-exempt
    non-deleted edition holds it in history with
    ``position < min_position``.
    """
    editions_by_id = {e.id: e for e in editions}
    protected: set[int] = set()
    for edition in editions:
        if edition.date_deleted is not None:
            continue
        if edition.current_build_id is not None:
            protected.add(edition.current_build_id)
    for row in history:
        history_edition = editions_by_id.get(row.edition_id)
        if history_edition is None or history_edition.date_deleted is not None:
            continue
        if (
            history_edition.lifecycle_exempt
            or row.position < rule.min_position
        ):
            protected.add(row.build_id)
    return protected
