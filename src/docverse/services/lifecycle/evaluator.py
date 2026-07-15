"""Pure evaluator for lifecycle rules."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from docverse.client.models import EditionKind, TrackingMode
from pydantic import BaseModel, ConfigDict, Field

from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.edition_build_history import EditionBuildHistory
from docverse.domain.lifecycle import (
    BuildHistoryOrphanRule,
    DraftInactivityRule,
    LifecycleRule,
    LifecycleRuleSet,
    RefDeletedRule,
)

__all__ = [
    "LifecycleDecision",
    "LifecycleEvaluationContext",
    "evaluate_lifecycle",
    "filter_rule_set",
    "resolve_rule_set",
]


@dataclass(frozen=True, kw_only=True, slots=True)
class LifecycleEvaluationContext:
    """Bundled inputs to :func:`evaluate_lifecycle`.

    Groups every per-project piece of state the evaluator reads so a
    new context field (e.g. ``live_refs`` for the ``ref_deleted``
    branch) can be added without changing the evaluator's public
    signature.

    ``live_refs`` is the set of bare ref names (branch + tag) that
    currently exist on the project's GitHub repository, or ``None``
    when no ref information is available (non-GitHub project, fetch
    not yet performed). ``None`` is structurally distinct from an
    empty set: ``frozenset()`` means "the repo exists but has no
    branches or tags right now" (every tracked ref is a candidate),
    while ``None`` means "we have no ref information" (the
    ``ref_deleted`` branch returns no matches).

    Namespace contract for ``live_refs`` (an inter-slice contract the
    future populators must honor — the webhook fast-path and the
    ``git_ref_audit`` cron land in later slices):

    - Names are **bare**: ``main``, ``v1.0``, not ``refs/heads/main``
      or ``refs/tags/v1.0``. The ``ref_deleted`` branch compares
      ``edition.tracking_params["git_ref"]`` (itself bare) against
      this set, so a populator that forgets to strip the
      ``refs/heads/`` / ``refs/tags/`` prefix would make every tracked
      ref look deleted.
    - Branches and tags are **flattened into one set**. A deleted
      branch that is shadowed by a same-named tag (or vice versa)
      therefore survives the ``ref_deleted`` branch. This is an
      accepted edge case, not a bug: collisions between branch and
      tag names are rare and keeping one set avoids threading a
      ref-kind discriminator through the evaluator.
    """

    editions: Sequence[Edition]
    builds: Sequence[Build]
    edition_build_history: Sequence[EditionBuildHistory]
    now: datetime
    live_refs: frozenset[str] | None = None


class LifecycleDecision(BaseModel):
    """Decision returned by :func:`evaluate_lifecycle`.

    ``edition_matches`` and ``build_matches`` map each matched entity
    id to the ``type`` discriminator of the rule that matched it, so
    the per-org worker can attribute each soft-delete to the right
    rule (and emit it in the audit-trail log) without re-deriving the
    mapping from entity type. ``edition_ids`` / ``build_ids`` expose
    the same id sets as frozensets for callers that only need the
    soft-delete candidates. ``rule_match_counts`` maps each rule
    ``type`` present in the rule set to the number of entities that
    rule matched.
    """

    model_config = ConfigDict(frozen=True)

    edition_matches: Mapping[int, str] = Field(
        default_factory=dict,
        description=(
            "Edition ids to soft-delete, keyed on the ``type``"
            " discriminator of the rule that matched them."
        ),
    )

    build_matches: Mapping[int, str] = Field(
        default_factory=dict,
        description=(
            "Build ids to soft-delete, keyed on the ``type``"
            " discriminator of the rule that matched them."
        ),
    )

    rule_match_counts: Mapping[str, int] = Field(
        default_factory=dict,
        description="Per-rule match count keyed on the rule's ``type``.",
    )

    @property
    def edition_ids(self) -> frozenset[int]:
        """Edition ids to soft-delete (derived from ``edition_matches``)."""
        return frozenset(self.edition_matches)

    @property
    def build_ids(self) -> frozenset[int]:
        """Build ids to soft-delete (derived from ``build_matches``)."""
        return frozenset(self.build_matches)


def evaluate_lifecycle(
    *,
    rule_set: LifecycleRuleSet,
    context: LifecycleEvaluationContext,
) -> LifecycleDecision:
    """Evaluate ``rule_set`` against the project state in ``context``.

    No database access. The caller hands in a
    :class:`LifecycleEvaluationContext` carrying already-loaded
    editions, builds, per-edition build-history rows, and the current
    ``live_refs`` set (when applicable); the function returns a
    :class:`LifecycleDecision` listing the entities the rule set has
    matched for soft-delete, each tagged with the matching rule's
    ``type`` discriminator.
    """
    editions_list = list(context.editions)
    builds_list = list(context.builds)
    history_list = list(context.edition_build_history)

    edition_matches: dict[int, str] = {}
    build_matches: dict[int, str] = {}
    rule_match_counts: dict[str, int] = {}

    for rule in rule_set.root:
        match rule:
            case DraftInactivityRule():
                draft_matches = _eval_draft_inactivity(
                    rule=rule, editions=editions_list, now=context.now
                )
                for edition_id in draft_matches:
                    edition_matches.setdefault(edition_id, rule.type)
                rule_match_counts[rule.type] = len(draft_matches)
            case BuildHistoryOrphanRule():
                orphan_matches = _eval_build_history_orphan(
                    rule=rule,
                    editions=editions_list,
                    builds=builds_list,
                    history=history_list,
                    now=context.now,
                )
                for build_id in orphan_matches:
                    build_matches.setdefault(build_id, rule.type)
                rule_match_counts[rule.type] = len(orphan_matches)
            case RefDeletedRule():
                ref_matches = _eval_ref_deleted(
                    editions=editions_list, live_refs=context.live_refs
                )
                for edition_id in ref_matches:
                    edition_matches.setdefault(edition_id, rule.type)
                rule_match_counts[rule.type] = len(ref_matches)
            case _:
                msg = f"unknown lifecycle rule type {rule.type!r}"
                raise RuntimeError(msg)

    return LifecycleDecision(
        edition_matches=edition_matches,
        build_matches=build_matches,
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


def filter_rule_set(
    rule_set: LifecycleRuleSet,
    *,
    include: tuple[type[LifecycleRule], ...],
) -> LifecycleRuleSet:
    """Return a copy of rule_set keeping only rules of the included kinds.

    Each lifecycle worker owns a specific subset of rule kinds (the
    git_ref_audit cron owns RefDeletedRule; lifecycle_eval owns
    DraftInactivityRule and BuildHistoryOrphanRule). Filtering before
    evaluate_lifecycle makes that ownership explicit and structural, so
    a rule kind a worker does not own can never fire from its code path.

    Filtering an already-valid set produces no duplicate ``type``s, so
    the :class:`LifecycleRuleSet` duplicate-type validator is satisfied.
    """
    return LifecycleRuleSet(
        root=[rule for rule in rule_set.root if isinstance(rule, include)]
    )


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
        if edition.kind != EditionKind.draft:
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


_REF_DELETED_TRACKING_MODES = frozenset(
    {TrackingMode.git_ref, TrackingMode.alternate_git_ref}
)


def _eval_ref_deleted(
    *,
    editions: list[Edition],
    live_refs: frozenset[str] | None,
) -> set[int]:
    """Return edition ids that match ``RefDeletedRule``.

    ``live_refs=None`` means no ref information was supplied for the
    project (non-GitHub source, or a caller that has not been
    migrated to provide the set). In that case the branch matches
    nothing — soft-deleting every draft on missing information would
    be the wrong default. An empty ``frozenset()`` is distinct: it
    represents a repo whose branch and tag sets are both currently
    empty, and every literal-ref draft becomes a candidate.
    """
    if live_refs is None:
        return set()
    matched: set[int] = set()
    for edition in editions:
        if edition.kind != EditionKind.draft:
            continue
        if edition.tracking_mode not in _REF_DELETED_TRACKING_MODES:
            continue
        if edition.lifecycle_exempt:
            continue
        if edition.date_deleted is not None:
            continue
        params = edition.tracking_params
        if not params:
            continue
        git_ref = params.get("git_ref")
        if not git_ref:
            continue
        if git_ref in live_refs:
            continue
        matched.add(edition.id)
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
