"""Tests for ``docverse.services.lifecycle.evaluator``.

Pure-function tests with no DB or HTTP. The evaluator is the highest-
value unit-testable surface of the ``lifecycle_eval`` background job
because the per-org worker, the dispatcher, and the reaper all
delegate the actual policy decisions to it. Locking the policy in
unit tests means a future change to the per-rule semantics has to
update both the rule and the test row that exercises it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from docverse.client.models import EditionKind, TrackingMode
from docverse.client.models.builds import BuildStatus
from docverse.domain.base32id import Base32Id
from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.edition_build_history import EditionBuildHistory
from docverse.domain.lifecycle import (
    BuildHistoryOrphanRule,
    DraftInactivityRule,
    LifecycleRuleSet,
    RefDeletedRule,
)
from docverse.services.lifecycle import (
    LifecycleEvaluationContext,
    evaluate_lifecycle,
    filter_rule_set,
    resolve_rule_set,
)

# A frozen "now" used across the tests. Chosen so subtracting whole-
# day offsets from it stays inside February 2026 and avoids
# month-boundary noise in the assertions.
NOW = datetime(2026, 2, 15, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _edition(
    *,
    edition_id: int,
    kind: EditionKind = EditionKind.draft,
    date_updated: datetime = NOW,
    lifecycle_exempt: bool = False,
    date_deleted: datetime | None = None,
    current_build_id: int | None = None,
    tracking_mode: TrackingMode = TrackingMode.git_ref,
    tracking_params: dict[str, str] | None = None,
) -> Edition:
    """Construct an ``Edition`` with sane defaults for evaluator tests."""
    return Edition(
        id=edition_id,
        slug=f"edition-{edition_id}",
        title=f"Edition {edition_id}",
        project_id=1,
        kind=kind,
        tracking_mode=tracking_mode,
        tracking_params=tracking_params,
        alternate_name=None,
        current_build_id=current_build_id,
        current_build_public_id=None,
        current_build_git_ref=None,
        lifecycle_exempt=lifecycle_exempt,
        publish_status=None,
        date_created=date_updated,
        date_updated=date_updated,
        date_deleted=date_deleted,
    )


def _build(
    *,
    build_id: int,
    date_completed: datetime | None = None,
    date_created: datetime | None = None,
    date_deleted: datetime | None = None,
) -> Build:
    """Construct a ``Build`` with sane defaults for evaluator tests."""
    return Build(
        id=build_id,
        public_id=Base32Id(build_id),
        project_id=1,
        git_ref="main",
        alternate_name=None,
        content_hash="sha256:" + "0" * 64,
        status=BuildStatus.completed,
        staging_key="staging/key",
        storage_prefix="proj/__builds/abc/",
        object_count=None,
        total_size_bytes=None,
        uploader="alice",
        annotations=None,
        date_created=date_created or NOW,
        date_uploaded=None,
        date_completed=date_completed,
        date_deleted=date_deleted,
    )


def _history(
    *,
    history_id: int,
    edition_id: int,
    build_id: int,
    position: int,
    date_created: datetime = NOW,
) -> EditionBuildHistory:
    """Construct an ``EditionBuildHistory`` row."""
    return EditionBuildHistory(
        id=history_id,
        edition_id=edition_id,
        build_id=build_id,
        position=position,
        publish_status=None,
        date_created=date_created,
    )


def _context(
    *,
    editions: list[Edition] | None = None,
    builds: list[Build] | None = None,
    edition_build_history: list[EditionBuildHistory] | None = None,
    now: datetime = NOW,
    live_refs: frozenset[str] | None = None,
) -> LifecycleEvaluationContext:
    """Construct a ``LifecycleEvaluationContext`` with sane defaults."""
    return LifecycleEvaluationContext(
        editions=editions or [],
        builds=builds or [],
        edition_build_history=edition_build_history or [],
        now=now,
        live_refs=live_refs,
    )


# ---------------------------------------------------------------------------
# Empty / no-op
# ---------------------------------------------------------------------------


def test_empty_rule_set_returns_empty_decision() -> None:
    """An empty rule set matches nothing and reports no counts."""
    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(root=[]),
        context=_context(
            editions=[
                _edition(edition_id=1, date_updated=NOW - timedelta(days=365))
            ],
        ),
    )

    assert decision.edition_ids == frozenset()
    assert decision.build_ids == frozenset()
    assert decision.rule_match_counts == {}


# ---------------------------------------------------------------------------
# draft_inactivity
# ---------------------------------------------------------------------------


def test_draft_inactivity_matches_stale_draft() -> None:
    """A draft edition older than ``max_days_inactive`` is matched."""
    stale = _edition(
        edition_id=10,
        kind=EditionKind.draft,
        date_updated=NOW - timedelta(days=45),
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[DraftInactivityRule(max_days_inactive=30)]
        ),
        context=_context(editions=[stale]),
    )

    assert decision.edition_ids == frozenset({10})
    assert decision.rule_match_counts == {"draft_inactivity": 1}


@pytest.mark.parametrize(
    "kind",
    [
        EditionKind.main,
        EditionKind.release,
        EditionKind.major,
        EditionKind.minor,
        EditionKind.alternate,
    ],
)
def test_draft_inactivity_only_matches_kind_draft(
    kind: EditionKind,
) -> None:
    """Only ``kind=draft`` editions are eligible — other kinds never match."""
    edition = _edition(
        edition_id=20,
        kind=kind,
        date_updated=NOW - timedelta(days=365),
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[DraftInactivityRule(max_days_inactive=30)]
        ),
        context=_context(editions=[edition]),
    )

    assert decision.edition_ids == frozenset()
    assert decision.rule_match_counts == {"draft_inactivity": 0}


def test_draft_inactivity_skips_lifecycle_exempt() -> None:
    """``lifecycle_exempt`` editions are never candidates."""
    exempt = _edition(
        edition_id=30,
        kind=EditionKind.draft,
        date_updated=NOW - timedelta(days=365),
        lifecycle_exempt=True,
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[DraftInactivityRule(max_days_inactive=30)]
        ),
        context=_context(editions=[exempt]),
    )

    assert decision.edition_ids == frozenset()


def test_draft_inactivity_skips_already_soft_deleted() -> None:
    """An already-soft-deleted draft is not re-matched."""
    deleted = _edition(
        edition_id=40,
        kind=EditionKind.draft,
        date_updated=NOW - timedelta(days=365),
        date_deleted=NOW - timedelta(days=1),
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[DraftInactivityRule(max_days_inactive=30)]
        ),
        context=_context(editions=[deleted]),
    )

    assert decision.edition_ids == frozenset()


def test_draft_inactivity_excludes_exact_boundary() -> None:
    """A draft updated exactly ``max_days_inactive`` ago is not matched.

    The rule uses a strict less-than against ``now - max_days_inactive``
    so an edition exactly on the boundary survives. The very next
    microsecond older would match — see
    :func:`test_draft_inactivity_matches_just_past_boundary`.
    """
    on_boundary = _edition(
        edition_id=50,
        kind=EditionKind.draft,
        date_updated=NOW - timedelta(days=30),
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[DraftInactivityRule(max_days_inactive=30)]
        ),
        context=_context(editions=[on_boundary]),
    )

    assert decision.edition_ids == frozenset()


def test_draft_inactivity_matches_just_past_boundary() -> None:
    """One microsecond older than the boundary does match."""
    past = _edition(
        edition_id=51,
        kind=EditionKind.draft,
        date_updated=NOW - timedelta(days=30) - timedelta(microseconds=1),
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[DraftInactivityRule(max_days_inactive=30)]
        ),
        context=_context(editions=[past]),
    )

    assert decision.edition_ids == frozenset({51})


# ---------------------------------------------------------------------------
# build_history_orphan
# ---------------------------------------------------------------------------


def test_build_history_orphan_keeps_current_build() -> None:
    """A build that is ``current_build_id`` of an edition is protected."""
    current_build = _build(
        build_id=100,
        date_completed=NOW - timedelta(days=365),
    )
    edition = _edition(
        edition_id=60,
        kind=EditionKind.release,
        current_build_id=100,
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[BuildHistoryOrphanRule(min_position=5, min_age_days=30)]
        ),
        context=_context(editions=[edition], builds=[current_build]),
    )

    assert decision.build_ids == frozenset()
    assert decision.rule_match_counts == {"build_history_orphan": 0}


def test_build_history_orphan_keeps_in_window_position() -> None:
    """A build at a history position < ``min_position`` is protected."""
    build = _build(
        build_id=110,
        date_completed=NOW - timedelta(days=365),
    )
    edition = _edition(edition_id=70, kind=EditionKind.release)
    history_row = _history(
        history_id=200, edition_id=70, build_id=110, position=2
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[BuildHistoryOrphanRule(min_position=5, min_age_days=30)]
        ),
        context=_context(
            editions=[edition],
            builds=[build],
            edition_build_history=[history_row],
        ),
    )

    assert decision.build_ids == frozenset()


def test_build_history_orphan_keeps_shared_when_one_edition_in_window() -> (
    None
):
    """Two editions reference the same build; one keeps it in window."""
    build = _build(
        build_id=120,
        date_completed=NOW - timedelta(days=365),
    )
    edition_old = _edition(edition_id=80, kind=EditionKind.release)
    edition_new = _edition(edition_id=81, kind=EditionKind.release)
    rows = [
        # Position 10 in the older edition's history (out of window).
        _history(
            history_id=300,
            edition_id=80,
            build_id=120,
            position=10,
        ),
        # Position 1 in the other edition's history (in window).
        _history(
            history_id=301,
            edition_id=81,
            build_id=120,
            position=1,
        ),
    ]

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[BuildHistoryOrphanRule(min_position=5, min_age_days=30)]
        ),
        context=_context(
            editions=[edition_old, edition_new],
            builds=[build],
            edition_build_history=rows,
        ),
    )

    assert decision.build_ids == frozenset()


def test_build_history_orphan_matches_orphan_past_position_and_age() -> None:
    """A build past ``min_position`` everywhere and old enough is matched."""
    build = _build(
        build_id=130,
        date_completed=NOW - timedelta(days=365),
    )
    edition = _edition(edition_id=90, kind=EditionKind.release)
    history_row = _history(
        history_id=400, edition_id=90, build_id=130, position=10
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[BuildHistoryOrphanRule(min_position=5, min_age_days=30)]
        ),
        context=_context(
            editions=[edition],
            builds=[build],
            edition_build_history=[history_row],
        ),
    )

    assert decision.build_ids == frozenset({130})
    assert decision.rule_match_counts == {"build_history_orphan": 1}


def test_build_history_orphan_age_threshold_protects_recent_build() -> None:
    """Recent builds past ``min_position`` are not yet eligible.

    A build past ``min_position`` but younger than ``min_age_days``
    survives until the age threshold catches up.
    """
    young_build = _build(
        build_id=140,
        date_completed=NOW - timedelta(days=5),
    )
    edition = _edition(edition_id=100, kind=EditionKind.release)
    history_row = _history(
        history_id=500, edition_id=100, build_id=140, position=10
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[BuildHistoryOrphanRule(min_position=5, min_age_days=30)]
        ),
        context=_context(
            editions=[edition],
            builds=[young_build],
            edition_build_history=[history_row],
        ),
    )

    assert decision.build_ids == frozenset()


def test_build_history_orphan_uses_date_created_when_completed_missing() -> (
    None
):
    """Fall back to ``date_created`` when ``date_completed`` is null.

    ``date_completed`` may be null on never-finished builds; the age
    threshold still needs a value to compare against, so
    ``date_created`` is the documented fallback.
    """
    build = _build(
        build_id=150,
        date_completed=None,
        date_created=NOW - timedelta(days=365),
    )
    edition = _edition(edition_id=110, kind=EditionKind.release)
    history_row = _history(
        history_id=600, edition_id=110, build_id=150, position=10
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[BuildHistoryOrphanRule(min_position=5, min_age_days=30)]
        ),
        context=_context(
            editions=[edition],
            builds=[build],
            edition_build_history=[history_row],
        ),
    )

    assert decision.build_ids == frozenset({150})


def test_build_history_orphan_exempt_edition_protects_referenced_builds() -> (
    None
):
    """An exempt edition exempts every build it references."""
    build_current = _build(
        build_id=160,
        date_completed=NOW - timedelta(days=365),
    )
    build_history = _build(
        build_id=161,
        date_completed=NOW - timedelta(days=365),
    )
    exempt = _edition(
        edition_id=120,
        kind=EditionKind.release,
        lifecycle_exempt=True,
        current_build_id=160,
    )
    history_row = _history(
        history_id=700,
        edition_id=120,
        build_id=161,
        position=10,
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[BuildHistoryOrphanRule(min_position=5, min_age_days=30)]
        ),
        context=_context(
            editions=[exempt],
            builds=[build_current, build_history],
            edition_build_history=[history_row],
        ),
    )

    assert decision.build_ids == frozenset()


def test_build_history_orphan_ignores_history_from_deleted_editions() -> None:
    """A soft-deleted edition no longer protects the builds it references."""
    build = _build(
        build_id=170,
        date_completed=NOW - timedelta(days=365),
    )
    deleted_edition = _edition(
        edition_id=130,
        kind=EditionKind.draft,
        date_deleted=NOW - timedelta(days=1),
    )
    history_row = _history(
        history_id=800,
        edition_id=130,
        build_id=170,
        position=1,
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[BuildHistoryOrphanRule(min_position=5, min_age_days=30)]
        ),
        context=_context(
            editions=[deleted_edition],
            builds=[build],
            edition_build_history=[history_row],
        ),
    )

    assert decision.build_ids == frozenset({170})


def test_build_history_orphan_skips_already_deleted_build() -> None:
    """An already-soft-deleted build is not re-emitted as a candidate."""
    build = _build(
        build_id=180,
        date_completed=NOW - timedelta(days=365),
        date_deleted=NOW - timedelta(days=1),
    )
    edition = _edition(edition_id=140, kind=EditionKind.release)
    history_row = _history(
        history_id=900, edition_id=140, build_id=180, position=10
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[BuildHistoryOrphanRule(min_position=5, min_age_days=30)]
        ),
        context=_context(
            editions=[edition],
            builds=[build],
            edition_build_history=[history_row],
        ),
    )

    assert decision.build_ids == frozenset()


# ---------------------------------------------------------------------------
# ref_deleted
# ---------------------------------------------------------------------------


def test_ref_deleted_matches_draft_tracking_deleted_branch() -> None:
    """A draft edition whose tracked branch is missing from ``live_refs``."""
    deleted_branch = _edition(
        edition_id=200,
        kind=EditionKind.draft,
        tracking_mode=TrackingMode.git_ref,
        tracking_params={"git_ref": "tickets/DM-12345"},
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(root=[RefDeletedRule()]),
        context=_context(
            editions=[deleted_branch],
            live_refs=frozenset({"main"}),
        ),
    )

    assert decision.edition_ids == frozenset({200})
    assert decision.rule_match_counts == {"ref_deleted": 1}


def test_ref_deleted_skips_draft_tracking_live_branch() -> None:
    """A draft tracking a branch still present in ``live_refs`` survives."""
    live = _edition(
        edition_id=210,
        kind=EditionKind.draft,
        tracking_mode=TrackingMode.git_ref,
        tracking_params={"git_ref": "tickets/DM-12345"},
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(root=[RefDeletedRule()]),
        context=_context(
            editions=[live],
            live_refs=frozenset({"main", "tickets/DM-12345"}),
        ),
    )

    assert decision.edition_ids == frozenset()
    assert decision.rule_match_counts == {"ref_deleted": 0}


def test_ref_deleted_skips_lifecycle_exempt_edition() -> None:
    """``lifecycle_exempt=True`` editions survive even when ref is gone."""
    exempt = _edition(
        edition_id=220,
        kind=EditionKind.draft,
        tracking_mode=TrackingMode.git_ref,
        tracking_params={"git_ref": "tickets/DM-12345"},
        lifecycle_exempt=True,
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(root=[RefDeletedRule()]),
        context=_context(
            editions=[exempt],
            live_refs=frozenset({"main"}),
        ),
    )

    assert decision.edition_ids == frozenset()


def test_ref_deleted_skips_release_edition_pinned_to_deleted_tag() -> None:
    """A non-draft (release) edition pinned to a deleted tag is not matched.

    SQR-112 user story 9: release editions pinned to tags must not
    be surprise-deleted if an upstream pipeline force-recreates the
    tag, so the candidate filter requires ``kind == draft``.
    """
    release = _edition(
        edition_id=230,
        kind=EditionKind.release,
        tracking_mode=TrackingMode.git_ref,
        tracking_params={"git_ref": "v1.0.0"},
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(root=[RefDeletedRule()]),
        context=_context(
            editions=[release],
            live_refs=frozenset({"main"}),
        ),
    )

    assert decision.edition_ids == frozenset()


def test_ref_deleted_matches_alternate_git_ref_draft() -> None:
    """A draft on ``alternate_git_ref`` is matched on the same rules."""
    alt = _edition(
        edition_id=240,
        kind=EditionKind.draft,
        tracking_mode=TrackingMode.alternate_git_ref,
        tracking_params={"git_ref": "feature/x"},
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(root=[RefDeletedRule()]),
        context=_context(
            editions=[alt],
            live_refs=frozenset({"main"}),
        ),
    )

    assert decision.edition_ids == frozenset({240})


def test_ref_deleted_returns_empty_when_live_refs_is_none() -> None:
    """``live_refs=None`` means "no ref info" — the branch matches nothing.

    The audit / sync paths populate ``live_refs`` from a GitHub fetch.
    A ``None`` value means the fetch did not happen (e.g. non-GitHub
    project, or a #337 caller that has not been updated yet); the
    correct semantic is to skip the branch rather than soft-delete
    every draft edition.
    """
    draft = _edition(
        edition_id=250,
        kind=EditionKind.draft,
        tracking_mode=TrackingMode.git_ref,
        tracking_params={"git_ref": "tickets/DM-12345"},
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(root=[RefDeletedRule()]),
        context=_context(editions=[draft], live_refs=None),
    )

    assert decision.edition_ids == frozenset()
    assert decision.rule_match_counts == {"ref_deleted": 0}


def test_ref_deleted_skips_edition_with_no_git_ref_tracking_param() -> None:
    """A literal-ref draft with no ``tracking_params['git_ref']`` is skipped.

    Defensive: even though every draft created via the slug-derivation
    path has ``git_ref`` populated, the evaluator must not raise on a
    malformed row.
    """
    bare = _edition(
        edition_id=260,
        kind=EditionKind.draft,
        tracking_mode=TrackingMode.git_ref,
        tracking_params=None,
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(root=[RefDeletedRule()]),
        context=_context(
            editions=[bare],
            live_refs=frozenset({"main"}),
        ),
    )

    assert decision.edition_ids == frozenset()


def test_ref_deleted_skips_already_soft_deleted_edition() -> None:
    """An already-soft-deleted draft is not re-emitted as a candidate."""
    deleted = _edition(
        edition_id=270,
        kind=EditionKind.draft,
        tracking_mode=TrackingMode.git_ref,
        tracking_params={"git_ref": "tickets/DM-12345"},
        date_deleted=NOW - timedelta(days=1),
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(root=[RefDeletedRule()]),
        context=_context(
            editions=[deleted],
            live_refs=frozenset({"main"}),
        ),
    )

    assert decision.edition_ids == frozenset()


def test_ref_deleted_skips_non_literal_tracking_mode() -> None:
    """Computed tracking modes (e.g. ``lsst_doc``) are not eligible."""
    computed = _edition(
        edition_id=280,
        kind=EditionKind.draft,
        tracking_mode=TrackingMode.lsst_doc,
        tracking_params={"git_ref": "main"},
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(root=[RefDeletedRule()]),
        context=_context(
            editions=[computed],
            live_refs=frozenset({"other"}),
        ),
    )

    assert decision.edition_ids == frozenset()


def test_ref_deleted_with_empty_live_refs_matches_all_eligible_drafts() -> (
    None
):
    """An empty ``live_refs`` (repo with no branches/tags) still matches.

    Empty-set semantics: every tracked ref is, by definition, missing
    from ``live_refs``. Differentiating an empty fetch (repo exists
    but has no refs) from a missing fetch (``live_refs=None``) is the
    whole point of the nullable field.
    """
    draft = _edition(
        edition_id=290,
        kind=EditionKind.draft,
        tracking_mode=TrackingMode.git_ref,
        tracking_params={"git_ref": "tickets/DM-12345"},
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(root=[RefDeletedRule()]),
        context=_context(editions=[draft], live_refs=frozenset()),
    )

    assert decision.edition_ids == frozenset({290})


# ---------------------------------------------------------------------------
# rule combination
# ---------------------------------------------------------------------------


def test_multiple_rules_each_report_their_own_counts() -> None:
    """A rule set carrying two rule types reports a count per rule."""
    stale_draft = _edition(
        edition_id=160,
        kind=EditionKind.draft,
        date_updated=NOW - timedelta(days=365),
    )
    orphan_build = _build(
        build_id=190,
        date_completed=NOW - timedelta(days=365),
    )
    holder_edition = _edition(edition_id=161, kind=EditionKind.release)
    orphan_row = _history(
        history_id=1000,
        edition_id=161,
        build_id=190,
        position=10,
    )

    decision = evaluate_lifecycle(
        rule_set=LifecycleRuleSet(
            root=[
                DraftInactivityRule(max_days_inactive=30),
                BuildHistoryOrphanRule(min_position=5, min_age_days=30),
            ]
        ),
        context=_context(
            editions=[stale_draft, holder_edition],
            builds=[orphan_build],
            edition_build_history=[orphan_row],
        ),
    )

    assert decision.edition_ids == frozenset({160})
    assert decision.build_ids == frozenset({190})
    assert decision.rule_match_counts == {
        "draft_inactivity": 1,
        "build_history_orphan": 1,
    }


# ---------------------------------------------------------------------------
# resolve_rule_set
# ---------------------------------------------------------------------------


def test_resolve_uses_project_rules_when_set() -> None:
    """A project rule set replaces the org rule set."""
    org = LifecycleRuleSet(root=[DraftInactivityRule(max_days_inactive=7)])
    project = LifecycleRuleSet(
        root=[DraftInactivityRule(max_days_inactive=90)]
    )

    resolved = resolve_rule_set(org_rules=org, project_rules=project)

    assert resolved is project


def test_resolve_falls_back_to_org_when_project_unset() -> None:
    """``project_rules=None`` inherits the org rule set."""
    org = LifecycleRuleSet(root=[DraftInactivityRule(max_days_inactive=7)])

    resolved = resolve_rule_set(org_rules=org, project_rules=None)

    assert resolved is org


def test_resolve_returns_empty_when_both_unset() -> None:
    """No rules anywhere produces an empty rule set."""
    resolved = resolve_rule_set(org_rules=None, project_rules=None)

    assert resolved.root == []


def test_resolve_project_empty_list_overrides_org() -> None:
    """An explicit empty project rule set is the project opt-out.

    User story 3: project admins must be able to disable org-level
    lifecycle policy without negotiating org defaults. Passing
    ``project_rules=LifecycleRuleSet(root=[])`` is the on-the-wire
    representation of that opt-out, so the resolver must keep the
    empty set rather than fall through to the org rules.
    """
    org = LifecycleRuleSet(root=[DraftInactivityRule(max_days_inactive=7)])
    project = LifecycleRuleSet(root=[])

    resolved = resolve_rule_set(org_rules=org, project_rules=project)

    assert resolved.root == []


def test_resolve_does_not_merge() -> None:
    """Resolution replaces — it never unions org and project rules."""
    org = LifecycleRuleSet(root=[DraftInactivityRule(max_days_inactive=7)])
    project = LifecycleRuleSet(
        root=[BuildHistoryOrphanRule(min_position=5, min_age_days=30)]
    )

    resolved = resolve_rule_set(org_rules=org, project_rules=project)

    assert [rule.type for rule in resolved.root] == ["build_history_orphan"]


# ---------------------------------------------------------------------------
# filter_rule_set
# ---------------------------------------------------------------------------


def test_filter_rule_set_keeps_only_ref_deleted() -> None:
    """``include=(RefDeletedRule,)`` keeps exactly the ref-deleted rule."""
    rule_set = LifecycleRuleSet(
        root=[
            DraftInactivityRule(max_days_inactive=30),
            BuildHistoryOrphanRule(min_position=5, min_age_days=30),
            RefDeletedRule(),
        ]
    )

    filtered = filter_rule_set(rule_set, include=(RefDeletedRule,))

    assert [rule.type for rule in filtered.root] == ["ref_deleted"]


def test_filter_rule_set_keeps_lifecycle_eval_rule_kinds() -> None:
    """``include`` with the two lifecycle_eval kinds drops ref_deleted."""
    rule_set = LifecycleRuleSet(
        root=[
            DraftInactivityRule(max_days_inactive=30),
            BuildHistoryOrphanRule(min_position=5, min_age_days=30),
            RefDeletedRule(),
        ]
    )

    filtered = filter_rule_set(
        rule_set, include=(DraftInactivityRule, BuildHistoryOrphanRule)
    )

    assert [rule.type for rule in filtered.root] == [
        "draft_inactivity",
        "build_history_orphan",
    ]


def test_filter_rule_set_empty_match_returns_empty_set() -> None:
    """A rule set with none of the included kinds filters to empty."""
    rule_set = LifecycleRuleSet(
        root=[DraftInactivityRule(max_days_inactive=30)]
    )

    filtered = filter_rule_set(rule_set, include=(RefDeletedRule,))

    assert filtered.root == []
