"""Tests for the dedicated metrics enums (SQR-112 D4/D7).

These enums are a published Avro contract, so the members are pinned
here by value rather than only through the workers that emit them: a
rename that a worker test would silently follow is a schema break for
every Sasquatch consumer, and this file is where that break shows up.
"""

from __future__ import annotations

from docverse.models import (
    BuildHistoryOrphanRule,
    DraftInactivityRule,
    RefDeletedRule,
)
from docverse_server.metrics import LifecycleActionTrigger, LifecycleReapAction


def test_purgatory_cleanup_is_a_lifecycle_action_trigger() -> None:
    """The nightly sweep is a third emitter of ``lifecycle_action``.

    ``trigger`` is what separates one worker's reaps from another's in
    the same event stream, so the sweep needs its own member rather than
    borrowing ``lifecycle_eval``'s.
    """
    assert LifecycleActionTrigger.purgatory_cleanup == "purgatory_cleanup"


def test_retention_expired_is_a_lifecycle_reap_action() -> None:
    """The sweep's reap reason is a member, not a lifecycle-rule type.

    Every other member mirrors a lifecycle-rule ``type`` discriminator,
    but no rule reclaims storage — the retention window on the
    organization does. ``from_rule_type`` therefore never produces this
    member; the sweep names it directly.
    """
    assert LifecycleReapAction.retention_expired == "retention_expired"
    rule_types = {
        DraftInactivityRule(max_days_inactive=30).type,
        BuildHistoryOrphanRule(min_position=1, min_age_days=30).type,
        RefDeletedRule().type,
    }
    assert LifecycleReapAction.retention_expired.value not in rule_types
