"""Lifecycle-rule evaluation for the ``lifecycle_eval`` background job.

The evaluator in :mod:`docverse_server.services.lifecycle.evaluator` is the
single highest-value unit-testable surface of the lifecycle subsystem:
it decides which editions and builds should be soft-deleted given a
resolved rule set and pre-fetched project state. DM-54914's LTD-sync
path reuses the same function with proactively-fetched data, so the
evaluator API is shaped as a pure function over typed inputs.

The rule schema lives in :mod:`docverse_server.domain.lifecycle` (a re-export
of the canonical ``docverse`` models).
"""

from __future__ import annotations

from .evaluator import (
    LifecycleDecision,
    LifecycleEvaluationContext,
    evaluate_lifecycle,
    filter_rule_set,
    resolve_rule_set,
)

__all__ = [
    "LifecycleDecision",
    "LifecycleEvaluationContext",
    "evaluate_lifecycle",
    "filter_rule_set",
    "resolve_rule_set",
]
