"""Domain re-exports of the lifecycle-rule schema.

The schema itself lives in ``docverse`` so external API
consumers get the typed models. This module is a thin shim so server-
side code can import the same types from a domain-prefixed path
without reaching across the package boundary.
"""

from __future__ import annotations

from typing import Final

from docverse.models.lifecycle import (
    BuildHistoryOrphanRule,
    DraftInactivityRule,
    LifecycleRule,
    LifecycleRuleSet,
    RefDeletedRule,
)

__all__ = [
    "REF_DELETED_RULE_TYPE",
    "BuildHistoryOrphanRule",
    "DraftInactivityRule",
    "LifecycleRule",
    "LifecycleRuleSet",
    "RefDeletedRule",
]

REF_DELETED_RULE_TYPE: Final = RefDeletedRule().type
"""The ``type`` discriminator of :class:`RefDeletedRule`.

The soft-delete paths that only ever apply the ``ref_deleted`` rule —
the ``delete`` webhook processor, the ``git_ref_audit`` worker (whose
evaluation is filtered to this rule), and the default-branch duplicate
retirer — record this as their keeper-sync tombstone note, the way the
paths that evaluate several rules record the matched rule's ``type``
(PRD #830).
"""
