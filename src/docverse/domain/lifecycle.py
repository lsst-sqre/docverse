"""Domain re-exports of the lifecycle-rule schema.

The schema itself lives in ``docverse-client`` so external API
consumers get the typed models. This module is a thin shim so server-
side code can import the same types from a domain-prefixed path
without reaching across the package boundary.
"""

from __future__ import annotations

from docverse.client.models.lifecycle import (
    BuildHistoryOrphanRule,
    DraftInactivityRule,
    LifecycleRule,
    LifecycleRuleSet,
    RefDeletedRule,
)

__all__ = [
    "BuildHistoryOrphanRule",
    "DraftInactivityRule",
    "LifecycleRule",
    "LifecycleRuleSet",
    "RefDeletedRule",
]
