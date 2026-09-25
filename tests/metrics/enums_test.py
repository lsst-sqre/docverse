"""Tests for the dedicated metrics enums (SQR-112 D4/D7).

These enums are a published Avro contract, so the members are pinned
here by value rather than only through the workers that emit them: a
rename that a worker test would silently follow is a schema break for
every Sasquatch consumer, and this file is where that break shows up.
"""

from __future__ import annotations

import pytest

from docverse.models import (
    BuildHistoryOrphanRule,
    DraftInactivityRule,
    RefDeletedRule,
)
from docverse_server.metrics import (
    HttpStatusClass,
    LifecycleActionTrigger,
    LifecycleReapAction,
)


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


def test_http_status_class_values() -> None:
    """The status classes are pinned by the value dashboards group on.

    ``status_class`` is an InfluxDB tag on ``api_request``, so its values
    are what a query's ``GROUP BY`` and ``WHERE`` clauses quote; the
    RFC 9110 class shorthand (``4xx``) is the vocabulary an operator
    already uses for them.
    """
    assert [member.value for member in HttpStatusClass] == [
        "1xx",
        "2xx",
        "3xx",
        "4xx",
        "5xx",
    ]


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (100, HttpStatusClass.informational),
        (200, HttpStatusClass.successful),
        (204, HttpStatusClass.successful),
        (304, HttpStatusClass.redirection),
        (404, HttpStatusClass.client_error),
        (499, HttpStatusClass.client_error),
        (500, HttpStatusClass.server_error),
        (599, HttpStatusClass.server_error),
    ],
)
def test_http_status_class_from_status_code(
    status_code: int, expected: HttpStatusClass
) -> None:
    """A status code maps to its class by its hundreds digit."""
    assert HttpStatusClass.from_status_code(status_code) is expected


@pytest.mark.parametrize("status_code", [0, 99, 600])
def test_http_status_class_rejects_codes_outside_rfc_range(
    status_code: int,
) -> None:
    """A code outside ``100``-``599`` has no class and is refused."""
    with pytest.raises(ValueError, match=str(status_code)):
        HttpStatusClass.from_status_code(status_code)
