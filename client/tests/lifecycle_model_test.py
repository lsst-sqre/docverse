"""Tests for the lifecycle-rule client models."""

from __future__ import annotations

import pytest
from pydantic import TypeAdapter, ValidationError

from docverse.client.models import (
    BuildHistoryOrphanRule,
    DraftInactivityRule,
    LifecycleRuleSet,
    ProjectCreate,
    RefDeletedRule,
)
from docverse.client.models.lifecycle import LifecycleRule

_lifecycle_rule_adapter: TypeAdapter[object] = TypeAdapter(LifecycleRule)


def test_draft_inactivity_rule_validates_minimum() -> None:
    rule = DraftInactivityRule(max_days_inactive=30)
    assert rule.type == "draft_inactivity"
    assert rule.max_days_inactive == 30


def test_draft_inactivity_rule_rejects_non_positive_days() -> None:
    with pytest.raises(ValidationError):
        DraftInactivityRule(max_days_inactive=0)


def test_build_history_orphan_rule_validates() -> None:
    rule = BuildHistoryOrphanRule(min_position=5, min_age_days=30)
    assert rule.type == "build_history_orphan"
    assert rule.min_position == 5
    assert rule.min_age_days == 30


def test_build_history_orphan_rule_rejects_non_positive_position() -> None:
    with pytest.raises(ValidationError):
        BuildHistoryOrphanRule(min_position=0, min_age_days=30)


def test_build_history_orphan_rule_rejects_negative_age() -> None:
    with pytest.raises(ValidationError):
        BuildHistoryOrphanRule(min_position=1, min_age_days=-1)


def test_ref_deleted_rule_has_no_parameters() -> None:
    rule = RefDeletedRule()
    assert rule.type == "ref_deleted"


def test_ref_deleted_rule_rejects_extra_field() -> None:
    with pytest.raises(ValidationError):
        RefDeletedRule(enabled=True)  # type: ignore[call-arg]


def test_lifecycle_rule_discriminator_routes_to_draft_inactivity() -> None:
    payload = {"type": "draft_inactivity", "max_days_inactive": 30}
    rule = _lifecycle_rule_adapter.validate_python(payload)
    assert isinstance(rule, DraftInactivityRule)


def test_lifecycle_rule_discriminator_routes_to_build_history_orphan() -> None:
    payload = {
        "type": "build_history_orphan",
        "min_position": 5,
        "min_age_days": 30,
    }
    rule = _lifecycle_rule_adapter.validate_python(payload)
    assert isinstance(rule, BuildHistoryOrphanRule)


def test_lifecycle_rule_discriminator_routes_to_ref_deleted() -> None:
    payload = {"type": "ref_deleted"}
    rule = _lifecycle_rule_adapter.validate_python(payload)
    assert isinstance(rule, RefDeletedRule)


def test_lifecycle_rule_rejects_unknown_type() -> None:
    with pytest.raises(ValidationError) as exc_info:
        _lifecycle_rule_adapter.validate_python(
            {"type": "purgatory_eviction", "enabled": True}
        )
    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["type"] == "union_tag_invalid"
    assert errors[0]["input"] == {
        "type": "purgatory_eviction",
        "enabled": True,
    }


def test_lifecycle_rule_rejects_missing_discriminator() -> None:
    with pytest.raises(ValidationError) as exc_info:
        _lifecycle_rule_adapter.validate_python({"max_days_inactive": 30})
    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["type"] == "union_tag_not_found"


def test_lifecycle_rule_rejects_missing_required_field() -> None:
    with pytest.raises(ValidationError):
        _lifecycle_rule_adapter.validate_python({"type": "draft_inactivity"})


def test_lifecycle_rule_rejects_extra_field() -> None:
    with pytest.raises(ValidationError):
        _lifecycle_rule_adapter.validate_python(
            {
                "type": "draft_inactivity",
                "max_days_inactive": 30,
                "extra": "nope",
            }
        )


def test_lifecycle_rule_set_accepts_multiple_distinct_types() -> None:
    payload = [
        {"type": "draft_inactivity", "max_days_inactive": 30},
        {
            "type": "build_history_orphan",
            "min_position": 5,
            "min_age_days": 30,
        },
        {"type": "ref_deleted"},
    ]
    rule_set = LifecycleRuleSet.model_validate(payload)
    assert len(rule_set.root) == 3


def test_lifecycle_rule_set_rejects_duplicate_types() -> None:
    payload = [
        {"type": "draft_inactivity", "max_days_inactive": 30},
        {"type": "draft_inactivity", "max_days_inactive": 60},
    ]
    with pytest.raises(ValidationError) as exc_info:
        LifecycleRuleSet.model_validate(payload)
    assert "duplicate" in str(exc_info.value).lower()


def test_lifecycle_rule_set_accepts_empty_list() -> None:
    rule_set = LifecycleRuleSet.model_validate([])
    assert rule_set.root == []


def test_lifecycle_rule_set_dumps_to_json_friendly_list() -> None:
    payload = [
        {"type": "draft_inactivity", "max_days_inactive": 30},
        {
            "type": "build_history_orphan",
            "min_position": 5,
            "min_age_days": 30,
        },
    ]
    rule_set = LifecycleRuleSet.model_validate(payload)
    dumped = rule_set.model_dump(mode="json")
    assert dumped == payload


def test_project_create_accepts_typed_lifecycle_rules() -> None:
    payload = ProjectCreate(
        slug="pipelines",
        title="Pipelines",
        source_url="https://example.com/example/pipelines",
        lifecycle_rules=[  # type: ignore[arg-type]
            {"type": "draft_inactivity", "max_days_inactive": 30},
            {"type": "ref_deleted"},
        ],
    )
    assert isinstance(payload.lifecycle_rules, LifecycleRuleSet)
    assert len(payload.lifecycle_rules.root) == 2


def test_project_create_rejects_unknown_lifecycle_rule_type() -> None:
    with pytest.raises(ValidationError):
        ProjectCreate(
            slug="pipelines",
            title="Pipelines",
            source_url="https://example.com/example/pipelines",
            lifecycle_rules=[  # type: ignore[arg-type]
                {"type": "purgatory_eviction", "enabled": True},
            ],
        )


def test_project_create_rejects_duplicate_lifecycle_rule_types() -> None:
    with pytest.raises(ValidationError):
        ProjectCreate(
            slug="pipelines",
            title="Pipelines",
            source_url="https://example.com/example/pipelines",
            lifecycle_rules=[  # type: ignore[arg-type]
                {"type": "draft_inactivity", "max_days_inactive": 30},
                {"type": "draft_inactivity", "max_days_inactive": 60},
            ],
        )
