"""Tests for the ``KeeperSyncConfig`` client model."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from docverse.models import (
    KeeperSyncConfig,
    KeeperSyncConfigUpdate,
    KeeperSyncRun,
    KeeperSyncScopePreview,
    KeeperSyncTombstone,
)


def test_default_is_disabled_with_default_url_and_empty_allowlist() -> None:
    config = KeeperSyncConfig()
    assert config.enabled is False
    assert str(config.ltd_base_url) == "https://keeper.lsst.codes/"
    assert config.project_slugs == []


def test_default_scope_fields_are_empty() -> None:
    """The three scope fields added by PRD #667 default to empty lists."""
    config = KeeperSyncConfig()
    assert config.project_slug_patterns == []
    assert config.exclude_project_slugs == []
    assert config.exclude_project_slug_patterns == []


def test_round_trips_explicit_allowlist() -> None:
    config = KeeperSyncConfig(
        enabled=True,
        ltd_base_url="https://keeper.example.com/",
        project_slugs=["alpha", "beta"],
    )
    dumped = config.model_dump(mode="json")
    restored = KeeperSyncConfig.model_validate(dumped)
    assert restored == config


def test_round_trips_wildcard() -> None:
    """``project_slugs="*"`` round-trips through ``model_dump``."""
    config = KeeperSyncConfig(
        enabled=True,
        ltd_base_url="https://keeper.example.com/",
        project_slugs="*",
    )
    dumped = config.model_dump(mode="json")
    assert dumped["project_slugs"] == "*"
    restored = KeeperSyncConfig.model_validate(dumped)
    assert restored.project_slugs == "*"


def test_rejects_unknown_string_token_for_project_slugs() -> None:
    """Only the literal ``"*"`` is accepted; ``"all"`` etc. is rejected."""
    with pytest.raises(ValidationError):
        KeeperSyncConfig(
            enabled=True,
            ltd_base_url="https://keeper.example.com/",
            project_slugs="ALL",  # type: ignore[arg-type]
        )


def test_rejects_invalid_url() -> None:
    with pytest.raises(ValidationError):
        KeeperSyncConfig(
            enabled=True,
            ltd_base_url="not-a-url",  # type: ignore[arg-type]
            project_slugs=[],
        )


@pytest.mark.parametrize(
    ("config_kwargs", "slug", "expected"),
    [
        # Explicit list membership.
        ({"project_slugs": ["sqr-112"]}, "sqr-112", True),
        ({"project_slugs": ["sqr-112"]}, "dmtn-001", False),
        # Empty config includes nothing.
        ({}, "sqr-112", False),
        # Include pattern.
        ({"project_slug_patterns": [r"sqr-\d+"]}, "sqr-112", True),
        ({"project_slug_patterns": [r"sqr-\d+"]}, "dmtn-001", False),
        # Wildcard includes everything.
        ({"project_slugs": "*"}, "anything", True),
        # Wildcard plus redundant include patterns is allowed.
        (
            {"project_slugs": "*", "project_slug_patterns": [r"sqr-\d+"]},
            "dmtn-001",
            True,
        ),
        # Excludes win over the wildcard...
        (
            {"project_slugs": "*", "exclude_project_slugs": ["www"]},
            "www",
            False,
        ),
        # ...over an explicit include...
        (
            {
                "project_slugs": ["sqr-112"],
                "exclude_project_slugs": ["sqr-112"],
            },
            "sqr-112",
            False,
        ),
        # ...and over a pattern include.
        (
            {
                "project_slug_patterns": [r"sqr-\d+"],
                "exclude_project_slugs": ["sqr-112"],
            },
            "sqr-112",
            False,
        ),
        # Exclude patterns win too.
        (
            {
                "project_slugs": "*",
                "exclude_project_slug_patterns": [r"test-.*"],
            },
            "test-thing",
            False,
        ),
        (
            {
                "project_slug_patterns": [r"sqr-\d+"],
                "exclude_project_slug_patterns": [r"sqr-9\d"],
            },
            "sqr-90",
            False,
        ),
        # ``fullmatch`` boundaries: ``sqr-1`` is not a prefix match.
        ({"project_slug_patterns": ["sqr-1"]}, "sqr-1", True),
        ({"project_slug_patterns": ["sqr-1"]}, "sqr-10", False),
        ({"project_slug_patterns": ["sqr-1"]}, "sqr-100", False),
        ({"project_slug_patterns": ["qr-1"]}, "sqr-1", False),
        # Matching is case-sensitive.
        ({"project_slug_patterns": [r"sqr-\d+"]}, "SQR-112", False),
        ({"project_slugs": ["sqr-112"]}, "SQR-112", False),
        (
            {"project_slugs": "*", "exclude_project_slugs": ["WWW"]},
            "www",
            True,
        ),
    ],
)
def test_is_in_scope_rule(
    *, config_kwargs: dict[str, object], slug: str, expected: bool
) -> None:
    """The scope rule: includes, then excludes, ``fullmatch``, case."""
    config = KeeperSyncConfig(**config_kwargs)  # type: ignore[arg-type]
    assert config.is_in_scope(slug) is expected


def test_filter_in_scope_preserves_input_order() -> None:
    """``filter_in_scope`` keeps the caller's (LTD listing) order."""
    config = KeeperSyncConfig(
        project_slugs=["zulu", "alpha"],
        project_slug_patterns=[r"sqr-\d+"],
        exclude_project_slugs=["sqr-999"],
    )
    given = ["sqr-999", "zulu", "dmtn-001", "sqr-1", "alpha", "sqr-2"]
    assert config.filter_in_scope(given) == ["zulu", "sqr-1", "alpha", "sqr-2"]


def test_filter_in_scope_wildcard_returns_every_slug() -> None:
    """``"*"`` with no excludes passes the whole listing through."""
    config = KeeperSyncConfig(project_slugs="*")
    given = ["b", "a", "c"]
    assert config.filter_in_scope(given) == given


def test_stored_config_without_scope_fields_loads_with_defaults() -> None:
    """A config persisted before PRD #667 loads with empty scope fields."""
    stored = {
        "enabled": True,
        "ltd_base_url": "https://keeper.lsst.codes/",
        "project_slugs": ["sqr-112"],
    }
    config = KeeperSyncConfig.model_validate(stored)
    assert config.project_slug_patterns == []
    assert config.exclude_project_slugs == []
    assert config.exclude_project_slug_patterns == []
    assert config.is_in_scope("sqr-112") is True
    assert config.is_in_scope("dmtn-001") is False


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_rejects_uncompilable_pattern(field: str) -> None:
    """A bad regex names both the field and the offending pattern."""
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncConfig.model_validate({field: ["sqr-("]})
    message = str(excinfo.value)
    assert field in message
    assert "sqr-(" in message


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_rejects_over_long_pattern(field: str) -> None:
    """A pattern longer than 256 characters is rejected."""
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncConfig.model_validate({field: ["a" * 257]})
    assert field in str(excinfo.value)


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_accepts_pattern_at_the_length_limit(field: str) -> None:
    """256 characters is the limit, not one short of it."""
    config = KeeperSyncConfig.model_validate({field: ["a" * 256]})
    assert getattr(config, field) == ["a" * 256]


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_rejects_more_than_100_patterns(field: str) -> None:
    """More than 100 entries in a pattern field is rejected."""
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncConfig.model_validate({field: [f"p{i}" for i in range(101)]})
    assert field in str(excinfo.value)


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_accepts_exactly_100_patterns(field: str) -> None:
    """100 entries is the limit, not one short of it."""
    patterns = [f"p{i}" for i in range(100)]
    config = KeeperSyncConfig.model_validate({field: patterns})
    assert getattr(config, field) == patterns


def test_config_update_all_fields_default_unset() -> None:
    """An empty update dumps to nothing under ``exclude_unset``."""
    update = KeeperSyncConfigUpdate()
    assert update.model_dump(exclude_unset=True) == {}


def test_config_update_omits_untouched_fields() -> None:
    """Only the provided field survives ``model_dump(exclude_unset=True)``."""
    update = KeeperSyncConfigUpdate(enabled=False)
    assert update.model_dump(exclude_unset=True) == {"enabled": False}


def test_config_update_project_slugs_replaces_wholesale() -> None:
    """``project_slugs`` carries the full replacement list, or ``"*"``."""
    update = KeeperSyncConfigUpdate(project_slugs=["alpha", "beta"])
    assert update.model_dump(exclude_unset=True) == {
        "project_slugs": ["alpha", "beta"]
    }
    wildcard = KeeperSyncConfigUpdate(project_slugs="*")
    assert wildcard.model_dump(exclude_unset=True) == {"project_slugs": "*"}


@pytest.mark.parametrize(
    "field",
    [
        "project_slug_patterns",
        "exclude_project_slugs",
        "exclude_project_slug_patterns",
    ],
)
def test_config_update_scope_fields_replace_wholesale(field: str) -> None:
    """Each new scope field carries its full replacement list."""
    update = KeeperSyncConfigUpdate.model_validate({field: ["alpha"]})
    assert update.model_dump(exclude_unset=True) == {field: ["alpha"]}


@pytest.mark.parametrize(
    "field",
    [
        "project_slug_patterns",
        "exclude_project_slugs",
        "exclude_project_slug_patterns",
    ],
)
def test_config_update_rejects_explicit_null_on_scope_fields(
    field: str,
) -> None:
    """An explicit ``null`` is rejected on each new scope field."""
    with pytest.raises(ValidationError):
        KeeperSyncConfigUpdate.model_validate({field: None})


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_config_update_rejects_uncompilable_pattern(field: str) -> None:
    """``PATCH`` shares the model's pattern validation with ``PUT``."""
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncConfigUpdate.model_validate({field: ["sqr-("]})
    message = str(excinfo.value)
    assert field in message
    assert "sqr-(" in message


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_config_update_rejects_over_long_pattern(field: str) -> None:
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncConfigUpdate.model_validate({field: ["a" * 257]})
    assert field in str(excinfo.value)


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_config_update_rejects_more_than_100_patterns(field: str) -> None:
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncConfigUpdate.model_validate(
            {field: [f"p{i}" for i in range(101)]}
        )
    assert field in str(excinfo.value)


def test_config_update_forbids_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        KeeperSyncConfigUpdate.model_validate({"unknown": True})


def test_config_update_rejects_unknown_string_token() -> None:
    """Only the literal ``"*"`` is accepted for ``project_slugs``."""
    with pytest.raises(ValidationError):
        KeeperSyncConfigUpdate(project_slugs="ALL")  # type: ignore[arg-type]


def test_keeper_sync_run_id_is_base32_string() -> None:
    """``KeeperSyncRun.id`` carries the Base32 public id as a string.

    Locks the API contract change from an integer primary key to the
    run's Base32 public identifier; the value round-trips unchanged
    through ``model_dump``/``model_validate``.
    """
    run = KeeperSyncRun(
        self_url="https://docverse.example/orgs/o/keeper-sync/runs/AAAA-BBBB",
        jobs_url=(
            "https://docverse.example/orgs/o/keeper-sync/runs/AAAA-BBBB/jobs"
        ),
        id="AAAA-BBBB-CCCC-05",
        kind="backfill",
        status="pending",
        pending_count=0,
        succeeded_count=0,
        failed_count=0,
        total_count=0,
        date_started=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert run.id == "AAAA-BBBB-CCCC-05"

    restored = KeeperSyncRun.model_validate(run.model_dump(mode="json"))
    assert restored.id == "AAAA-BBBB-CCCC-05"


def test_keeper_sync_tombstone_id_is_base32_string() -> None:
    """``KeeperSyncTombstone.id`` carries the Base32 public id as a string.

    Locks the API contract change from the raw ``keeper_sync_state``
    primary key (``state_id``) to the row's Base32 public identifier;
    the value round-trips unchanged through
    ``model_dump``/``model_validate``.
    """
    tombstone = KeeperSyncTombstone(
        self_url=(
            "https://docverse.example/orgs/o/keeper-sync/tombstones/"
            "AAAA-BBBB-CCCC-05"
        ),
        id="AAAA-BBBB-CCCC-05",
        resource_type="edition",
        ltd_slug="v1.0",
        ltd_id=42,
        date_tombstoned=datetime(2026, 1, 1, tzinfo=UTC),
        tombstone_reason="manual_delete",
        display_path="proj/v1.0",
    )
    assert tombstone.id == "AAAA-BBBB-CCCC-05"

    restored = KeeperSyncTombstone.model_validate(
        tombstone.model_dump(mode="json")
    )
    assert restored.id == "AAAA-BBBB-CCCC-05"


def test_extra_fields_forbidden() -> None:
    with pytest.raises(ValidationError):
        KeeperSyncConfig.model_validate(
            {
                "enabled": False,
                "ltd_base_url": "https://keeper.lsst.codes/",
                "project_slugs": [],
                "unknown": True,
            }
        )


def test_scope_preview_slug_lists_default_to_empty() -> None:
    """Only the two counts are required; every slug list defaults empty.

    An org whose scope resolves to nothing still gets a well-formed
    report rather than a response with missing keys.
    """
    preview = KeeperSyncScopePreview(ltd_count=3, in_scope_count=0)
    assert preview.in_scope_slugs == []
    assert preview.new_slugs == []
    assert preview.tombstoned_slugs == []
    assert preview.unmatched_project_slugs == []
