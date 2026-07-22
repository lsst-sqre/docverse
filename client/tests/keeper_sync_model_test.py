"""Tests for the ``KeeperSyncConfig`` client model."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from docverse.client.models import (
    KeeperSyncConfig,
    KeeperSyncConfigUpdate,
    KeeperSyncRun,
    KeeperSyncTombstone,
)


def test_default_is_disabled_with_default_url_and_empty_allowlist() -> None:
    config = KeeperSyncConfig()
    assert config.enabled is False
    assert str(config.ltd_base_url) == "https://keeper.lsst.codes/"
    assert config.project_slugs == []


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
