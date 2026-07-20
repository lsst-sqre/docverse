"""Tests for the ``KeeperSyncConfig`` client model."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from docverse.client.models import KeeperSyncConfig, KeeperSyncRun


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
