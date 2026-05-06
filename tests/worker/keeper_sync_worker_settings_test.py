"""Tests for the dual-queue ``WorkerSettings`` shape in ``worker.main``."""

from __future__ import annotations

from docverse.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse.worker.functions import (
    keeper_sync_project,
    keeper_sync_run_discovery,
)
from docverse.worker.main import (
    KeeperSyncWorkerSettings,
    WorkerSettings,
    shutdown,
    startup,
)


def test_keeper_sync_worker_settings_uses_dedicated_queue() -> None:
    """The sync queue is isolated from the default arq queue."""
    assert KeeperSyncWorkerSettings.queue_name == KEEPER_SYNC_QUEUE_NAME
    assert KeeperSyncWorkerSettings.queue_name != WorkerSettings.queue_name


def test_keeper_sync_worker_settings_registers_keeper_sync_functions() -> None:
    """Both keeper_sync functions are registered on the dedicated queue."""
    assert keeper_sync_run_discovery in KeeperSyncWorkerSettings.functions
    assert keeper_sync_project in KeeperSyncWorkerSettings.functions


def test_default_worker_does_not_register_keeper_sync_functions() -> None:
    """The default queue stays free of keeper-sync work."""
    assert keeper_sync_run_discovery not in WorkerSettings.functions
    assert keeper_sync_project not in WorkerSettings.functions


def test_keeper_sync_worker_settings_share_lifecycle_hooks() -> None:
    """Both classes share startup/shutdown for one factory builder."""
    assert KeeperSyncWorkerSettings.on_startup is startup
    assert KeeperSyncWorkerSettings.on_shutdown is shutdown
    assert KeeperSyncWorkerSettings.on_startup is WorkerSettings.on_startup
    assert KeeperSyncWorkerSettings.on_shutdown is WorkerSettings.on_shutdown
