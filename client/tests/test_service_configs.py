"""Tests for service configuration client models."""

from __future__ import annotations

from docverse.client.models.services import CloudflareWorkersConfig


def test_cloudflare_workers_config_with_kv_namespace() -> None:
    config = CloudflareWorkersConfig(
        account_id="acct",
        zone_id="zone",
        kv_namespace_id="kv-ns-abc",
    )
    assert config.kv_namespace_id == "kv-ns-abc"


def test_cloudflare_workers_config_without_kv_namespace() -> None:
    """Existing JSONB rows without kv_namespace_id still parse."""
    config = CloudflareWorkersConfig.model_validate(
        {"provider": "cloudflare_workers", "account_id": "a", "zone_id": "z"}
    )
    assert config.kv_namespace_id is None
