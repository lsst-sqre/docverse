"""Tests for the publish-edition CLI command."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import click
import httpx
import pytest
from click.testing import CliRunner

from docverse.cli import main


@pytest.fixture
def cf_env(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Set Cloudflare environment variables."""
    env = {
        "CLOUDFLARE_API_TOKEN": "test-token",
        "CLOUDFLARE_ACCOUNT_ID": "test-account-id",
        "CLOUDFLARE_KV_NAMESPACE_ID": "test-namespace-id",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return env


def _make_mock_build(
    storage_prefix: str = "myproject/__builds/ABC123/",
) -> MagicMock:
    """Create a mock Build domain object."""
    build = MagicMock()
    build.storage_prefix = storage_prefix
    return build


@patch("docverse.cli.httpx.AsyncClient")
@patch("docverse.cli._lookup_build")
def test_publish_edition_happy_path(
    mock_lookup: MagicMock,
    mock_httpx_cls: MagicMock,
    cf_env: dict[str, str],
) -> None:
    mock_build = _make_mock_build()
    mock_lookup.return_value = mock_build

    # Set up mock httpx client
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.put = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_httpx_cls.return_value = mock_client

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "publish-edition",
            "--org",
            "myorg",
            "--project",
            "myproject",
            "--edition",
            "main",
            "--build-id",
            "ABC123",
        ],
    )

    assert result.exit_code == 0, result.output

    # Verify DB lookup was called with correct args
    mock_lookup.assert_called_once_with(
        org_slug="myorg",
        project_slug="myproject",
        edition_slug="main",
        build_id="ABC123",
    )

    # Verify Cloudflare KV API was called correctly
    mock_client.put.assert_called_once()
    call_args = mock_client.put.call_args
    expected_url = (
        "https://api.cloudflare.com/client/v4/accounts/test-account-id"
        "/storage/kv/namespaces/test-namespace-id/values/myproject/main"
    )
    assert call_args[0][0] == expected_url

    # Verify authorization header
    assert call_args[1]["headers"]["Authorization"] == "Bearer test-token"

    # Verify the JSON value written
    kv_value = json.loads(call_args[1]["content"])
    assert kv_value == {
        "build_id": "ABC123",
        "r2_prefix": "myproject/__builds/ABC123/",
    }


@patch("docverse.cli.httpx.AsyncClient")
@patch("docverse.cli._lookup_build")
def test_publish_edition_api_failure(
    mock_lookup: MagicMock,
    mock_httpx_cls: MagicMock,
    cf_env: dict[str, str],
) -> None:
    mock_build = _make_mock_build()
    mock_lookup.return_value = mock_build

    # Set up mock httpx client that raises on status
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Server Error",
        request=MagicMock(),
        response=mock_response,
    )

    mock_client = AsyncMock()
    mock_client.put = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_httpx_cls.return_value = mock_client

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "publish-edition",
            "--org",
            "myorg",
            "--project",
            "myproject",
            "--edition",
            "main",
            "--build-id",
            "ABC123",
        ],
    )

    assert result.exit_code == 1
    assert "Cloudflare KV API error" in result.output


@patch("docverse.cli.httpx.AsyncClient")
@patch("docverse.cli._lookup_build")
def test_publish_edition_build_not_found(
    mock_lookup: MagicMock,
    mock_httpx_cls: MagicMock,
    cf_env: dict[str, str],
) -> None:
    mock_lookup.side_effect = click.ClickException(
        "Build 'NOTFOUND' not found"
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "publish-edition",
            "--org",
            "myorg",
            "--project",
            "myproject",
            "--edition",
            "main",
            "--build-id",
            "NOTFOUND",
        ],
    )

    assert result.exit_code == 1
    assert "Build 'NOTFOUND' not found" in result.output
    mock_httpx_cls.assert_not_called()


@patch("docverse.cli.httpx.AsyncClient")
@patch("docverse.cli._lookup_build")
def test_publish_edition_invalid_build_id(
    mock_lookup: MagicMock,
    mock_httpx_cls: MagicMock,
    cf_env: dict[str, str],
) -> None:
    mock_lookup.side_effect = click.ClickException(
        "Invalid build ID '!!INVALID!!': not a valid base32 string"
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "publish-edition",
            "--org",
            "myorg",
            "--project",
            "myproject",
            "--edition",
            "main",
            "--build-id",
            "!!INVALID!!",
        ],
    )

    assert result.exit_code == 1
    assert "Invalid build ID" in result.output
    mock_httpx_cls.assert_not_called()


@patch("docverse.cli.httpx.AsyncClient")
@patch("docverse.cli._lookup_build")
def test_publish_edition_edition_not_found(
    mock_lookup: MagicMock,
    mock_httpx_cls: MagicMock,
    cf_env: dict[str, str],
) -> None:
    mock_lookup.side_effect = click.ClickException(
        "Edition 'badslug' not found"
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "publish-edition",
            "--org",
            "myorg",
            "--project",
            "myproject",
            "--edition",
            "badslug",
            "--build-id",
            "ABC123",
        ],
    )

    assert result.exit_code == 1
    assert "Edition 'badslug' not found" in result.output
    mock_httpx_cls.assert_not_called()


@pytest.mark.parametrize(
    "missing_var",
    [
        "CLOUDFLARE_API_TOKEN",
        "CLOUDFLARE_ACCOUNT_ID",
        "CLOUDFLARE_KV_NAMESPACE_ID",
    ],
)
def test_publish_edition_missing_env_var(
    cf_env: dict[str, str],
    missing_var: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(missing_var)
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "publish-edition",
            "--org",
            "myorg",
            "--project",
            "myproject",
            "--edition",
            "main",
            "--build-id",
            "ABC123",
        ],
    )
    assert result.exit_code == 1
    assert missing_var in result.output
