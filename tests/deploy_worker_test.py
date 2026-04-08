"""Tests for the deploy-worker CLI command."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from click.testing import CliRunner

from docverse.cli import main


@pytest.fixture
def mock_monorepo(tmp_path: Path) -> Path:
    """Create a mock monorepo with a cloudflare-worker directory."""
    monorepo = tmp_path / "monorepo"
    worker_dir = monorepo / "cloudflare-worker"
    worker_dir.mkdir(parents=True)
    (worker_dir / "package.json").write_text(
        '{"name": "docverse-worker", "version": "0.0.0"}'
    )
    return monorepo


@pytest.fixture
def mock_deployments_repo(tmp_path: Path) -> Path:
    """Create a mock deployments repo with environment-layered config."""
    repo = tmp_path / "deployments"
    repo.mkdir()
    (repo / "wrangler.toml").write_text(
        "[env.dev]\n"
        'name = "docverse-worker-dev"\n'
        "kv_namespaces = [\n"
        '  { binding = "EDITIONS_KV",'
        ' id = "dev-kv-namespace-id" }\n'
        "]\n"
        "\n"
        "[[env.dev.r2_buckets]]\n"
        'binding = "BUILDS_R2"\n'
        'bucket_name = "docverse-dev-builds"\n'
        "\n"
        "[env.production]\n"
        'name = "docverse-worker"\n'
        "kv_namespaces = [\n"
        '  { binding = "EDITIONS_KV",'
        ' id = "prod-kv-namespace-id" }\n'
        "]\n"
        "\n"
        "[[env.production.r2_buckets]]\n"
        'binding = "BUILDS_R2"\n'
        'bucket_name = "docverse-builds"\n'
    )
    return repo


def _npm_pack_side_effect(*args: object, **kwargs: object) -> MagicMock:
    cmd = args[0]
    assert isinstance(cmd, list)
    if cmd == ["npm", "pack"]:
        result = MagicMock()
        result.stdout = "docverse-worker-0.0.0.tgz\n"
        return result
    return MagicMock()


@pytest.mark.parametrize(
    "bad_env",
    ["--config=evil", "foo bar", "dev;rm", "dev\nstaging", ""],
)
def test_deploy_worker_invalid_env_name(
    bad_env: str,
    mock_monorepo: Path,
    mock_deployments_repo: Path,
) -> None:
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "deploy-worker",
            "--docverse-repo",
            str(mock_monorepo),
            "--deployments-repo",
            str(mock_deployments_repo),
            "--env",
            bad_env,
        ],
    )
    assert result.exit_code == 1
    assert "Invalid environment name" in result.output


@patch("docverse.cli.shutil.copy2")
@patch("docverse.cli.subprocess.run")
def test_deploy_worker_happy_path(
    mock_run: MagicMock,
    mock_copy: MagicMock,
    mock_monorepo: Path,
    mock_deployments_repo: Path,
) -> None:
    mock_run.side_effect = _npm_pack_side_effect
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "deploy-worker",
            "--docverse-repo",
            str(mock_monorepo),
            "--deployments-repo",
            str(mock_deployments_repo),
            "--env",
            "dev",
        ],
    )

    assert result.exit_code == 0, result.output

    worker_dir = mock_monorepo.resolve() / "cloudflare-worker"
    deployments = mock_deployments_repo.resolve()
    dest_dir = deployments / "worker"

    assert mock_run.call_count == 3
    mock_run.assert_has_calls(
        [
            call(
                ["npm", "pack"],
                check=True,
                capture_output=True,
                text=True,
                cwd=str(worker_dir),
            ),
            call(
                [
                    "tar",
                    "xzf",
                    "docverse-worker-0.0.0.tgz",
                    "--strip-components=1",
                ],
                check=True,
                cwd=str(dest_dir),
            ),
            call(
                ["npx", "wrangler", "deploy", "--env", "dev"],
                check=True,
                cwd=str(deployments),
            ),
        ]
    )

    mock_copy.assert_called_once_with(
        worker_dir / "docverse-worker-0.0.0.tgz",
        dest_dir / "docverse-worker-0.0.0.tgz",
    )


@patch("docverse.cli.shutil.copy2")
@patch("docverse.cli.subprocess.run")
def test_deploy_worker_npm_pack_fails(
    mock_run: MagicMock,
    mock_copy: MagicMock,
    mock_monorepo: Path,
    mock_deployments_repo: Path,
) -> None:
    mock_run.side_effect = subprocess.CalledProcessError(
        1, "npm pack", stderr="npm ERR! something"
    )
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "deploy-worker",
            "--docverse-repo",
            str(mock_monorepo),
            "--deployments-repo",
            str(mock_deployments_repo),
            "--env",
            "dev",
        ],
    )

    assert result.exit_code == 1
    assert "npm pack failed" in result.output


@patch("docverse.cli.shutil.copy2")
@patch("docverse.cli.subprocess.run")
def test_deploy_worker_tar_extract_fails(
    mock_run: MagicMock,
    mock_copy: MagicMock,
    mock_monorepo: Path,
    mock_deployments_repo: Path,
) -> None:
    call_count = 0

    def side_effect(*args: object, **kwargs: object) -> MagicMock:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return _npm_pack_side_effect(*args)
        raise subprocess.CalledProcessError(1, "tar")

    mock_run.side_effect = side_effect
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "deploy-worker",
            "--docverse-repo",
            str(mock_monorepo),
            "--deployments-repo",
            str(mock_deployments_repo),
            "--env",
            "dev",
        ],
    )

    assert result.exit_code == 1
    assert "Failed to unpack worker tarball" in result.output


@patch("docverse.cli.shutil.copy2")
@patch("docverse.cli.subprocess.run")
def test_deploy_worker_wrangler_deploy_fails(
    mock_run: MagicMock,
    mock_copy: MagicMock,
    mock_monorepo: Path,
    mock_deployments_repo: Path,
) -> None:
    call_count = 0

    def side_effect(*args: object, **kwargs: object) -> MagicMock:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return _npm_pack_side_effect(*args)
        raise subprocess.CalledProcessError(1, "wrangler")

    mock_run.side_effect = side_effect
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "deploy-worker",
            "--docverse-repo",
            str(mock_monorepo),
            "--deployments-repo",
            str(mock_deployments_repo),
            "--env",
            "dev",
        ],
    )

    assert result.exit_code == 1
    assert "wrangler deploy failed" in result.output


def test_deploy_worker_missing_cloudflare_worker_dir(
    tmp_path: Path,
    mock_deployments_repo: Path,
) -> None:
    empty_monorepo = tmp_path / "empty-monorepo"
    empty_monorepo.mkdir()
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "deploy-worker",
            "--docverse-repo",
            str(empty_monorepo),
            "--deployments-repo",
            str(mock_deployments_repo),
            "--env",
            "dev",
        ],
    )

    assert result.exit_code == 1
    assert "cloudflare-worker/ not found" in result.output


@patch("docverse.cli.shutil.copy2")
@patch("docverse.cli.subprocess.run")
def test_deploy_worker_creates_worker_dest_dir(
    mock_run: MagicMock,
    mock_copy: MagicMock,
    mock_monorepo: Path,
    mock_deployments_repo: Path,
) -> None:
    mock_run.side_effect = _npm_pack_side_effect
    dest_dir = mock_deployments_repo / "worker"
    assert not dest_dir.exists()

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "deploy-worker",
            "--docverse-repo",
            str(mock_monorepo),
            "--deployments-repo",
            str(mock_deployments_repo),
            "--env",
            "dev",
        ],
    )

    assert result.exit_code == 0, result.output
    assert dest_dir.exists()


@pytest.mark.parametrize("env_name", ["dev", "production"])
@patch("docverse.cli.shutil.copy2")
@patch("docverse.cli.subprocess.run")
def test_deploy_worker_env_passed_to_wrangler(
    mock_run: MagicMock,
    mock_copy: MagicMock,
    mock_monorepo: Path,
    mock_deployments_repo: Path,
    env_name: str,
) -> None:
    mock_run.side_effect = _npm_pack_side_effect
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "deploy-worker",
            "--docverse-repo",
            str(mock_monorepo),
            "--deployments-repo",
            str(mock_deployments_repo),
            "--env",
            env_name,
        ],
    )

    assert result.exit_code == 0, result.output

    wrangler_call = mock_run.call_args_list[2]
    assert wrangler_call == call(
        ["npx", "wrangler", "deploy", "--env", env_name],
        check=True,
        cwd=str(mock_deployments_repo.resolve()),
    )


@patch("docverse.cli.shutil.copy2")
@patch("docverse.cli.subprocess.run")
def test_deploy_worker_dry_run(
    mock_run: MagicMock,
    mock_copy: MagicMock,
    mock_monorepo: Path,
    mock_deployments_repo: Path,
) -> None:
    mock_run.side_effect = _npm_pack_side_effect
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "deploy-worker",
            "--docverse-repo",
            str(mock_monorepo),
            "--deployments-repo",
            str(mock_deployments_repo),
            "--env",
            "dev",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output

    deployments = mock_deployments_repo.resolve()
    outdir = deployments / "dist"
    wrangler_call = mock_run.call_args_list[2]
    assert wrangler_call == call(
        [
            "npx",
            "wrangler",
            "deploy",
            "--env",
            "dev",
            "--dry-run",
            f"--outdir={outdir}",
        ],
        check=True,
        cwd=str(deployments),
    )
