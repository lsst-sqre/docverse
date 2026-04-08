"""Integration test for the deploy-worker CLI command.

This test exercises the full pipeline: npm pack, tar unpack, and
wrangler deploy --dry-run. It requires Node.js and npm on PATH.
"""

from __future__ import annotations

import shutil
import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

from docverse.cli import main

WRANGLER_TOML = textwrap.dedent("""\
    # Mock deployments repo wrangler.toml
    # The main entry point references the unpacked worker source.
    name = "docverse-worker"
    main = "worker/src/index.ts"
    compatibility_date = "2025-04-01"
    compatibility_flags = ["nodejs_compat"]

    [env.dev]
    name = "docverse-worker-dev"
    kv_namespaces = [
      { binding = "EDITIONS_KV", id = "dev-kv-namespace-id" }
    ]

    [[env.dev.r2_buckets]]
    binding = "BUILDS_R2"
    bucket_name = "docverse-dev-builds"

    [env.dev.vars]
    URL_SCHEME = "subdomain"

    [env.production]
    name = "docverse-worker"
    kv_namespaces = [
      { binding = "EDITIONS_KV", id = "prod-kv-namespace-id" }
    ]

    [[env.production.r2_buckets]]
    binding = "BUILDS_R2"
    bucket_name = "docverse-builds"

    [env.production.vars]
    URL_SCHEME = "subdomain"
""")

PACKAGE_JSON = textwrap.dedent("""\
    {
      "name": "docverse-cloudflare-deployments",
      "version": "0.0.0",
      "private": true
    }
""")


def _repo_root() -> Path:
    """Return the monorepo root (parent of this test file's repo)."""
    return Path(__file__).resolve().parent.parent.parent


@pytest.fixture
def deployments_repo(tmp_path: Path) -> Path:
    """Create a mock deployments repo with environment-layered config."""
    repo = tmp_path / "deployments"
    repo.mkdir()
    (repo / "wrangler.toml").write_text(WRANGLER_TOML)
    (repo / "package.json").write_text(PACKAGE_JSON)

    # Copy wrangler from the monorepo's cloudflare-worker install.
    # In CI the deploy-worker-test job runs ``npm ci`` before pytest;
    # locally, run ``npm ci`` in cloudflare-worker/ first.
    worker_dir = _repo_root() / "cloudflare-worker"
    node_modules = worker_dir / "node_modules"
    if not node_modules.is_dir():
        pytest.skip(
            "cloudflare-worker/node_modules not found "
            "— run 'npm ci' in cloudflare-worker/ first"
        )
    shutil.copytree(node_modules, repo / "node_modules", symlinks=True)
    return repo


def test_deploy_worker_dry_run_integration(
    deployments_repo: Path,
) -> None:
    """Full pipeline: npm pack -> unpack -> wrangler deploy --dry-run."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "deploy-worker",
            "--docverse-repo",
            str(_repo_root()),
            "--deployments-repo",
            str(deployments_repo),
            "--env",
            "dev",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output

    # Verify the worker source was unpacked
    worker_dir = deployments_repo / "worker"
    assert worker_dir.is_dir()
    assert (worker_dir / "src" / "index.ts").is_file()
    assert (worker_dir / "package.json").is_file()

    # Verify the tarball was cleaned up
    assert not list(worker_dir.glob("*.tgz"))

    # Verify dry-run output directory was created by wrangler
    dist_dir = deployments_repo / "dist"
    assert dist_dir.is_dir()
