"""Tests for the docverse upload CLI command."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import respx
from click.testing import CliRunner

from docverse.client._cli import main
from docverse.client.models.queue_enums import JobStatus

BASE_URL = "https://docverse.example.com"
TOKEN = "test-token"
ORG = "testorg"
PROJECT = "testproj"

# Valid Crockford Base32 IDs with checksum
BUILD_ID = "000000000195"
JOB_ID = "000000000292"


def _build_response(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "self_url": f"/orgs/{ORG}/projects/{PROJECT}/builds/{BUILD_ID}",
        "project_url": f"/orgs/{ORG}/projects/{PROJECT}",
        "id": BUILD_ID,
        "git_ref": "abc123",
        "alternate_name": None,
        "content_hash": "sha256:" + "a" * 64,
        "status": "pending",
        "upload_url": "https://storage.example.com/presigned-put",
        "queue_url": None,
        "object_count": None,
        "total_size_bytes": None,
        "uploader": "ci-bot",
        "annotations": None,
        "date_created": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        "date_uploaded": None,
        "date_completed": None,
    }
    data.update(overrides)
    return data


def _job_response(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "self_url": "/queue/jobs/" + JOB_ID,
        "id": JOB_ID,
        "kind": "build_processing",
        "status": "completed",
        "phase": None,
        "progress": None,
        "errors": None,
        "date_created": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        "date_started": None,
        "date_completed": None,
    }
    data.update(overrides)
    return data


def _setup_routes(
    router: respx.Router,
    *,
    build_overrides: dict[str, Any] | None = None,
    complete_overrides: dict[str, Any] | None = None,
    job_overrides: dict[str, Any] | None = None,
) -> None:
    """Set up standard respx routes for a full upload flow."""
    build_url = f"/orgs/{ORG}/projects/{PROJECT}/builds"
    build_self = f"/orgs/{ORG}/projects/{PROJECT}/builds/{BUILD_ID}"
    queue_url = "/queue/jobs/" + JOB_ID

    router.post(build_url).mock(
        return_value=httpx.Response(
            201, json=_build_response(**(build_overrides or {}))
        )
    )
    router.put("https://storage.example.com/presigned-put").mock(
        return_value=httpx.Response(200)
    )
    complete_data = {
        "status": "uploaded",
        "queue_url": queue_url,
        **(complete_overrides or {}),
    }
    router.patch(build_self).mock(
        return_value=httpx.Response(200, json=_build_response(**complete_data))
    )
    if job_overrides is not None or complete_overrides is None:
        router.get(queue_url).mock(
            return_value=httpx.Response(
                200, json=_job_response(**(job_overrides or {}))
            )
        )


def _invoke(
    source_dir: Path,
    extra_args: list[str] | None = None,
) -> Any:
    """Invoke the upload command with standard options."""
    runner = CliRunner()
    args = [
        "upload",
        "--org",
        ORG,
        "--project",
        PROJECT,
        "--git-ref",
        "abc123",
        "--dir",
        str(source_dir),
        "--token",
        TOKEN,
        "--base-url",
        BASE_URL,
    ]
    if extra_args:
        args.extend(extra_args)
    return runner.invoke(main, args, catch_exceptions=False)


def test_upload_success(tmp_path: Path) -> None:
    """Full happy path: exit 0, output contains completion message."""
    source = tmp_path / "docs"
    source.mkdir()
    (source / "index.html").write_text("<h1>Hello</h1>")

    with respx.mock() as router:
        _setup_routes(router, job_overrides={"status": "completed"})
        with patch("docverse.client._client.asyncio.sleep"):
            result = _invoke(source)

    assert result.exit_code == 0
    assert "Build processing complete" in result.output


def test_upload_no_wait(tmp_path: Path) -> None:
    """With --no-wait: exit 0, no job polling."""
    source = tmp_path / "docs"
    source.mkdir()
    (source / "index.html").write_text("<h1>Hello</h1>")

    with respx.mock(assert_all_called=False) as router:
        _setup_routes(router)
        result = _invoke(source, extra_args=["--no-wait"])

    assert result.exit_code == 0
    assert "Upload complete" in result.output


def test_upload_completed_with_errors(tmp_path: Path) -> None:
    """Job returns completed_with_errors: exit code 2."""
    source = tmp_path / "docs"
    source.mkdir()
    (source / "index.html").write_text("<h1>Hello</h1>")

    with respx.mock() as router:
        _setup_routes(
            router,
            job_overrides={
                "status": JobStatus.completed_with_errors,
                "phase": "editions",
            },
        )
        with patch("docverse.client._client.asyncio.sleep"):
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "upload",
                    "--org",
                    ORG,
                    "--project",
                    PROJECT,
                    "--git-ref",
                    "abc123",
                    "--dir",
                    str(source),
                    "--token",
                    TOKEN,
                    "--base-url",
                    BASE_URL,
                ],
            )

    assert result.exit_code == 2  # noqa: PLR2004


def test_upload_failed(tmp_path: Path) -> None:
    """Job returns failed: exit code 1 (ClickException)."""
    source = tmp_path / "docs"
    source.mkdir()
    (source / "index.html").write_text("<h1>Hello</h1>")

    with respx.mock() as router:
        _setup_routes(
            router,
            job_overrides={"status": "failed", "phase": "inventory"},
        )
        with patch("docverse.client._client.asyncio.sleep"):
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "upload",
                    "--org",
                    ORG,
                    "--project",
                    PROJECT,
                    "--git-ref",
                    "abc123",
                    "--dir",
                    str(source),
                    "--token",
                    TOKEN,
                    "--base-url",
                    BASE_URL,
                ],
            )

    assert result.exit_code == 1
    assert "failed" in result.output.lower()


def test_upload_git_ref_detection(tmp_path: Path) -> None:
    """Without --git-ref, uses _detect_git_ref value."""
    source = tmp_path / "docs"
    source.mkdir()
    (source / "index.html").write_text("<h1>Hello</h1>")

    detected_sha = "deadbeef" * 5

    with respx.mock() as router:
        _setup_routes(router, job_overrides={"status": "completed"})

        with (
            patch(
                "docverse.client._cli._detect_git_ref",
                return_value=detected_sha,
            ),
            patch("docverse.client._client.asyncio.sleep"),
        ):
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "upload",
                    "--org",
                    ORG,
                    "--project",
                    PROJECT,
                    "--dir",
                    str(source),
                    "--token",
                    TOKEN,
                    "--base-url",
                    BASE_URL,
                ],
                catch_exceptions=False,
            )

    assert result.exit_code == 0
    # The detected SHA should appear in the output
    assert detected_sha in result.output
