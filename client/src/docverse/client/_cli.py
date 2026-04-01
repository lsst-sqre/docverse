"""Command-line interface for the Docverse client."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path

import click

from ._annotations import detect_github_actions_annotations, merge_annotations
from ._client import DocverseClient
from ._exceptions import BuildProcessingError, DocverseClientError
from ._tar import create_tarball
from .models.queue_enums import JobStatus

__all__ = ["main"]


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(message="%(version)s")
def main() -> None:
    """Docverse documentation hosting client."""


@main.command()
@click.option(
    "--org",
    envvar="DOCVERSE_ORG",
    show_envvar=True,
    required=True,
    help="Organization slug.",
)
@click.option(
    "--project",
    envvar="DOCVERSE_PROJECT",
    show_envvar=True,
    required=True,
    help="Project slug.",
)
@click.option(
    "--git-ref",
    envvar="DOCVERSE_GIT_REF",
    show_envvar=True,
    default=None,
    help="Git ref for the build. Defaults to HEAD.",
)
@click.option(
    "--dir",
    "source_dir",
    envvar="DOCVERSE_DIR",
    show_envvar=True,
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Directory containing the built documentation.",
)
@click.option(
    "--token",
    envvar="DOCVERSE_TOKEN",
    show_envvar=True,
    required=True,
    help="API bearer token.",
)
@click.option(
    "--base-url",
    envvar="DOCVERSE_API",
    show_envvar=True,
    default="https://roundtable.lsst.cloud/docverse/api",
    show_default=True,
    help="Docverse API base URL.",
)
@click.option(
    "--alternate-name",
    envvar="DOCVERSE_ALTERNATE",
    show_envvar=True,
    default=None,
    help="Alternate deployment name.",
)
@click.option(
    "--annotation",
    "-a",
    "annotations",
    multiple=True,
    help="Manual annotation in KEY=VALUE format. Can be repeated.",
)
@click.option(
    "--auto-annotations/--no-auto-annotations",
    default=True,
    show_default=True,
    help="Auto-detect annotations from CI environment variables.",
)
@click.option(
    "--no-wait",
    is_flag=True,
    default=False,
    help="Exit after upload without waiting for processing.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show detailed HTTP request/response information.",
)
def upload(  # noqa: PLR0913
    org: str,
    project: str,
    git_ref: str | None,
    source_dir: Path,
    token: str,
    base_url: str,
    alternate_name: str | None,
    annotations: tuple[str, ...],
    auto_annotations: bool,  # noqa: FBT001
    no_wait: bool,  # noqa: FBT001
    verbose: bool,  # noqa: FBT001
) -> None:
    """Upload a documentation build."""
    if git_ref is None:
        git_ref = _detect_git_ref()

    # Parse manual annotations
    manual: dict[str, str] | None = None
    if annotations:
        manual = {}
        for item in annotations:
            if "=" not in item:
                msg = f"Invalid annotation format (expected KEY=VALUE): {item}"
                raise click.BadParameter(msg, param_hint="'--annotation'")
            key, value = item.split("=", 1)
            manual[key] = value

    asyncio.run(
        _upload_async(
            org=org,
            project=project,
            git_ref=git_ref,
            source_dir=source_dir,
            token=token,
            base_url=base_url,
            alternate_name=alternate_name,
            auto_annotations=auto_annotations,
            manual_annotations=manual,
            no_wait=no_wait,
            verbose=verbose,
        )
    )


def _detect_git_ref() -> str:
    """Detect the current git ref from HEAD."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],  # noqa: S607
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        msg = (
            "Could not detect git ref from HEAD. "
            "Provide --git-ref or set DOCVERSE_GIT_REF."
        )
        raise click.UsageError(msg) from exc
    return result.stdout.strip()


async def _upload_async(  # noqa: PLR0913
    *,
    org: str,
    project: str,
    git_ref: str,
    source_dir: Path,
    token: str,
    base_url: str,
    alternate_name: str | None,
    auto_annotations: bool,
    manual_annotations: dict[str, str] | None,
    no_wait: bool,
    verbose: bool,
) -> None:
    """Run the upload workflow."""
    tarball_path: Path | None = None
    try:
        click.echo(f"Creating tarball from {source_dir}")
        tarball_path, content_hash = create_tarball(source_dir)
        click.echo(f"Content hash: {content_hash}")

        # Build annotations from auto-detection and manual entries
        auto = (
            detect_github_actions_annotations() if auto_annotations else None
        )
        merged_annotations = merge_annotations(auto, manual_annotations)

        async with DocverseClient(base_url, token, verbose=verbose) as client:
            click.echo(f"Creating build for {org}/{project} @ {git_ref}")
            build = await client.create_build(
                org,
                project,
                git_ref=git_ref,
                content_hash=content_hash,
                alternate_name=alternate_name,
                annotations=merged_annotations,
            )
            click.echo(f"Build created: {build.id}")

            if build.upload_url is None:
                msg = "Server did not return an upload URL"
                raise DocverseClientError(msg)  # noqa: TRY301

            click.echo("Uploading tarball")
            await client.upload_tarball(build.upload_url, tarball_path)

            click.echo("Signalling upload complete")
            build = await client.complete_upload(build.self_url)

            if no_wait:
                click.echo("Upload complete (--no-wait specified)")
                return

            if build.queue_url is None:
                msg = "Server did not return a queue URL after upload"
                raise DocverseClientError(msg)  # noqa: TRY301

            click.echo("Waiting for build processing")
            job = await client.wait_for_job(build.queue_url)

            if job.status == JobStatus.completed_with_errors:
                click.echo(
                    f"Build completed with errors (phase={job.phase})",
                    err=True,
                )
                sys.exit(2)
            elif job.status == JobStatus.cancelled:
                click.echo("Build processing was cancelled", err=True)
                sys.exit(2)
            else:
                click.echo("Build processing complete")

    except BuildProcessingError as exc:
        raise click.ClickException(str(exc)) from exc
    except DocverseClientError as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        if tarball_path is not None:
            tarball_path.unlink(missing_ok=True)
