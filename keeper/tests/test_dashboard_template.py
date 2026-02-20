"""Test the built-in dashboard template by rendering to a local directory."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import List

from keeper.dashboard.context import (
    BuildContext,
    BuildContextList,
    EditionContext,
    EditionContextList,
    ProjectContext,
)
from keeper.dashboard.templateproviders import BuiltinTemplateProvider
from keeper.models import EditionKind


def test_templates() -> None:
    output_dir = Path(__file__).parent.parent.joinpath("dashboard_dev")

    # Create mock data
    project = ProjectContext(
        title="LTD Test Project",
        source_repo_url="https://github.com/lsst-sqre/ltd-keeper",
        url="https://example.com/ltd-test/",
    )
    editions: List[EditionContext] = []
    builds: List[BuildContext] = []

    editions.append(
        EditionContext(
            title="Current",
            url="https://example.com/ltd-test/",
            date_updated=datetime(2022, 6, 24, tzinfo=timezone.utc),
            kind=EditionKind.main,
            slug="__main",
            git_ref="main",
            github_url="https://example.com/ltd-test/tree/main",
        )
    )

    editions.append(
        EditionContext(
            title="1.0.0",
            url="https://example.com/ltd-test/v/1.0.0",
            date_updated=datetime(2022, 6, 21, tzinfo=timezone.utc),
            kind=EditionKind.release,
            slug="1.0.0",
            git_ref="1.0.0",
            github_url="https://example.com/ltd-test/tree/1.0.0",
        )
    )
    editions.append(
        EditionContext(
            title="1.1.0",
            url="https://example.com/ltd-test/v/1.1.0",
            date_updated=datetime(2022, 6, 22, tzinfo=timezone.utc),
            kind=EditionKind.release,
            slug="1.1.0",
            git_ref="1.1.0",
            github_url="https://example.com/ltd-test/tree/1.1.0",
        )
    )
    editions.append(
        EditionContext(
            title="2.0.0",
            url="https://example.com/ltd-test/v/2.0.0",
            date_updated=datetime(2022, 6, 24, tzinfo=timezone.utc),
            kind=EditionKind.release,
            slug="2.0.0",
            git_ref="2.0.0",
            github_url="https://example.com/ltd-test/tree/2.0.0",
        )
    )
    editions.append(
        EditionContext(
            title="my-branch",
            url="https://example.com/ltd-test/v/my-branch",
            date_updated=datetime(2022, 6, 24, tzinfo=timezone.utc),
            kind=EditionKind.draft,
            slug="my-branch",
            git_ref="my-branch",
            github_url="https://example.com/ltd-test/tree/my-branch",
        )
    )

    builds.append(
        BuildContext(
            slug="1",
            url="https://example.com/ltd-test/builds/1",
            git_ref="main",
            date=datetime(2022, 6, 21, tzinfo=timezone.utc),
        )
    )

    template = BuiltinTemplateProvider()
    template.render_locally(
        directory=output_dir,
        project_context=project,
        edition_contexts=EditionContextList(editions),
        build_contexts=BuildContextList(builds),
    )
