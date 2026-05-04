"""Tests for build provenance annotation helpers."""

from __future__ import annotations

from unittest.mock import patch

from docverse.client._annotations import (
    detect_github_actions_annotations,
    merge_annotations,
)
from docverse.client.models.builds import BuildAnnotations


class TestBuildAnnotations:
    """Tests for the BuildAnnotations model."""

    def test_defaults_to_none(self) -> None:
        model = BuildAnnotations()
        assert model.commit_sha is None
        assert model.ci_platform is None

    def test_extra_fields_allowed(self) -> None:
        model = BuildAnnotations(
            commit_sha="abc123", custom_key="custom_value"
        )
        assert model.commit_sha == "abc123"
        assert model.model_dump()["custom_key"] == "custom_value"

    def test_serialization_round_trip(self) -> None:
        model = BuildAnnotations(
            commit_sha="abc",
            ci_platform="github-actions",
            extra_field="hello",
        )
        data = model.model_dump()
        restored = BuildAnnotations.model_validate(data)
        assert restored.commit_sha == "abc"
        assert restored.ci_platform == "github-actions"
        assert data["extra_field"] == "hello"


class TestDetectGitHubActionsAnnotations:
    """Tests for detect_github_actions_annotations."""

    def test_not_github_actions(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            assert detect_github_actions_annotations() is None

    def test_github_actions_full(self) -> None:
        env = {
            "GITHUB_ACTIONS": "true",
            "GITHUB_SHA": "deadbeef",
            "GITHUB_REPOSITORY": "owner/repo",
            "GITHUB_RUN_ID": "12345",
            "GITHUB_SERVER_URL": "https://github.com",
            "GITHUB_RUN_ATTEMPT": "1",
            "GITHUB_WORKFLOW": "CI",
            "GITHUB_ACTOR": "user",
            "GITHUB_EVENT_NAME": "push",
        }
        with patch.dict("os.environ", env, clear=True):
            result = detect_github_actions_annotations()

        assert result is not None
        assert result.commit_sha == "deadbeef"
        assert result.github_repository == "owner/repo"
        assert result.github_run_id == "12345"
        assert (
            result.github_run_url
            == "https://github.com/owner/repo/actions/runs/12345"
        )
        assert result.github_run_attempt == "1"
        assert result.github_workflow == "CI"
        assert result.github_actor == "user"
        assert result.github_event_name == "push"
        assert result.ci_platform == "github-actions"

    def test_github_actions_minimal(self) -> None:
        env = {"GITHUB_ACTIONS": "true"}
        with patch.dict("os.environ", env, clear=True):
            result = detect_github_actions_annotations()

        assert result is not None
        assert result.commit_sha is None
        assert result.github_run_url is None
        assert result.ci_platform == "github-actions"


class TestMergeAnnotations:
    """Tests for merge_annotations."""

    def test_both_none(self) -> None:
        assert merge_annotations(None, None) is None

    def test_auto_only(self) -> None:
        auto = BuildAnnotations(commit_sha="abc", ci_platform="gh")
        result = merge_annotations(auto, None)
        assert result is not None
        assert result.commit_sha == "abc"
        assert result.ci_platform == "gh"

    def test_manual_only(self) -> None:
        result = merge_annotations(None, {"foo": "bar"})
        assert result is not None
        assert result.model_dump()["foo"] == "bar"

    def test_manual_overrides_auto(self) -> None:
        auto = BuildAnnotations(commit_sha="auto-sha", ci_platform="gh")
        manual = {"commit_sha": "manual-sha", "custom": "val"}
        result = merge_annotations(auto, manual)
        assert result is not None
        assert result.commit_sha == "manual-sha"
        assert result.ci_platform == "gh"
        assert result.model_dump()["custom"] == "val"

    def test_none_fields_excluded_from_auto(self) -> None:
        auto = BuildAnnotations(commit_sha="abc")
        result = merge_annotations(auto, None)
        assert result is not None
        dumped = result.model_dump()
        assert dumped.get("github_run_id") is None
