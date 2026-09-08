"""Tests for docverse.models.builds."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from docverse.models import Build, BuildCreate, BuildStatus


def test_content_hash_is_optional() -> None:
    """``content_hash`` may be omitted entirely.

    The field is a deprecated transport digest: content identity is the
    server's to compute, so a client that has nothing to say about the
    bytes it uploaded must be able to stay silent rather than invent a
    value.
    """
    data = BuildCreate(git_ref="main")
    assert data.content_hash is None
    assert "content_hash" not in data.model_dump(exclude_none=True)


def test_content_hash_pattern_still_enforced() -> None:
    """A supplied digest must still match ``sha256:<64 hex>``.

    Optional widens what may be omitted, not what may be sent: an old
    client that sends a malformed digest is still rejected rather than
    silently writing junk into a column the convergence lookup reads.
    """
    with pytest.raises(ValidationError):
        BuildCreate(git_ref="main", content_hash="not-a-digest")


def test_content_hash_accepted_when_supplied() -> None:
    """A well-formed digest from an old client round-trips unchanged."""
    digest = "sha256:" + "a" * 64
    data = BuildCreate(git_ref="main", content_hash=digest)
    assert data.content_hash == digest


def test_content_hash_documented_as_deprecated() -> None:
    """The deprecation lives in the field description, not a warning.

    There is no runtime ``DeprecationWarning``, so the description is
    the only signal an integrator gets — in the OpenAPI schema and in
    ``help()`` alike.
    """
    description = BuildCreate.model_fields["content_hash"].description
    assert description is not None
    assert "deprecated" in description.lower()


def test_content_hash_flagged_deprecated_in_json_schema() -> None:
    """The deprecation is machine-readable, not only prose.

    Prose in the description tells a human; ``deprecated: true`` in the
    JSON Schema tells a code generator, which is what actually stops the
    field from being propagated into new client SDKs. Asserting the flag
    on the emitted schema (rather than on the ``Field`` call) also pins
    that it survives the ``str | None`` ``anyOf`` wrapping, which is
    where a naively-placed extra would get buried.
    """
    schema = BuildCreate.model_json_schema()["properties"]["content_hash"]
    assert schema["deprecated"] is True


def test_terminal_statuses_include_superseded_and_cancelled() -> None:
    """``BuildStatus`` carries the two never-published terminal values.

    A build that a newer build for the same ref took over, and a build
    deleted before processing finished, are both terminal outcomes the
    API reports — and both are distinct from ``failed``, which means
    something went wrong. Clients filtering builds by status need the
    values to exist in the enum before they can ask for them.
    """
    assert BuildStatus.superseded == "superseded"
    assert BuildStatus.cancelled == "cancelled"


def test_build_model_validates_new_terminal_statuses() -> None:
    """``Build.model_validate`` accepts a payload in either new status."""
    for status in (BuildStatus.superseded, BuildStatus.cancelled):
        build = Build.model_validate(
            {
                "id": "1x7r-9fd4-hw1b-51",
                "project_url": ("https://example.com/orgs/o/projects/p"),
                "self_url": "https://example.com/orgs/o/projects/p/builds/b",
                "git_ref": "main",
                "status": status.value,
                "content_hash": "sha256:" + "a" * 64,
                "uploader": "someone",
                "date_created": "2026-09-02T00:00:00Z",
            }
        )
        assert build.status is status


def test_status_partition_covers_every_member() -> None:
    """Every status is exactly one of unfinished, terminal, or the signal.

    ``is_unfinished`` and ``is_terminal`` are the single definition of
    the partition the server's transition table, retirement helpers and
    ``build_processing`` worker all branch on, so a status added to the
    enum without a place in it is a bug waiting to happen: the server
    would treat the newcomer as live *and* refuse every transition out
    of it. ``uploaded`` is deliberately in neither half — it is a PATCH
    signal value that is never persisted on a row.
    """
    for status in BuildStatus:
        if status is BuildStatus.uploaded:
            assert not status.is_unfinished
            assert not status.is_terminal
        else:
            assert status.is_unfinished is not status.is_terminal


def test_unfinished_statuses_are_the_ones_a_build_can_leave() -> None:
    """Only ``pending`` and ``processing`` mean the build is still live."""
    unfinished = {status for status in BuildStatus if status.is_unfinished}
    assert unfinished == {BuildStatus.pending, BuildStatus.processing}


def test_terminal_statuses_are_the_build_s_final_answer() -> None:
    """The four never-leave statuses are the terminal half."""
    terminal = {status for status in BuildStatus if status.is_terminal}
    assert terminal == {
        BuildStatus.completed,
        BuildStatus.failed,
        BuildStatus.superseded,
        BuildStatus.cancelled,
    }


def test_build_reports_when_its_objects_were_reclaimed() -> None:
    """``Build.date_purged`` says whether the build is still restorable.

    A soft-deleted build keeps its row and its history, so
    ``date_deleted`` alone cannot tell a caller whether the content is
    still there. ``date_purged`` is the field that can: null means the
    objects survive and a restore would hand back a working build, a
    timestamp means the purgatory sweep has already reclaimed them and
    the row is a tombstone.
    """
    now = datetime(2026, 9, 8, 12, 0, tzinfo=UTC)
    build = Build(
        self_url="https://example.com/build",
        project_url="https://example.com/project",
        id="1txq-55pj-1x5m-16",
        git_ref="main",
        content_hash="sha256:" + "a" * 64,
        status=BuildStatus.completed,
        uploader="jdoe",
        date_created=now,
        date_purged=now,
    )
    assert build.date_purged == now
    assert Build.model_validate_json(build.model_dump_json()) == build


def test_build_date_purged_defaults_to_none() -> None:
    """A build the sweep has not reached reports ``date_purged`` null.

    Every build predating the purgatory sweep is in exactly that state,
    so the field has to be omissible rather than required — an old
    server's response must still parse.
    """
    build = Build(
        self_url="https://example.com/build",
        project_url="https://example.com/project",
        id="1txq-55pj-1x5m-16",
        git_ref="main",
        content_hash="sha256:" + "a" * 64,
        status=BuildStatus.completed,
        uploader="jdoe",
        date_created=datetime(2026, 9, 8, 12, 0, tzinfo=UTC),
    )
    assert build.date_purged is None
