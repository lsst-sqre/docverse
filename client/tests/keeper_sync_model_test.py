"""Tests for the ``KeeperSyncConfig`` client model."""

from __future__ import annotations

import re
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from docverse.models import (
    KeeperSyncConfig,
    KeeperSyncConfigUpdate,
    KeeperSyncConfigWrite,
    KeeperSyncProjectStatus,
    KeeperSyncRun,
    KeeperSyncScopePreview,
    KeeperSyncScopePreviewRequest,
    KeeperSyncTombstone,
)

_PROJECT_STATUS_BASE = "https://docverse.example/orgs/o"


def _project_status_payload(**overrides: object) -> dict[str, object]:
    """Build a minimal well-formed ``KeeperSyncProjectStatus`` body.

    Only the required fields, so a test can say which optional field it
    is actually about by naming it in ``overrides``.
    """
    payload: dict[str, object] = {
        "self_url": f"{_PROJECT_STATUS_BASE}/keeper-sync/projects/sqr-112",
        "org_url": _PROJECT_STATUS_BASE,
        "sync_refresh_url": (
            f"{_PROJECT_STATUS_BASE}/keeper-sync/projects/sqr-112/refresh"
        ),
        "editions_sync_url": (
            f"{_PROJECT_STATUS_BASE}/keeper-sync/projects/sqr-112/editions"
        ),
        "ltd_slug": "sqr-112",
        "tier_status": [],
    }
    payload.update(overrides)
    return payload


def test_default_is_disabled_with_default_url_and_empty_allowlist() -> None:
    config = KeeperSyncConfig()
    assert config.enabled is False
    assert str(config.ltd_base_url) == "https://keeper.lsst.codes/"
    assert config.project_slugs == []


def test_default_scope_fields_are_empty() -> None:
    """The three scope fields added by PRD #667 default to empty lists."""
    config = KeeperSyncConfig()
    assert config.project_slug_patterns == []
    assert config.exclude_project_slugs == []
    assert config.exclude_project_slug_patterns == []


def test_round_trips_explicit_allowlist() -> None:
    config = KeeperSyncConfig(
        enabled=True,
        ltd_base_url="https://keeper.example.com/",
        project_slugs=["alpha", "beta"],
    )
    dumped = config.model_dump(mode="json")
    restored = KeeperSyncConfig.model_validate(dumped)
    assert restored == config


def test_round_trips_wildcard() -> None:
    """``project_slugs="*"`` round-trips through ``model_dump``."""
    config = KeeperSyncConfig(
        enabled=True,
        ltd_base_url="https://keeper.example.com/",
        project_slugs="*",
    )
    dumped = config.model_dump(mode="json")
    assert dumped["project_slugs"] == "*"
    restored = KeeperSyncConfig.model_validate(dumped)
    assert restored.project_slugs == "*"


def test_rejects_unknown_string_token_for_project_slugs() -> None:
    """Only the literal ``"*"`` is accepted; ``"all"`` etc. is rejected."""
    with pytest.raises(ValidationError):
        KeeperSyncConfig(
            enabled=True,
            ltd_base_url="https://keeper.example.com/",
            project_slugs="ALL",  # type: ignore[arg-type]
        )


def test_rejects_invalid_url() -> None:
    with pytest.raises(ValidationError):
        KeeperSyncConfig(
            enabled=True,
            ltd_base_url="not-a-url",  # type: ignore[arg-type]
            project_slugs=[],
        )


@pytest.mark.parametrize(
    ("config_kwargs", "slug", "expected"),
    [
        # Explicit list membership.
        ({"project_slugs": ["sqr-112"]}, "sqr-112", True),
        ({"project_slugs": ["sqr-112"]}, "dmtn-001", False),
        # Empty config includes nothing.
        ({}, "sqr-112", False),
        # Include pattern.
        ({"project_slug_patterns": [r"sqr-\d+"]}, "sqr-112", True),
        ({"project_slug_patterns": [r"sqr-\d+"]}, "dmtn-001", False),
        # Wildcard includes everything.
        ({"project_slugs": "*"}, "anything", True),
        # Wildcard plus redundant include patterns is allowed.
        (
            {"project_slugs": "*", "project_slug_patterns": [r"sqr-\d+"]},
            "dmtn-001",
            True,
        ),
        # Excludes win over the wildcard...
        (
            {"project_slugs": "*", "exclude_project_slugs": ["www"]},
            "www",
            False,
        ),
        # ...over an explicit include...
        (
            {
                "project_slugs": ["sqr-112"],
                "exclude_project_slugs": ["sqr-112"],
            },
            "sqr-112",
            False,
        ),
        # ...and over a pattern include.
        (
            {
                "project_slug_patterns": [r"sqr-\d+"],
                "exclude_project_slugs": ["sqr-112"],
            },
            "sqr-112",
            False,
        ),
        # Exclude patterns win too.
        (
            {
                "project_slugs": "*",
                "exclude_project_slug_patterns": [r"test-.*"],
            },
            "test-thing",
            False,
        ),
        (
            {
                "project_slug_patterns": [r"sqr-\d+"],
                "exclude_project_slug_patterns": [r"sqr-9\d"],
            },
            "sqr-90",
            False,
        ),
        # ``fullmatch`` boundaries: ``sqr-1`` is not a prefix match.
        ({"project_slug_patterns": ["sqr-1"]}, "sqr-1", True),
        ({"project_slug_patterns": ["sqr-1"]}, "sqr-10", False),
        ({"project_slug_patterns": ["sqr-1"]}, "sqr-100", False),
        ({"project_slug_patterns": ["qr-1"]}, "sqr-1", False),
        # Matching is case-sensitive.
        ({"project_slug_patterns": [r"sqr-\d+"]}, "SQR-112", False),
        ({"project_slugs": ["sqr-112"]}, "SQR-112", False),
        (
            {"project_slugs": "*", "exclude_project_slugs": ["WWW"]},
            "www",
            True,
        ),
    ],
)
def test_is_in_scope_rule(
    *, config_kwargs: dict[str, object], slug: str, expected: bool
) -> None:
    """The scope rule: includes, then excludes, ``fullmatch``, case."""
    config = KeeperSyncConfig(**config_kwargs)  # type: ignore[arg-type]
    assert config.is_in_scope(slug) is expected


def test_filter_in_scope_preserves_input_order() -> None:
    """``filter_in_scope`` keeps the caller's (LTD listing) order."""
    config = KeeperSyncConfig(
        project_slugs=["zulu", "alpha"],
        project_slug_patterns=[r"sqr-\d+"],
        exclude_project_slugs=["sqr-999"],
    )
    given = ["sqr-999", "zulu", "dmtn-001", "sqr-1", "alpha", "sqr-2"]
    assert config.filter_in_scope(given) == ["zulu", "sqr-1", "alpha", "sqr-2"]


def test_filter_in_scope_wildcard_returns_every_slug() -> None:
    """``"*"`` with no excludes passes the whole listing through."""
    config = KeeperSyncConfig(project_slugs="*")
    given = ["b", "a", "c"]
    assert config.filter_in_scope(given) == given


@pytest.mark.parametrize(
    ("config_kwargs", "slugs", "expected_in_scope", "expected_excluded"),
    [
        # An exact exclude removes an explicitly included slug.
        (
            {
                "project_slugs": ["sqr-112", "dmtn-001"],
                "exclude_project_slugs": ["dmtn-001"],
            },
            ["sqr-112", "dmtn-001"],
            ["sqr-112"],
            1,
        ),
        # An exclude pattern removes pattern-included slugs.
        (
            {
                "project_slug_patterns": [r"sqr-\d+"],
                "exclude_project_slug_patterns": [r"sqr-9\d"],
            },
            ["sqr-1", "sqr-90", "sqr-91", "dmtn-001"],
            ["sqr-1"],
            2,
        ),
        # Both exclude kinds contribute to the same counter.
        (
            {
                "project_slugs": "*",
                "exclude_project_slugs": ["www"],
                "exclude_project_slug_patterns": [r"test-.*"],
            },
            ["www", "test-a", "test-b", "sqr-1"],
            ["sqr-1"],
            3,
        ),
        # ``"*"`` plus an exclude: everything is included, one removed.
        (
            {"project_slugs": "*", "exclude_project_slugs": ["www"]},
            ["www", "sqr-1"],
            ["sqr-1"],
            1,
        ),
        # A slug excluded that no include rule admitted counts zero: it
        # was never in scope for an exclude to take away.
        (
            {
                "project_slugs": ["sqr-112"],
                "exclude_project_slugs": ["dmtn-001"],
            },
            ["sqr-112", "dmtn-001"],
            ["sqr-112"],
            0,
        ),
        # No exclude rules at all.
        (
            {"project_slug_patterns": [r"sqr-\d+"]},
            ["sqr-1", "dmtn-001"],
            ["sqr-1"],
            0,
        ),
    ],
)
def test_resolve_scope_reports_excluded_count(
    *,
    config_kwargs: dict[str, object],
    slugs: list[str],
    expected_in_scope: list[str],
    expected_excluded: int,
) -> None:
    """``resolve_scope`` classifies in one pass: in-scope list + count."""
    config = KeeperSyncConfig(**config_kwargs)  # type: ignore[arg-type]
    in_scope, excluded_count = config.resolve_scope(slugs)
    assert in_scope == expected_in_scope
    assert excluded_count == expected_excluded
    # ``filter_in_scope`` is the same pass with the counter dropped.
    assert config.filter_in_scope(slugs) == in_scope


def test_resolve_scope_preserves_input_order() -> None:
    """``resolve_scope`` keeps the caller's (LTD listing) order."""
    config = KeeperSyncConfig(
        project_slugs=["zulu", "alpha"],
        project_slug_patterns=[r"sqr-\d+"],
        exclude_project_slugs=["sqr-999"],
    )
    given = ["sqr-999", "zulu", "dmtn-001", "sqr-1", "alpha", "sqr-2"]
    in_scope, excluded_count = config.resolve_scope(given)
    assert in_scope == ["zulu", "sqr-1", "alpha", "sqr-2"]
    assert excluded_count == 1


def test_compiled_scope_patterns_are_reused_across_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One instance compiles each scope pattern once, not once per call.

    ``is_in_scope`` is the per-request scope gate, so a fresh compile of
    every include and exclude pattern on each call is what this guards
    against (issue #676).
    """
    # Patterns unique to this test, so the compiled-pattern cache is
    # cold no matter what else ran first.
    config = KeeperSyncConfig(
        project_slug_patterns=[r"reuse-probe-\d+"],
        exclude_project_slug_patterns=[r"reuse-probe-9\d"],
    )

    compiled: list[str] = []
    real_compile = re.compile

    def counting_compile(pattern: str, flags: int = 0) -> re.Pattern[str]:
        compiled.append(pattern)
        return real_compile(pattern, flags)

    monkeypatch.setattr(re, "compile", counting_compile)

    assert config.is_in_scope("reuse-probe-1") is True
    first_pass = list(compiled)
    assert sorted(first_pass) == [r"reuse-probe-9\d", r"reuse-probe-\d+"]

    assert config.is_in_scope("reuse-probe-90") is False
    assert config.filter_in_scope(["reuse-probe-2", "other"]) == [
        "reuse-probe-2"
    ]
    assert config.resolve_scope(["reuse-probe-3"]) == (["reuse-probe-3"], 0)
    assert compiled == first_pass


def test_stored_config_without_scope_fields_loads_with_defaults() -> None:
    """A config persisted before PRD #667 loads with empty scope fields."""
    stored = {
        "enabled": True,
        "ltd_base_url": "https://keeper.lsst.codes/",
        "project_slugs": ["sqr-112"],
    }
    config = KeeperSyncConfig.model_validate(stored)
    assert config.project_slug_patterns == []
    assert config.exclude_project_slugs == []
    assert config.exclude_project_slug_patterns == []
    assert config.is_in_scope("sqr-112") is True
    assert config.is_in_scope("dmtn-001") is False


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_rejects_uncompilable_pattern(field: str) -> None:
    """A bad regex names both the field and the offending pattern."""
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncConfig.model_validate({field: ["sqr-("]})
    message = str(excinfo.value)
    assert field in message
    assert "sqr-(" in message


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_rejects_over_long_pattern(field: str) -> None:
    """A pattern longer than 256 characters is rejected."""
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncConfig.model_validate({field: ["a" * 257]})
    assert field in str(excinfo.value)


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_accepts_pattern_at_the_length_limit(field: str) -> None:
    """256 characters is the limit, not one short of it."""
    config = KeeperSyncConfig.model_validate({field: ["a" * 256]})
    assert getattr(config, field) == ["a" * 256]


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_rejects_more_than_100_patterns(field: str) -> None:
    """More than 100 entries in a pattern field is rejected."""
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncConfig.model_validate({field: [f"p{i}" for i in range(101)]})
    assert field in str(excinfo.value)


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_accepts_exactly_100_patterns(field: str) -> None:
    """100 entries is the limit, not one short of it."""
    patterns = [f"p{i}" for i in range(100)]
    config = KeeperSyncConfig.model_validate({field: patterns})
    assert getattr(config, field) == patterns


def test_config_update_all_fields_default_unset() -> None:
    """An empty update dumps to nothing under ``exclude_unset``."""
    update = KeeperSyncConfigUpdate()
    assert update.model_dump(exclude_unset=True) == {}


def test_config_update_omits_untouched_fields() -> None:
    """Only the provided field survives ``model_dump(exclude_unset=True)``."""
    update = KeeperSyncConfigUpdate(enabled=False)
    assert update.model_dump(exclude_unset=True) == {"enabled": False}


def test_config_update_project_slugs_replaces_wholesale() -> None:
    """``project_slugs`` carries the full replacement list, or ``"*"``."""
    update = KeeperSyncConfigUpdate(project_slugs=["alpha", "beta"])
    assert update.model_dump(exclude_unset=True) == {
        "project_slugs": ["alpha", "beta"]
    }
    wildcard = KeeperSyncConfigUpdate(project_slugs="*")
    assert wildcard.model_dump(exclude_unset=True) == {"project_slugs": "*"}


@pytest.mark.parametrize(
    "field",
    [
        "project_slug_patterns",
        "exclude_project_slugs",
        "exclude_project_slug_patterns",
    ],
)
def test_config_update_scope_fields_replace_wholesale(field: str) -> None:
    """Each new scope field carries its full replacement list."""
    update = KeeperSyncConfigUpdate.model_validate({field: ["alpha"]})
    assert update.model_dump(exclude_unset=True) == {field: ["alpha"]}


@pytest.mark.parametrize(
    "field",
    [
        "project_slug_patterns",
        "exclude_project_slugs",
        "exclude_project_slug_patterns",
    ],
)
def test_config_update_rejects_explicit_null_on_scope_fields(
    field: str,
) -> None:
    """An explicit ``null`` is rejected on each new scope field."""
    with pytest.raises(ValidationError):
        KeeperSyncConfigUpdate.model_validate({field: None})


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_config_update_rejects_uncompilable_pattern(field: str) -> None:
    """``PATCH`` shares the model's pattern validation with ``PUT``."""
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncConfigUpdate.model_validate({field: ["sqr-("]})
    message = str(excinfo.value)
    assert field in message
    assert "sqr-(" in message


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_config_update_rejects_over_long_pattern(field: str) -> None:
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncConfigUpdate.model_validate({field: ["a" * 257]})
    assert field in str(excinfo.value)


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_config_update_rejects_more_than_100_patterns(field: str) -> None:
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncConfigUpdate.model_validate(
            {field: [f"p{i}" for i in range(101)]}
        )
    assert field in str(excinfo.value)


def test_config_update_forbids_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        KeeperSyncConfigUpdate.model_validate({"unknown": True})


def test_config_update_rejects_unknown_string_token() -> None:
    """Only the literal ``"*"`` is accepted for ``project_slugs``."""
    with pytest.raises(ValidationError):
        KeeperSyncConfigUpdate(project_slugs="ALL")  # type: ignore[arg-type]


def test_scope_preview_request_rejects_ltd_base_url() -> None:
    """A preview body may not repoint the LTD instance it fetches.

    The preview resolves a *scope*, and the LTD instance is not part of
    a scope. Honouring a candidate ``ltd_base_url`` would make the
    endpoint fetch an operator-supplied URL from inside the cluster and
    report the upstream status back — an interactive status oracle for
    internal hosts — so the field is refused outright, by name.
    """
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncScopePreviewRequest.model_validate(
            {"ltd_base_url": "http://169.254.169.254/"}
        )
    assert "ltd_base_url" in str(excinfo.value)


def test_scope_preview_request_rejects_null_ltd_base_url() -> None:
    """Even an explicit ``null`` for the field is refused.

    Nothing about ``null`` is a safe special case: the field simply has
    no meaning in a preview body, so naming it at all is the error.
    """
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncScopePreviewRequest.model_validate({"ltd_base_url": None})
    assert "ltd_base_url" in str(excinfo.value)


def test_scope_preview_request_schema_omits_ltd_base_url() -> None:
    """The published schema does not advertise a field it always rejects."""
    schema = KeeperSyncScopePreviewRequest.model_json_schema()
    assert "ltd_base_url" not in schema["properties"]
    assert "project_slug_patterns" in schema["properties"]


def test_scope_preview_request_is_a_config_update() -> None:
    """It merges over the stored config exactly as a ``PATCH`` body does."""
    request = KeeperSyncScopePreviewRequest(project_slugs="*")
    assert isinstance(request, KeeperSyncConfigUpdate)
    assert request.model_dump(exclude_unset=True) == {"project_slugs": "*"}


@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
def test_scope_preview_request_validates_patterns_like_patch(
    field: str,
) -> None:
    """The preview is not a way to sneak an invalid pattern past ``PATCH``."""
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncScopePreviewRequest.model_validate({field: ["sqr-("]})
    message = str(excinfo.value)
    assert field in message
    assert "sqr-(" in message


def test_keeper_sync_run_id_is_base32_string() -> None:
    """``KeeperSyncRun.id`` carries the Base32 public id as a string.

    Locks the API contract change from an integer primary key to the
    run's Base32 public identifier; the value round-trips unchanged
    through ``model_dump``/``model_validate``.
    """
    run = KeeperSyncRun(
        self_url="https://docverse.example/orgs/o/keeper-sync/runs/AAAA-BBBB",
        jobs_url=(
            "https://docverse.example/orgs/o/keeper-sync/runs/AAAA-BBBB/jobs"
        ),
        id="AAAA-BBBB-CCCC-05",
        kind="backfill",
        status="pending",
        pending_count=0,
        succeeded_count=0,
        failed_count=0,
        total_count=0,
        date_started=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert run.id == "AAAA-BBBB-CCCC-05"

    restored = KeeperSyncRun.model_validate(run.model_dump(mode="json"))
    assert restored.id == "AAAA-BBBB-CCCC-05"


def test_keeper_sync_tombstone_id_is_base32_string() -> None:
    """``KeeperSyncTombstone.id`` carries the Base32 public id as a string.

    Locks the API contract change from the raw ``keeper_sync_state``
    primary key (``state_id``) to the row's Base32 public identifier;
    the value round-trips unchanged through
    ``model_dump``/``model_validate``.
    """
    tombstone = KeeperSyncTombstone(
        self_url=(
            "https://docverse.example/orgs/o/keeper-sync/tombstones/"
            "AAAA-BBBB-CCCC-05"
        ),
        id="AAAA-BBBB-CCCC-05",
        resource_type="edition",
        ltd_slug="v1.0",
        ltd_id=42,
        date_tombstoned=datetime(2026, 1, 1, tzinfo=UTC),
        tombstone_reason="manual_delete",
        display_path="proj/v1.0",
    )
    assert tombstone.id == "AAAA-BBBB-CCCC-05"

    restored = KeeperSyncTombstone.model_validate(
        tombstone.model_dump(mode="json")
    )
    assert restored.id == "AAAA-BBBB-CCCC-05"


def test_unknown_fields_are_ignored() -> None:
    """A config carrying an unknown key loads, with the key dropped.

    ``KeeperSyncConfig`` is a *read* model: it parses both the stored
    JSONB blob and a server's response body, either of which may carry a
    field a newer server writes and this release does not know. Tolerating
    it keeps an older reader — a rolled-back server loading the row, or a
    published client parsing the response — working instead of failing
    validation on a field it would only discard anyway.
    """
    config = KeeperSyncConfig.model_validate(
        {
            "enabled": False,
            "ltd_base_url": "https://keeper.lsst.codes/",
            "project_slugs": [],
            "unknown": True,
        }
    )
    assert config.enabled is False
    assert "unknown" not in config.model_dump()


def test_config_write_forbids_unknown_fields() -> None:
    """``PUT``'s request model stays strict even though the read model isn't.

    The asymmetry is the point: an unknown key reaching a reader is a
    newer writer's field and is dropped, but an unknown key in a request
    body is the caller's typo and must not be silently discarded.
    """
    with pytest.raises(ValidationError):
        KeeperSyncConfigWrite.model_validate(
            {
                "enabled": False,
                "ltd_base_url": "https://keeper.lsst.codes/",
                "project_slugs": [],
                "unknown": True,
            }
        )


def test_config_write_accepts_a_full_config_payload() -> None:
    """The write model parses every field the read model does."""
    payload = {
        "enabled": True,
        "ltd_base_url": "https://keeper.example.com/",
        "project_slugs": ["sqr-112"],
        "project_slug_patterns": [r"sqr-\d+"],
        "exclude_project_slugs": ["www"],
        "exclude_project_slug_patterns": [r"test-.*"],
    }
    write = KeeperSyncConfigWrite.model_validate(payload)
    assert write.model_dump(mode="json") == payload
    assert KeeperSyncConfig.model_validate(payload) == KeeperSyncConfig(
        **write.model_dump()
    )


def test_config_write_validates_patterns() -> None:
    """An uncompilable pattern is rejected on ``PUT`` as it is on ``PATCH``."""
    with pytest.raises(ValidationError) as excinfo:
        KeeperSyncConfigWrite.model_validate({"project_slug_patterns": ["a("]})
    assert "project_slug_patterns" in str(excinfo.value)


def test_scope_preview_slug_lists_default_to_empty() -> None:
    """Only the two counts are required; every slug list defaults empty.

    An org whose scope resolves to nothing still gets a well-formed
    report rather than a response with missing keys.
    """
    preview = KeeperSyncScopePreview(ltd_count=3, in_scope_count=0)
    assert preview.in_scope_slugs == []
    assert preview.new_slugs == []
    assert preview.tombstoned_slugs == []
    assert preview.unmatched_project_slugs == []
    assert preview.unmatched_exclude_project_slugs == []


def test_scope_preview_example_validates_against_the_model() -> None:
    """The OpenAPI example is a real response body, field for field.

    The example is what an operator reads in the API docs before they
    ever call the endpoint, so it has to parse as the model *and* name
    every field — a field missing from the example is one the docs
    quietly imply the response does not have.
    """
    schema_extra = KeeperSyncScopePreview.model_config["json_schema_extra"]
    assert isinstance(schema_extra, dict)
    examples = schema_extra["examples"]
    assert isinstance(examples, list)
    example = examples[0]
    assert isinstance(example, dict)

    assert set(example) == set(KeeperSyncScopePreview.model_fields)
    preview = KeeperSyncScopePreview.model_validate(example)
    assert preview.model_dump(mode="json") == example


def test_project_status_in_scope_defaults_to_true() -> None:
    """A body without ``in_scope`` reads back as in scope.

    The field is new, so a response from a server that predates it
    carries no such key. Defaulting to ``true`` keeps that body meaning
    what it always meant: every project the old listing returned was
    one an operator had no reason to treat as excluded.
    """
    status = KeeperSyncProjectStatus.model_validate(_project_status_payload())
    assert status.in_scope is True


def test_project_status_in_scope_round_trips_false() -> None:
    """An excluded project reports ``in_scope: false`` and keeps it.

    This is the value the flag exists for: the listing keeps returning
    a project that has fallen out of scope, so ``false`` has to survive
    a dump/validate round-trip to reach whatever reads the listing.
    """
    status = KeeperSyncProjectStatus.model_validate(
        _project_status_payload(in_scope=False)
    )
    assert status.in_scope is False
    assert status.model_dump(mode="json")["in_scope"] is False


def test_project_status_in_scope_examples_validate_against_the_model() -> None:
    """The field's OpenAPI examples are values the model accepts.

    An example in the published schema is what an operator reads before
    they ever call the endpoint, so both of them have to parse — and
    the field has to carry the description that says which endpoint can
    report ``false``.
    """
    field = KeeperSyncProjectStatus.model_fields["in_scope"]
    assert field.description
    assert field.examples == [True, False]
    for example in field.examples:
        status = KeeperSyncProjectStatus.model_validate(
            _project_status_payload(in_scope=example)
        )
        assert status.in_scope is example
