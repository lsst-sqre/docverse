# Test skeleton template

The parametrized shape for testing a `to_sentry` override, drawn from
the parametrized `to_sentry` tests in `tests/exceptions_test.py:108-179`.
(`tests/storage/ltd/client_test.py:209-243` uses the same
direct-inspection assertions for a single override, just not
parametrized.) Copy it, rename
`InvalidFooStateError` and the `foo_*` fields, and place it next to the
module the override lives in (§5 of `SKILL.md` covers placement). The
"three rules" that explain why it's shaped this way live in §5.

```python
"""Tests for the ``to_sentry`` override on ``InvalidFooStateError``."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from docverse.exceptions import InvalidFooStateError


_FOO_STATE_CASES: list[
    tuple[str, Callable[[], InvalidFooStateError], dict[str, str]]
] = [
    (
        "full-transition",
        lambda: InvalidFooStateError(
            current_state="processing",
            target_state="completed",
            foo_public_id="01ABCDEF",
            org_slug="my-org",
        ),
        {
            "org_slug": "my-org",
            "foo_current_state": "processing",
            "foo_target_state": "completed",
        },
    ),
    (
        "partial-context",
        lambda: InvalidFooStateError(
            target_state="processing",
            org_slug="my-org",
        ),
        {
            "org_slug": "my-org",
            "foo_target_state": "processing",
        },
    ),
]


@pytest.mark.parametrize(
    ("case", "factory", "expected_tags"),
    _FOO_STATE_CASES,
    ids=[case for case, _, _ in _FOO_STATE_CASES],
)
def test_invalid_foo_state_to_sentry_tags(
    case: str,
    factory: Callable[[], InvalidFooStateError],
    expected_tags: dict[str, str],
) -> None:
    """``to_sentry`` surfaces API-facing slugs and state names as tags."""
    info = factory().to_sentry()
    assert info.tags == expected_tags
    # High-cardinality fields belong in the context, not in tags.
    assert "foo_public_id" not in info.tags


@pytest.mark.parametrize(
    ("case", "factory", "expected_tags"),
    _FOO_STATE_CASES,
    ids=[case for case, _, _ in _FOO_STATE_CASES],
)
def test_invalid_foo_state_to_sentry_context(
    case: str,
    factory: Callable[[], InvalidFooStateError],
    expected_tags: dict[str, str],
) -> None:
    """``to_sentry`` exposes the full transition snapshot as a context."""
    exc = factory()
    context = exc.to_sentry().contexts["foo_transition"]
    assert context["foo_public_id"] == exc.foo_public_id
    assert context["org_slug"] == exc.org_slug
    assert context["current_state"] == exc.current_state
    assert context["target_state"] == exc.target_state
```
