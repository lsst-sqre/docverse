# Constructor pattern template

The shape every server-side exception that overrides `to_sentry`
follows. Copy it, rename `Foo` / `InvalidFooStateError` and the fields
to your exception, and fill in `_format_message`. Verify against
`InvalidBuildStateError` (`src/docverse/exceptions.py:180-204`) when in
doubt. The rationale for each piece — the "five things to copy
verbatim" and the `Literal`-alias rule — lives in §3 of `SKILL.md`.

```python
from typing import override

from safir.slack.sentry import SentryEventInfo

from docverse.exceptions import DocverseSlackException


class InvalidFooStateError(DocverseSlackException):
    """One-line summary of what this error means.

    Carries the foo's API-facing identifiers (...) so a Sentry
    triager can paste them straight into a ``GET /...`` URL without
    translating an internal row id. Construct with the structured
    kwargs; ``message`` defaults to a useful summary when omitted.
    """

    def __init__(
        self,
        *,
        # Identifying fields — every one optional, every one
        # defaulting to ``None`` so partial-context raises work.
        current_state: str | None = None,
        target_state: str | None = None,
        foo_public_id: str | None = None,
        org_slug: str | None = None,
        message: str | None = None,
    ) -> None:
        if message is None:
            message = self._format_message(
                current_state=current_state,
                target_state=target_state,
                foo_public_id=foo_public_id,
            )
        super().__init__(message)
        self.current_state = current_state
        self.target_state = target_state
        self.foo_public_id = foo_public_id
        self.org_slug = org_slug

    @override
    def to_sentry(self) -> SentryEventInfo:
        info = super().to_sentry()
        if self.org_slug is not None:
            info.tags["org_slug"] = self.org_slug
        if self.current_state is not None:
            info.tags["foo_current_state"] = self.current_state
        if self.target_state is not None:
            info.tags["foo_target_state"] = self.target_state
        info.contexts["foo_transition"] = {
            "foo_public_id": self.foo_public_id,
            "org_slug": self.org_slug,
            "current_state": self.current_state,
            "target_state": self.target_state,
        }
        return info

    @staticmethod
    def _format_message(
        *,
        current_state: str | None,
        target_state: str | None,
        foo_public_id: str | None,
    ) -> str:
        ...
```
