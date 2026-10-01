"""Spy on aiobotocore session construction.

Each ``AioSession`` parses botocore's S3 service model and endpoint
ruleset on first use, so a process that builds one per build copy churns
megabytes of short-lived objects per copy (PRD #753). Tests that pin the
one-session-per-process wiring count constructions with
:func:`record_aiobotocore_sessions`.
"""

from __future__ import annotations

from typing import Any

import pytest
from aiobotocore.session import AioSession

__all__ = ["record_aiobotocore_sessions"]


def record_aiobotocore_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> list[AioSession]:
    """Record every ``AioSession`` constructed until the test ends.

    The spy wraps the constructor that ``aiobotocore.session.get_session``
    calls rather than ``get_session`` itself: modules import
    ``get_session`` by name, so patching the function would miss every
    alias bound before the patch, while every session, however it is
    obtained, passes through ``AioSession.__init__``.

    Returns
    -------
    list of AioSession
        The sessions constructed since the call, in construction order.
        The list fills as the test runs.
    """
    created: list[AioSession] = []
    original_init = AioSession.__init__

    def _recording_init(self: AioSession, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        created.append(self)

    monkeypatch.setattr(AioSession, "__init__", _recording_init)
    return created
