"""Tests for the MockEditionPublisher."""

from __future__ import annotations

import pytest

from docverse.storage.editionpublisher import (
    EditionPublisher,
    MockEditionPublisher,
    PublishCall,
)


@pytest.mark.asyncio
async def test_records_publish_calls() -> None:
    publisher = MockEditionPublisher()
    async with publisher as pub:
        await pub.publish(
            project_slug="myproject",
            edition_slug="main",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
        )
        await pub.publish(
            project_slug="myproject",
            edition_slug="v1",
            build_public_id="DEF456",
            object_key_prefix="myproject/__builds/DEF456/",
        )

    assert publisher.calls == [
        PublishCall(
            project_slug="myproject",
            edition_slug="main",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
        ),
        PublishCall(
            project_slug="myproject",
            edition_slug="v1",
            build_public_id="DEF456",
            object_key_prefix="myproject/__builds/DEF456/",
        ),
    ]


@pytest.mark.asyncio
async def test_implements_protocol() -> None:
    publisher = MockEditionPublisher()
    assert isinstance(publisher, EditionPublisher)
