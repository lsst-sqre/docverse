"""Tests for the MockEditionPublisher."""

from __future__ import annotations

import pytest

from docverse_server.domain.edition_pointer import EditionPointer
from docverse_server.storage.editionpublisher import (
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
            cache_profile="long",
        )
        await pub.publish(
            project_slug="myproject",
            edition_slug="v1",
            build_public_id="DEF456",
            object_key_prefix="myproject/__builds/DEF456/",
            cache_profile="short",
        )

    assert publisher.calls == [
        PublishCall(
            project_slug="myproject",
            edition_slug="main",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
            cache_profile="long",
        ),
        PublishCall(
            project_slug="myproject",
            edition_slug="v1",
            build_public_id="DEF456",
            object_key_prefix="myproject/__builds/DEF456/",
            cache_profile="short",
        ),
    ]


@pytest.mark.asyncio
async def test_implements_protocol() -> None:
    publisher = MockEditionPublisher()
    assert isinstance(publisher, EditionPublisher)


@pytest.mark.asyncio
async def test_publish_is_readable_as_a_pointer() -> None:
    """The mock models the edge, not just the calls made against it."""
    publisher = MockEditionPublisher()
    async with publisher as pub:
        await pub.publish(
            project_slug="myproject",
            edition_slug="main",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
            cache_profile="long",
        )
        pointers = await pub.get_pointers(["myproject/main"])

    assert pointers == {
        "myproject/main": EditionPointer(
            build_public_id="ABC123",
            r2_prefix="myproject/__builds/ABC123/",
            cache_profile="long",
        )
    }


@pytest.mark.asyncio
async def test_unpublish_clears_the_pointer() -> None:
    """An unpublished edition reads back the way an absent one does."""
    publisher = MockEditionPublisher()
    async with publisher as pub:
        await pub.publish(
            project_slug="myproject",
            edition_slug="main",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
            cache_profile="long",
        )
        await pub.unpublish(project_slug="myproject", edition_slug="main")
        pointers = await pub.get_pointers(["myproject/main"])

    assert pointers == {"myproject/main": None}


@pytest.mark.asyncio
async def test_get_pointers_reports_unknown_keys_as_none() -> None:
    """Every requested key is answered, so callers need no default."""
    publisher = MockEditionPublisher()
    async with publisher as pub:
        pointers = await pub.get_pointers(["myproject/main", "other/v1"])

    assert pointers == {"myproject/main": None, "other/v1": None}


@pytest.mark.asyncio
async def test_seeded_pointer_is_readable_and_unrecorded() -> None:
    """Seeded drift is visible to a read but is not a publish call."""
    publisher = MockEditionPublisher()
    publisher.seed_pointer(
        project_slug="myproject",
        edition_slug="main",
        build_public_id="STALE1",
        object_key_prefix="myproject/__builds/STALE1/",
        cache_profile="long",
    )
    async with publisher as pub:
        pointers = await pub.get_pointers(["myproject/main"])

    assert pointers == {
        "myproject/main": EditionPointer(
            build_public_id="STALE1",
            r2_prefix="myproject/__builds/STALE1/",
            cache_profile="long",
        )
    }
    assert publisher.calls == []


@pytest.mark.asyncio
async def test_removed_pointer_is_unrecorded() -> None:
    """A key lost at the edge is not an unpublish the loop performed."""
    publisher = MockEditionPublisher()
    publisher.seed_pointer(
        project_slug="myproject",
        edition_slug="main",
        build_public_id="ABC123",
        object_key_prefix="myproject/__builds/ABC123/",
    )
    publisher.remove_pointer(project_slug="myproject", edition_slug="main")
    async with publisher as pub:
        pointers = await pub.get_pointers(["myproject/main"])

    assert pointers == {"myproject/main": None}
    assert publisher.unpublish_calls == []
