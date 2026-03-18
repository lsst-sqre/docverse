"""Tests for the MockObjectStore."""

from __future__ import annotations

import pytest

from docverse.storage.mock_objectstore import MockObjectStore


@pytest.mark.asyncio
async def test_upload_and_download() -> None:
    store = MockObjectStore()
    await store.upload_object(
        key="test/file.txt",
        data=b"hello world",
        content_type="text/plain",
    )
    data = await store.download_object(key="test/file.txt")
    assert data == b"hello world"


@pytest.mark.asyncio
async def test_download_missing_raises() -> None:
    store = MockObjectStore()
    with pytest.raises(KeyError):
        await store.download_object(key="nonexistent")


@pytest.mark.asyncio
async def test_list_objects() -> None:
    store = MockObjectStore()
    await store.upload_object(
        key="prefix/a.txt", data=b"a", content_type="text/plain"
    )
    await store.upload_object(
        key="prefix/b.txt", data=b"b", content_type="text/plain"
    )
    await store.upload_object(
        key="other/c.txt", data=b"c", content_type="text/plain"
    )
    keys = await store.list_objects(prefix="prefix/")
    assert keys == ["prefix/a.txt", "prefix/b.txt"]


@pytest.mark.asyncio
async def test_delete_object() -> None:
    store = MockObjectStore()
    await store.upload_object(
        key="del.txt", data=b"x", content_type="text/plain"
    )
    await store.delete_object(key="del.txt")
    with pytest.raises(KeyError):
        await store.download_object(key="del.txt")


@pytest.mark.asyncio
async def test_delete_nonexistent_is_noop() -> None:
    store = MockObjectStore()
    # Should not raise
    await store.delete_object(key="nope")


@pytest.mark.asyncio
async def test_presigned_upload_url() -> None:
    store = MockObjectStore()
    url = await store.generate_presigned_upload_url(
        key="staging/build.tar.gz",
        content_type="application/gzip",
    )
    assert "staging/build.tar.gz" in url
    assert "application/gzip" in url


@pytest.mark.asyncio
async def test_presigned_download_url() -> None:
    store = MockObjectStore()
    url = await store.generate_presigned_download_url(
        key="staging/build.tar.gz",
    )
    assert "staging/build.tar.gz" in url
