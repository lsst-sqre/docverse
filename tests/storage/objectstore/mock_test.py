"""Tests for the MockObjectStore."""

from __future__ import annotations

import io

import pytest

from docverse_server.storage.objectstore import MockObjectStore, ObjectEntry


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
async def test_upload_reports_a_single_attempt() -> None:
    """An in-memory write never retries, so it always took one attempt."""
    store = MockObjectStore()
    attempts = await store.upload_object(
        key="test/file.txt", data=b"x", content_type="text/plain"
    )
    assert attempts == 1


@pytest.mark.asyncio
async def test_download_missing_raises() -> None:
    store = MockObjectStore()
    with pytest.raises(KeyError):
        await store.download_object(key="nonexistent")


@pytest.mark.asyncio
async def test_download_object_to_file_writes_the_object() -> None:
    """The streaming download writes the object's bytes and counts them."""
    store = MockObjectStore()
    await store.upload_object(
        key="staging/build.tar.gz", data=b"tarball", content_type="x"
    )
    target = io.BytesIO()

    written = await store.download_object_to_file(
        key="staging/build.tar.gz", fileobj=target
    )

    assert written == len(b"tarball")
    assert target.getvalue() == b"tarball"


@pytest.mark.asyncio
async def test_download_object_to_file_missing_raises() -> None:
    """A missing key raises as ``download_object`` does, writing nothing."""
    store = MockObjectStore()
    target = io.BytesIO()
    with pytest.raises(KeyError):
        await store.download_object_to_file(key="nonexistent", fileobj=target)
    assert target.getvalue() == b""


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
async def test_list_object_entries_reports_each_object_size() -> None:
    """Entries pair each matching key with its stored size, key-sorted."""
    store = MockObjectStore()
    await store.upload_object(
        key="prefix/b.txt", data=b"bbb", content_type="text/plain"
    )
    await store.upload_object(
        key="prefix/a.txt", data=b"a", content_type="text/plain"
    )
    await store.upload_object(
        key="other/c.txt", data=b"cc", content_type="text/plain"
    )

    entries = await store.list_object_entries(prefix="prefix/")

    assert entries == [
        ObjectEntry(key="prefix/a.txt", size=1),
        ObjectEntry(key="prefix/b.txt", size=3),
    ]


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


@pytest.mark.asyncio
async def test_delete_prefix_removes_only_matching_keys() -> None:
    """The tree under the prefix goes; everything else stays."""
    store = MockObjectStore()
    for key in ("builds/01/index.html", "builds/01/a/b.css", "builds/02/x"):
        await store.upload_object(
            key=key, data=b"x", content_type="text/plain"
        )

    deleted = await store.delete_prefix(prefix="builds/01/")

    assert deleted == 2
    assert sorted(store.objects) == ["builds/02/x"]


@pytest.mark.asyncio
async def test_delete_prefix_returns_zero_for_empty_prefix() -> None:
    """A prefix holding nothing is a no-op, not an error."""
    store = MockObjectStore()
    await store.upload_object(
        key="builds/02/x", data=b"x", content_type="text/plain"
    )

    assert await store.delete_prefix(prefix="builds/01/") == 0
    assert sorted(store.objects) == ["builds/02/x"]


@pytest.mark.asyncio
async def test_delete_prefix_refuses_a_blank_prefix() -> None:
    """An empty prefix would mean the whole bucket, so it is refused."""
    store = MockObjectStore()
    await store.upload_object(
        key="builds/02/x", data=b"x", content_type="text/plain"
    )

    with pytest.raises(ValueError, match="empty prefix"):
        await store.delete_prefix(prefix="")

    assert sorted(store.objects) == ["builds/02/x"]
