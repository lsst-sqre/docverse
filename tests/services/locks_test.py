"""Unit tests for ``docverse.services.lock_service``.

These tests pin the lock-id computation to hard-coded expected
integers so that two worker replicas computing the same
``LockKey`` always agree — a regression in the hashing scheme (wrong
``digest_size``, wrong join separator, forgotten class prefix) would
otherwise only be caught by the integration tests at runtime.
"""

from __future__ import annotations

from docverse.services.lock_service import (
    LockClass,
    LockKey,
    compute_lock_id,
)

SIGNED_INT64_MIN = -(1 << 63)
SIGNED_INT64_MAX = (1 << 63) - 1


def _high_16_bits(lock_id: int) -> int:
    """Return the high 16 bits of a signed 64-bit lock id."""
    unsigned = lock_id if lock_id >= 0 else lock_id + (1 << 64)
    return (unsigned >> 48) & 0xFFFF


def test_compute_lock_id_is_deterministic() -> None:
    """Hard-coded expected ids pin the hashing scheme across runs."""
    build_id = compute_lock_id(
        LockClass.BUILD_PROCESSING,
        org_id=1,
        project_id=2,
        git_ref="main",
    )
    edition_id = compute_lock_id(
        LockClass.EDITION_UPDATE,
        org_id=1,
        project_id=2,
        edition_id=3,
    )
    project_id = compute_lock_id(
        LockClass.PROJECT,
        org_id=1,
        project_id=2,
    )

    assert build_id == 136049745188599
    assert edition_id == 510729122043644
    assert project_id == 792169873651098


def test_lock_key_constructors_match_compute_lock_id() -> None:
    """``LockKey`` constructors delegate to ``compute_lock_id``."""
    assert LockKey.for_build_processing(1, 2, "main").lock_id == (
        136049745188599
    )
    assert LockKey.for_edition_update(1, 2, 3).lock_id == (
        510729122043644
    )
    assert LockKey.for_project(1, 2).lock_id == 792169873651098


def test_high_16_bits_match_lock_class() -> None:
    """Every constructor tags the high 16 bits with its class value."""
    build_key = LockKey.for_build_processing(
        org_id=42, project_id=99, git_ref="release/v1"
    )
    edition_key = LockKey.for_edition_update(
        org_id=42, project_id=99, edition_id=7
    )
    project_key = LockKey.for_project(org_id=42, project_id=99)

    assert _high_16_bits(build_key.lock_id) == (
        LockClass.BUILD_PROCESSING.value
    )
    assert _high_16_bits(edition_key.lock_id) == (
        LockClass.EDITION_UPDATE.value
    )
    assert _high_16_bits(project_key.lock_id) == LockClass.PROJECT.value


def test_different_classes_with_same_fields_differ() -> None:
    """Class prefix prevents cross-class lock-id collisions."""
    # Same underlying (org_id, project_id) fields — different classes.
    build = compute_lock_id(
        LockClass.BUILD_PROCESSING, org_id=1, project_id=2
    )
    edition = compute_lock_id(
        LockClass.EDITION_UPDATE, org_id=1, project_id=2
    )
    project = compute_lock_id(
        LockClass.PROJECT, org_id=1, project_id=2
    )

    assert build != edition
    assert build != project
    assert edition != project


def test_result_fits_in_signed_int64() -> None:
    """Results never overflow Postgres ``bigint``."""
    samples = [
        LockKey.for_build_processing(1, 2, "main").lock_id,
        LockKey.for_build_processing(
            2**31 - 1, 2**31 - 1, "x" * 256
        ).lock_id,
        LockKey.for_edition_update(1, 2, 3).lock_id,
        LockKey.for_edition_update(
            2**31 - 1, 2**31 - 1, 2**31 - 1
        ).lock_id,
        LockKey.for_project(1, 2).lock_id,
        LockKey.for_project(2**31 - 1, 2**31 - 1).lock_id,
    ]
    for value in samples:
        assert SIGNED_INT64_MIN <= value <= SIGNED_INT64_MAX


def test_lock_key_carries_label_and_class() -> None:
    """``LockKey`` exposes a human label and its ``LockClass``."""
    key = LockKey.for_edition_update(
        org_id=1, project_id=2, edition_id=3
    )
    assert key.lock_class is LockClass.EDITION_UPDATE
    assert "edition" in key.label
    assert "1" in key.label and "2" in key.label and "3" in key.label


def test_build_processing_differs_across_git_refs() -> None:
    """Two refs on the same project produce distinct lock ids."""
    main = LockKey.for_build_processing(1, 2, "main").lock_id
    release = LockKey.for_build_processing(
        1, 2, "release/v1"
    ).lock_id
    assert main != release
