import re
from dataclasses import replace

import pytest

from dal.pagination_session import (
    PAGINATION_SESSION_ID_MAX_LENGTH,
    PAGINATION_SESSION_ID_MIN_LENGTH,
    InMemoryPaginationSessionRegistry,
    create_pagination_session,
)

pytestmark = pytest.mark.pagination


def test_create_pagination_session_issues_bounded_opaque_id() -> None:
    """Session minting should return an opaque id with bounded length/charset."""
    session = create_pagination_session(
        tenant_id="tenant-a",
        provider_name="postgres",
        pagination_mode="offset",
        query_scope_fp="query-fingerprint-1",
        policy_snapshot_fp="policy-fingerprint-1",
        revocation_epoch=7,
        now_epoch_milliseconds=1_700_000_000_000,
    )

    assert (
        PAGINATION_SESSION_ID_MIN_LENGTH
        <= len(session.session_id)
        <= PAGINATION_SESSION_ID_MAX_LENGTH
    )
    assert re.fullmatch(r"[A-Za-z0-9_-]+", session.session_id)
    assert "tenant-a" not in session.session_id
    assert session.created_at_ms == 1_700_000_000_000


def test_in_memory_pagination_session_registry_roundtrip() -> None:
    """Registry should preserve session fields on put/get roundtrip."""
    now_state = {"value": 1_700_000_000_000}
    registry = InMemoryPaginationSessionRegistry(now_ms=lambda: now_state["value"])
    session = create_pagination_session(
        tenant_id="tenant-42",
        provider_name="postgres",
        pagination_mode="keyset",
        query_scope_fp="scope-fp-2",
        policy_snapshot_fp="policy-fp-2",
        revocation_epoch=3,
        now_epoch_milliseconds=now_state["value"],
    )

    registry.put(session)
    loaded = registry.get(session.session_id)

    assert loaded == session


def test_revoke_marks_session_revoked_and_subsequent_get_reads_revoked() -> None:
    """Revoked sessions should remain retrievable as revoked."""
    now_state = {"value": 1_700_000_000_000}
    registry = InMemoryPaginationSessionRegistry(now_ms=lambda: now_state["value"])
    session = create_pagination_session(
        tenant_id="tenant-99",
        provider_name="postgres",
        pagination_mode="offset",
        query_scope_fp="scope-fp-9",
        policy_snapshot_fp="policy-fp-9",
        revocation_epoch=11,
        now_epoch_milliseconds=now_state["value"],
    )
    registry.put(session)

    revoked = registry.revoke_session(session.session_id)
    loaded = registry.get(session.session_id)

    assert revoked is not None
    assert revoked.is_revoked is True
    assert loaded is not None
    assert loaded.is_revoked is True


def test_record_access_updates_bounded_audit_fields_deterministically() -> None:
    """Successful continuation access should increment pages and update last-access time."""
    now_state = {"value": 1_700_000_000_000}
    registry = InMemoryPaginationSessionRegistry(now_ms=lambda: now_state["value"])
    session = create_pagination_session(
        tenant_id="tenant-10",
        provider_name="postgres",
        pagination_mode="offset",
        query_scope_fp="scope-fp-10",
        policy_snapshot_fp="policy-fp-10",
        revocation_epoch=0,
        now_epoch_milliseconds=now_state["value"],
    )
    registry.put(session)

    now_state["value"] = 1_700_000_000_150
    first = registry.record_access(session.session_id)
    now_state["value"] = 1_700_000_000_300
    second = registry.record_access(session.session_id)

    assert first is not None
    assert first.pages_served_count == 1
    assert first.last_accessed_at_ms == 1_700_000_000_150
    assert second is not None
    assert second.pages_served_count == 2
    assert second.last_accessed_at_ms == 1_700_000_000_300


def test_record_access_returns_none_for_revoked_session() -> None:
    """Revoked sessions must not advance audit counters on access attempts."""
    now_state = {"value": 1_700_000_000_000}
    registry = InMemoryPaginationSessionRegistry(now_ms=lambda: now_state["value"])
    session = create_pagination_session(
        tenant_id="tenant-11",
        provider_name="postgres",
        pagination_mode="keyset",
        query_scope_fp="scope-fp-11",
        policy_snapshot_fp="policy-fp-11",
        revocation_epoch=0,
        now_epoch_milliseconds=now_state["value"],
    )
    registry.put(session)
    registry.revoke_session(session.session_id)
    now_state["value"] = 1_700_000_000_200

    updated = registry.record_access(session.session_id)
    loaded = registry.get(session.session_id)

    assert updated is None
    assert loaded is not None
    assert loaded.is_revoked is True
    assert loaded.pages_served_count == 0
    assert loaded.last_accessed_at_ms is None


def test_record_access_updates_bounded_row_size_estimate_after_successful_page() -> None:
    """Successful continuation pages should update rolling row-size estimate deterministically."""
    now_state = {"value": 1_700_000_000_000}
    registry = InMemoryPaginationSessionRegistry(now_ms=lambda: now_state["value"])
    session = create_pagination_session(
        tenant_id="tenant-12",
        provider_name="postgres",
        pagination_mode="offset",
        query_scope_fp="scope-fp-12",
        policy_snapshot_fp="policy-fp-12",
        revocation_epoch=0,
        now_epoch_milliseconds=now_state["value"],
    )
    registry.put(session)

    now_state["value"] = 1_700_000_000_100
    first = registry.record_access(
        session.session_id,
        page_row_count=2,
        page_bytes=200,
    )
    now_state["value"] = 1_700_000_000_200
    second = registry.record_access(
        session.session_id,
        page_row_count=4,
        page_bytes=600,
    )

    assert first is not None
    assert first.avg_row_bytes_estimate == 100
    assert first.last_page_row_count == 2
    assert first.last_page_bytes == 200
    assert second is not None
    # Weighted update: (100*3 + 150) // 4 = 112
    assert second.avg_row_bytes_estimate == 112
    assert second.last_page_row_count == 4
    assert second.last_page_bytes == 600


def test_record_access_on_rejected_session_does_not_mutate_row_size_estimate() -> None:
    """Rejected continuation attempts must not mutate rolling row-size state."""
    now_state = {"value": 1_700_000_000_000}
    registry = InMemoryPaginationSessionRegistry(now_ms=lambda: now_state["value"])
    base_session = create_pagination_session(
        tenant_id="tenant-13",
        provider_name="postgres",
        pagination_mode="keyset",
        query_scope_fp="scope-fp-13",
        policy_snapshot_fp="policy-fp-13",
        revocation_epoch=0,
        now_epoch_milliseconds=now_state["value"],
    )
    seeded_session = replace(
        base_session,
        avg_row_bytes_estimate=120,
        last_page_row_count=3,
        last_page_bytes=360,
    )
    registry.put(seeded_session)
    registry.revoke_session(seeded_session.session_id)

    updated = registry.record_access(
        seeded_session.session_id,
        page_row_count=10,
        page_bytes=10_000,
    )
    loaded = registry.get(seeded_session.session_id)

    assert updated is None
    assert loaded is not None
    assert loaded.avg_row_bytes_estimate == 120
    assert loaded.last_page_row_count == 3
    assert loaded.last_page_bytes == 360


def test_registry_ttl_expiry_evicts_stale_session() -> None:
    """Registry get should return None once a session ages beyond ttl_ms."""
    now_state = {"value": 1_700_000_000_000}
    registry = InMemoryPaginationSessionRegistry(ttl_ms=100, now_ms=lambda: now_state["value"])
    session = create_pagination_session(
        tenant_id="tenant-ttl",
        provider_name="postgres",
        pagination_mode="offset",
        query_scope_fp="scope-fp-ttl",
        policy_snapshot_fp="policy-fp-ttl",
        revocation_epoch=0,
        now_epoch_milliseconds=now_state["value"],
    )
    registry.put(session)

    now_state["value"] = 1_700_000_000_050
    assert registry.get(session.session_id) is not None
    now_state["value"] = 1_700_000_000_101
    assert registry.get(session.session_id) is None


def test_registry_capacity_eviction_removes_oldest_session() -> None:
    """Bounded max_entries should evict least-recently-used sessions."""
    now_state = {"value": 1_700_000_000_000}
    registry = InMemoryPaginationSessionRegistry(max_entries=1, now_ms=lambda: now_state["value"])
    session_one = create_pagination_session(
        tenant_id="tenant-evict-1",
        provider_name="postgres",
        pagination_mode="offset",
        query_scope_fp="scope-fp-evict-1",
        policy_snapshot_fp="policy-fp-evict-1",
        revocation_epoch=0,
        now_epoch_milliseconds=now_state["value"],
    )
    registry.put(session_one)

    now_state["value"] = 1_700_000_000_001
    session_two = create_pagination_session(
        tenant_id="tenant-evict-2",
        provider_name="postgres",
        pagination_mode="offset",
        query_scope_fp="scope-fp-evict-2",
        policy_snapshot_fp="policy-fp-evict-2",
        revocation_epoch=0,
        now_epoch_milliseconds=now_state["value"],
    )
    registry.put(session_two)

    assert registry.get(session_one.session_id) is None
    assert registry.get(session_two.session_id) is not None
