import re

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

    revoked = registry.revoke(session.session_id)
    loaded = registry.get(session.session_id)

    assert revoked is not None
    assert revoked.is_revoked is True
    assert loaded is not None
    assert loaded.is_revoked is True
