"""Unit tests for cursor envelope migration registry behavior."""

from __future__ import annotations

import pytest

from dal.pagination_cursor import (
    PAGINATION_CURSOR_KIND_UNSUPPORTED,
    PAGINATION_CURSOR_VERSION_UNSUPPORTED,
    CursorMigrationError,
    CursorMigrationRegistry,
    build_cursor_envelope,
)

pytestmark = pytest.mark.pagination


def test_cursor_registry_migrates_legacy_v0_to_v1_for_known_kind() -> None:
    """Registry should migrate known cursor kinds through explicit v0->v1 path."""
    registry = CursorMigrationRegistry(current_versions={"offset": 1})
    registry.register(
        cursor_kind="offset",
        from_version=0,
        to_version=1,
        migration=lambda payload: {
            **payload,
            "cursor_version": 1,
            "cursor_kind": "offset",
            "migrated": True,
        },
    )
    envelope = build_cursor_envelope(
        raw_payload={"o": 10, "l": 5, "f": "fingerprint-v0"},
        cursor_kind="offset",
        allow_legacy_v0=True,
    )

    migrated = registry.migrate(envelope)

    assert migrated.cursor_version == 1
    assert migrated.cursor_kind == "offset"
    assert migrated.payload["migrated"] is True
    assert migrated.payload["cursor_version"] == 1


def test_cursor_registry_rejects_unknown_version_fail_closed() -> None:
    """Cursor versions newer than the configured current version must fail closed."""
    registry = CursorMigrationRegistry(current_versions={"offset": 1})
    envelope = build_cursor_envelope(
        raw_payload={"cursor_version": 9, "cursor_kind": "offset", "payload": {}},
        cursor_kind="offset",
    )

    with pytest.raises(CursorMigrationError) as exc_info:
        registry.migrate(envelope)

    assert exc_info.value.reason_code == PAGINATION_CURSOR_VERSION_UNSUPPORTED


def test_cursor_registry_rejects_unknown_kind_fail_closed() -> None:
    """Unsupported cursor kinds must fail closed before migration begins."""
    with pytest.raises(CursorMigrationError) as exc_info:
        build_cursor_envelope(
            raw_payload={"cursor_version": 0},
            cursor_kind="unknown-kind",
            allow_legacy_v0=True,
        )

    assert exc_info.value.reason_code == PAGINATION_CURSOR_KIND_UNSUPPORTED
