"""Unit tests for cursor signing secret resolution (fail-closed behavior)."""

from __future__ import annotations

import pytest

from dal.pagination_cursor import (
    PAGINATION_CURSOR_MIN_SECRET_BYTES,
    PAGINATION_CURSOR_SECRET_MISSING,
    PAGINATION_CURSOR_SECRET_WEAK,
    CursorSigningSecretMissing,
    CursorSigningSecrets,
    CursorSigningSecretWeak,
    resolve_cursor_signing_secret,
)

_STRONG_SECRET = "test-pagination-secret-for-unit-tests-2026"


def test_returns_secret_when_configured_with_legacy_env(monkeypatch):
    """Resolver returns a strong legacy secret value."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", _STRONG_SECRET)
    monkeypatch.delenv("PAGINATION_CURSOR_HMAC_SECRET", raising=False)
    result = resolve_cursor_signing_secret()
    assert result == _STRONG_SECRET


def test_returns_secret_when_configured_with_preferred_env(monkeypatch):
    """Resolver accepts the preferred HMAC secret environment variable."""
    monkeypatch.setenv("PAGINATION_CURSOR_HMAC_SECRET", _STRONG_SECRET)
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    result = resolve_cursor_signing_secret()
    assert result == _STRONG_SECRET


def test_preferred_env_takes_precedence_over_legacy(monkeypatch):
    """Preferred env var is used when both are configured."""
    monkeypatch.setenv("PAGINATION_CURSOR_HMAC_SECRET", _STRONG_SECRET + "-preferred")
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", _STRONG_SECRET + "-legacy")
    result = resolve_cursor_signing_secret()
    assert result == _STRONG_SECRET + "-preferred"


def test_strips_whitespace_from_secret(monkeypatch):
    """Resolver strips whitespace from configured secret."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", f"  {_STRONG_SECRET}  ")
    monkeypatch.delenv("PAGINATION_CURSOR_HMAC_SECRET", raising=False)
    result = resolve_cursor_signing_secret()
    assert result == _STRONG_SECRET


def test_raises_when_secret_missing(monkeypatch):
    """Missing secret fails closed with stable reason code."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("PAGINATION_CURSOR_HMAC_SECRET", raising=False)
    with pytest.raises(CursorSigningSecretMissing) as exc_info:
        resolve_cursor_signing_secret()
    assert PAGINATION_CURSOR_SECRET_MISSING in str(exc_info.value)
    assert exc_info.value.reason_code == PAGINATION_CURSOR_SECRET_MISSING


def test_raises_when_secret_empty_string(monkeypatch):
    """Empty secret is treated as missing."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", "")
    monkeypatch.delenv("PAGINATION_CURSOR_HMAC_SECRET", raising=False)
    with pytest.raises(CursorSigningSecretMissing):
        resolve_cursor_signing_secret()


def test_raises_when_secret_whitespace_only(monkeypatch):
    """Whitespace-only secret is treated as missing."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", "   ")
    monkeypatch.delenv("PAGINATION_CURSOR_HMAC_SECRET", raising=False)
    with pytest.raises(CursorSigningSecretMissing):
        resolve_cursor_signing_secret()


def test_raises_when_secret_too_short(monkeypatch):
    """Weak secrets fail closed with stable weak-secret reason code."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", "too-short-secret")
    monkeypatch.delenv("PAGINATION_CURSOR_HMAC_SECRET", raising=False)
    with pytest.raises(CursorSigningSecretWeak) as exc_info:
        resolve_cursor_signing_secret()
    assert PAGINATION_CURSOR_SECRET_WEAK in str(exc_info.value)
    assert exc_info.value.reason_code == PAGINATION_CURSOR_SECRET_WEAK


def test_allow_unsigned_true_does_not_bypass_fail_closed(monkeypatch):
    """Legacy API parameter no longer allows unsigned cursor mode."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("PAGINATION_CURSOR_HMAC_SECRET", raising=False)
    with pytest.raises(CursorSigningSecretMissing):
        resolve_cursor_signing_secret(allow_unsigned=True)


def test_allow_unsigned_false_remains_fail_closed(monkeypatch):
    """Explicit allow_unsigned=False still fails closed when secret missing."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("PAGINATION_CURSOR_HMAC_SECRET", raising=False)
    with pytest.raises(CursorSigningSecretMissing):
        resolve_cursor_signing_secret(allow_unsigned=False)


def test_error_message_does_not_leak_secret_values(monkeypatch):
    """Error messages include env var names but not raw secret values."""
    leaked_secret = "this-should-not-leak"
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", leaked_secret)
    monkeypatch.delenv("PAGINATION_CURSOR_HMAC_SECRET", raising=False)
    with pytest.raises(CursorSigningSecretWeak) as exc_info:
        resolve_cursor_signing_secret()
    msg = str(exc_info.value)
    assert "PAGINATION_CURSOR_HMAC_SECRET" in msg
    assert "PAGINATION_CURSOR_SIGNING_SECRET" in msg
    assert str(PAGINATION_CURSOR_MIN_SECRET_BYTES) in msg
    assert leaked_secret not in msg


def test_resolver_state_from_env_missing(monkeypatch):
    """Shared resolver reports deterministic missing-secret state."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("PAGINATION_CURSOR_HMAC_SECRET", raising=False)
    resolved = CursorSigningSecrets.from_env()
    assert resolved.configured is False
    assert resolved.valid is False
    assert resolved.secret is None
    assert resolved.reason_code == PAGINATION_CURSOR_SECRET_MISSING


def test_resolver_state_from_env_weak(monkeypatch):
    """Shared resolver reports deterministic weak-secret state."""
    monkeypatch.setenv("PAGINATION_CURSOR_HMAC_SECRET", "short-secret")
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    resolved = CursorSigningSecrets.from_env()
    assert resolved.configured is True
    assert resolved.valid is False
    assert resolved.secret is None
    assert resolved.reason_code == PAGINATION_CURSOR_SECRET_WEAK
