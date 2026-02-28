"""Unit tests for cursor signing secret resolution (fail-closed behavior)."""

from __future__ import annotations

import pytest

from dal.pagination_cursor import (
    PAGINATION_CURSOR_SECRET_MISSING,
    CursorSigningSecretMissing,
    resolve_cursor_signing_secret,
)


def test_returns_secret_when_configured(monkeypatch):
    """Resolver returns the configured signing secret."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", "prod-secret-value")
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)
    result = resolve_cursor_signing_secret()
    assert result == "prod-secret-value"


def test_strips_whitespace_from_secret(monkeypatch):
    """Resolver strips whitespace from configured secret."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", "  my-secret  ")
    result = resolve_cursor_signing_secret()
    assert result == "my-secret"


def test_raises_when_secret_missing_and_insecure_not_allowed(monkeypatch):
    """Missing secret with insecure mode disabled fails closed."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)
    with pytest.raises(CursorSigningSecretMissing) as exc_info:
        resolve_cursor_signing_secret()
    assert PAGINATION_CURSOR_SECRET_MISSING in str(exc_info.value)
    assert exc_info.value.reason_code == PAGINATION_CURSOR_SECRET_MISSING


def test_raises_when_secret_empty_string(monkeypatch):
    """Empty string secret is treated as missing."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", "")
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)
    with pytest.raises(CursorSigningSecretMissing):
        resolve_cursor_signing_secret()


def test_raises_when_secret_whitespace_only(monkeypatch):
    """Whitespace-only secret is treated as missing."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", "   ")
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)
    with pytest.raises(CursorSigningSecretMissing):
        resolve_cursor_signing_secret()


def test_returns_none_when_insecure_flag_true(monkeypatch):
    """Insecure dev mode returns None for signing-disabled path."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.setenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", "true")
    result = resolve_cursor_signing_secret()
    assert result is None


def test_returns_none_when_insecure_flag_1(monkeypatch):
    """Insecure flag value '1' is accepted as truthy."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.setenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", "1")
    result = resolve_cursor_signing_secret()
    assert result is None


def test_raises_when_insecure_flag_false(monkeypatch):
    """Insecure flag value 'false' does not opt out of fail-closed."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.setenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", "false")
    with pytest.raises(CursorSigningSecretMissing):
        resolve_cursor_signing_secret()


def test_raises_when_insecure_flag_empty(monkeypatch):
    """Insecure flag empty string does not opt out of fail-closed."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.setenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", "")
    with pytest.raises(CursorSigningSecretMissing):
        resolve_cursor_signing_secret()


def test_allow_unsigned_parameter_overrides_env(monkeypatch):
    """Explicit allow_unsigned=True overrides missing env var."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)
    result = resolve_cursor_signing_secret(allow_unsigned=True)
    assert result is None


def test_allow_unsigned_false_overrides_env_true(monkeypatch):
    """Explicit allow_unsigned=False overrides env var set to true."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.setenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", "true")
    with pytest.raises(CursorSigningSecretMissing):
        resolve_cursor_signing_secret(allow_unsigned=False)


def test_secret_present_takes_precedence_over_insecure_flag(monkeypatch):
    """Configured secret is returned even when insecure flag is set."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", "real-secret")
    monkeypatch.setenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", "true")
    result = resolve_cursor_signing_secret()
    assert result == "real-secret"


def test_error_message_does_not_leak_secrets(monkeypatch):
    """Error message includes env var names but not leaked values."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)
    with pytest.raises(CursorSigningSecretMissing) as exc_info:
        resolve_cursor_signing_secret()
    msg = str(exc_info.value)
    assert "PAGINATION_CURSOR_SIGNING_SECRET" in msg
    assert "PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET" in msg
