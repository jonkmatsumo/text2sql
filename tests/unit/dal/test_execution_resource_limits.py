"""Tests for unified execution resource limit settings."""

import pytest

from dal.execution_resource_limits import ExecutionResourceLimits


def test_execution_resource_limits_defaults(monkeypatch):
    """Defaults should load to a safe, enforcing posture."""
    monkeypatch.delenv("EXECUTION_RESOURCE_MAX_ROWS", raising=False)
    monkeypatch.delenv("EXECUTION_RESOURCE_MAX_BYTES", raising=False)
    monkeypatch.delenv("EXECUTION_RESOURCE_MAX_EXECUTION_MS", raising=False)
    monkeypatch.delenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", raising=False)
    monkeypatch.delenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", raising=False)
    monkeypatch.delenv("EXECUTION_RESOURCE_ENFORCE_TIMEOUT", raising=False)

    limits = ExecutionResourceLimits.from_env()
    assert limits.enforce_row_limit is True
    assert limits.enforce_byte_limit is True
    assert limits.enforce_timeout is True
    assert limits.max_rows > 0
    assert limits.max_bytes > 0
    assert limits.max_execution_ms > 0


@pytest.mark.parametrize(
    "env_name, env_value, expected",
    [
        ("EXECUTION_RESOURCE_MAX_ROWS", "0", "EXECUTION_RESOURCE_MAX_ROWS > 0"),
        ("EXECUTION_RESOURCE_MAX_BYTES", "0", "EXECUTION_RESOURCE_MAX_BYTES > 0"),
        ("EXECUTION_RESOURCE_MAX_EXECUTION_MS", "0", "EXECUTION_RESOURCE_MAX_EXECUTION_MS > 0"),
    ],
)
def test_execution_resource_limits_fail_closed_on_invalid_values(
    monkeypatch, env_name: str, env_value: str, expected: str
):
    """Validation should fail closed when enforcing flags have invalid positive bounds."""
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_TIMEOUT", "true")
    monkeypatch.setenv(env_name, env_value)

    with pytest.raises(ValueError, match=expected):
        ExecutionResourceLimits.from_env()
