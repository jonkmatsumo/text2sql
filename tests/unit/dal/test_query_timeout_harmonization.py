"""Tests for provider timeout harmonization and canonical classification."""

import asyncio

import pytest

from common.models.error_metadata import ErrorCategory
from dal.async_utils import with_timeout
from dal.error_classification import classify_error
from dal.util.timeouts import QueryTimeoutError, run_with_timeout


@pytest.mark.asyncio
async def test_run_with_timeout_raises_typed_timeout_and_calls_cancel() -> None:
    """Shared timeout helper should raise a typed timeout and invoke cancellation."""
    cancel_called = False

    async def _slow_operation() -> str:
        await asyncio.sleep(0.05)
        return "ok"

    async def _cancel() -> None:
        nonlocal cancel_called
        cancel_called = True

    with pytest.raises(QueryTimeoutError) as exc_info:
        await run_with_timeout(
            _slow_operation,
            timeout_seconds=0.001,
            cancel=_cancel,
            provider="duckdb",
            operation_name="query.execute",
        )

    err = exc_info.value
    assert cancel_called is True
    assert err.provider == "duckdb"
    assert err.operation_name == "query.execute"
    assert classify_error("duckdb", err) == ErrorCategory.TIMEOUT


@pytest.mark.asyncio
async def test_with_timeout_wrapper_uses_shared_timeout_contract() -> None:
    """Legacy wrapper should preserve timeout semantics via shared helper."""
    cancel_called = False

    async def _cancel() -> None:
        nonlocal cancel_called
        cancel_called = True

    with pytest.raises(QueryTimeoutError) as exc_info:
        await with_timeout(asyncio.sleep(0.05), timeout_seconds=0.001, on_timeout=_cancel)

    assert cancel_called is True
    assert classify_error("athena", exc_info.value) == ErrorCategory.TIMEOUT


@pytest.mark.parametrize(
    "provider",
    [
        "postgres",
        "redshift",
        "snowflake",
        "bigquery",
        "athena",
        "databricks",
        "mysql",
        "sqlite",
        "duckdb",
        "clickhouse",
    ],
)
def test_timeout_classification_is_consistent_across_providers(provider: str) -> None:
    """Canonical classification should normalize provider timeout errors to TIMEOUT."""
    err = QueryTimeoutError(provider=provider, operation_name="query.fetch", timeout_seconds=3.0)
    assert classify_error(provider, err) == ErrorCategory.TIMEOUT
