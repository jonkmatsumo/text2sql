"""Cross-provider contract tests for canonical error classification."""

import pytest

from common.models.error_metadata import ErrorCategory
from dal.error_classification import classify_error

_PROVIDERS = (
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
)


@pytest.mark.parametrize(
    "scenario,messages,expected",
    [
        (
            "mutation_blocked",
            {
                "postgres": "permission denied: write blocked by read-only policy",
                "redshift": "permission denied: write blocked by read-only policy",
                "snowflake": "SQL access control error: insufficient privileges for write",
                "bigquery": "access denied: write blocked by read-only policy",
                "athena": "not authorized to execute write statement",
                "databricks": "unauthorized write operation in read-only mode",
                "mysql": "access denied for write operation",
                "sqlite": "access denied: attempt to write a readonly database",
                "duckdb": "permission denied for write operation in read-only mode",
                "clickhouse": "access denied for write operation in readonly mode",
            },
            ErrorCategory.AUTH,
        ),
        (
            "auth_error",
            {
                "postgres": "permission denied for relation users",
                "redshift": "not authorized to access table users",
                "snowflake": "insufficient privileges to operate on schema",
                "bigquery": "access denied: user does not have permission",
                "athena": "unauthorized access",
                "databricks": "not authorized",
                "mysql": "access denied for user",
                "sqlite": "permission denied",
                "duckdb": "permission denied",
                "clickhouse": "access denied",
            },
            ErrorCategory.AUTH,
        ),
        (
            "syntax_error",
            {provider: 'syntax error at or near "FROM"' for provider in _PROVIDERS},
            ErrorCategory.SYNTAX,
        ),
    ],
)
def test_cross_provider_category_contract_for_message_errors(
    scenario: str, messages: dict[str, str], expected: ErrorCategory
) -> None:
    """Equivalent scenarios should normalize to one canonical category."""
    results = {
        provider: classify_error(provider, Exception(messages[provider])) for provider in _PROVIDERS
    }

    assert all(
        category == expected for category in results.values()
    ), f"{scenario} classification drifted across providers: {results}"


@pytest.mark.parametrize("provider", _PROVIDERS)
def test_cross_provider_category_contract_for_timeout(provider: str) -> None:
    """Timeout failures should normalize to TIMEOUT across providers."""
    category = classify_error(provider, TimeoutError(f"{provider} timed out"))
    assert category == ErrorCategory.TIMEOUT
