from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class BackendCapabilities:
    """Capability flags for query-target backends."""

    execution_model: Literal["sync", "async"] = "sync"
    supports_arrays: bool = True
    supports_json_ops: bool = True
    supports_transactions: bool = True
    supports_fk_enforcement: bool = True
    supports_cost_estimation: bool = False
    supports_schema_cache: bool = False


def capabilities_for_provider(provider: str) -> BackendCapabilities:
    """Return capability flags for a given query-target provider."""
    normalized = provider.lower()
    if normalized == "redshift":
        return BackendCapabilities(
            execution_model="sync",
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_schema_cache=False,
        )
    if normalized == "mysql":
        return BackendCapabilities(
            execution_model="sync",
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=True,
            supports_fk_enforcement=False,
            supports_schema_cache=True,
        )
    if normalized == "sqlite":
        return BackendCapabilities(
            execution_model="sync",
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=True,
            supports_fk_enforcement=False,
            supports_schema_cache=True,
        )
    if normalized == "snowflake":
        return BackendCapabilities(
            execution_model="async",
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
        )
    if normalized == "bigquery":
        return BackendCapabilities(
            execution_model="async",
            supports_arrays=True,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_cost_estimation=True,
            supports_schema_cache=False,
        )
    if normalized == "athena":
        return BackendCapabilities(
            execution_model="async",
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_schema_cache=False,
        )
    if normalized == "databricks":
        return BackendCapabilities(
            execution_model="async",
            supports_arrays=True,
            supports_json_ops=True,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_schema_cache=False,
        )
    if normalized == "cockroachdb":
        return BackendCapabilities(
            execution_model="sync",
            supports_arrays=True,
            supports_json_ops=True,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_schema_cache=False,
        )
    if normalized == "duckdb":
        return BackendCapabilities(
            execution_model="sync",
            supports_arrays=True,
            supports_json_ops=True,
            supports_transactions=True,
            supports_fk_enforcement=False,
            supports_schema_cache=True,
        )
    if normalized == "clickhouse":
        return BackendCapabilities(
            execution_model="sync",
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_schema_cache=False,
        )
    return BackendCapabilities()
