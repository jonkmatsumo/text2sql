from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class BackendCapabilities:
    """Capability flags for query-target backends."""

    execution_model: Literal["sync", "async"] = "sync"
    supports_column_metadata: bool = True
    supports_cancel: bool = False
    supports_pagination: bool = False
    supports_arrays: bool = True
    supports_json_ops: bool = True
    supports_transactions: bool = True
    supports_fk_enforcement: bool = True
    supports_cost_estimation: bool = False
    supports_schema_cache: bool = False
    # Can provider/session enforce DB-level read-only mode?
    supports_session_read_only: bool = False
    # Does DAL apply defensive statement-level read-only guard?
    enforces_statement_read_only: bool = False


PAGINATION_PROVIDERS: set[str] = set()


def capabilities_for_provider(provider: str) -> BackendCapabilities:
    """Return capability flags for a given query-target provider."""
    normalized = provider.lower()
    supports_pagination = normalized in PAGINATION_PROVIDERS
    if normalized == "redshift":
        return BackendCapabilities(
            execution_model="sync",
            supports_pagination=supports_pagination,
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_schema_cache=False,
            supports_session_read_only=True,
            enforces_statement_read_only=True,
        )
    if normalized == "mysql":
        return BackendCapabilities(
            execution_model="sync",
            supports_pagination=supports_pagination,
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=True,
            supports_fk_enforcement=False,
            supports_schema_cache=True,
            supports_session_read_only=True,
        )
    if normalized == "postgres":
        return BackendCapabilities(
            execution_model="sync",
            supports_cancel=True,
            supports_pagination=supports_pagination,
            supports_session_read_only=True,
        )
    if normalized == "sqlite":
        return BackendCapabilities(
            execution_model="sync",
            supports_cancel=True,
            supports_pagination=supports_pagination,
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=True,
            supports_fk_enforcement=False,
            supports_schema_cache=True,
            supports_session_read_only=True,
        )
    if normalized == "snowflake":
        return BackendCapabilities(
            execution_model="async",
            supports_cancel=True,
            supports_pagination=supports_pagination,
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
            enforces_statement_read_only=True,
        )
    if normalized == "bigquery":
        return BackendCapabilities(
            execution_model="async",
            supports_cancel=True,
            supports_pagination=supports_pagination,
            supports_arrays=True,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_cost_estimation=True,
            supports_schema_cache=False,
            enforces_statement_read_only=True,
        )
    if normalized == "athena":
        return BackendCapabilities(
            execution_model="async",
            supports_cancel=True,
            supports_pagination=supports_pagination,
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_schema_cache=False,
        )
    if normalized == "databricks":
        return BackendCapabilities(
            execution_model="async",
            supports_cancel=True,
            supports_pagination=supports_pagination,
            supports_arrays=True,
            supports_json_ops=True,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_schema_cache=False,
        )
    if normalized == "cockroachdb":
        return BackendCapabilities(
            execution_model="sync",
            supports_pagination=supports_pagination,
            supports_arrays=True,
            supports_json_ops=True,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_schema_cache=False,
        )
    if normalized == "duckdb":
        return BackendCapabilities(
            execution_model="sync",
            supports_cancel=True,
            supports_pagination=supports_pagination,
            supports_arrays=True,
            supports_json_ops=True,
            supports_transactions=True,
            supports_fk_enforcement=False,
            supports_schema_cache=True,
            supports_session_read_only=True,
        )
    if normalized == "clickhouse":
        return BackendCapabilities(
            execution_model="sync",
            supports_pagination=supports_pagination,
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_schema_cache=False,
        )
    return BackendCapabilities()
