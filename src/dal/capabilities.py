from dataclasses import dataclass
from typing import Literal, Optional

TenantEnforcementMode = Literal["rls_session", "sql_rewrite", "unsupported"]


@dataclass(frozen=True)
class BackendCapabilities:
    """Capability flags for query-target backends."""

    provider_name: str = "unspecified"
    supports_tenant_enforcement: bool = False
    tenant_enforcement_mode: TenantEnforcementMode = "unsupported"
    supports_db_readonly_session: bool = False
    notes: Optional[str] = None
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
    supports_execution_role: bool = False
    supports_restricted_session: bool = False
    supports_row_cap: bool = True
    supports_timeout: bool = True
    supports_byte_cap: bool = True
    # Does DAL apply defensive statement-level read-only guard?
    enforces_statement_read_only: bool = False

    @property
    def supports_session_read_only(self) -> bool:
        """Backward-compatible alias for `supports_db_readonly_session`."""
        return self.supports_db_readonly_session


PAGINATION_PROVIDERS: set[str] = set()


def capabilities_for_provider(provider: str) -> BackendCapabilities:
    """Return capability flags for a given query-target provider."""
    normalized = (provider or "").strip().lower()
    supports_pagination = normalized in PAGINATION_PROVIDERS
    if normalized == "redshift":
        return BackendCapabilities(
            provider_name="redshift",
            execution_model="sync",
            supports_pagination=supports_pagination,
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_schema_cache=False,
            supports_db_readonly_session=True,
            enforces_statement_read_only=True,
        )
    if normalized == "mysql":
        return BackendCapabilities(
            provider_name="mysql",
            execution_model="sync",
            supports_pagination=supports_pagination,
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=True,
            supports_fk_enforcement=False,
            supports_schema_cache=True,
            supports_db_readonly_session=True,
        )
    if normalized == "postgres":
        return BackendCapabilities(
            provider_name="postgres",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="rls_session",
            execution_model="sync",
            supports_cancel=True,
            supports_pagination=supports_pagination,
            supports_db_readonly_session=True,
            supports_execution_role=True,
            supports_restricted_session=True,
        )
    if normalized == "sqlite":
        return BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            notes="Tenant enforcement uses SQL rewrite v1.",
            execution_model="sync",
            supports_cancel=True,
            supports_pagination=supports_pagination,
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=True,
            supports_fk_enforcement=False,
            supports_schema_cache=True,
        )
    if normalized == "snowflake":
        return BackendCapabilities(
            provider_name="snowflake",
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
            provider_name="bigquery",
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
            provider_name="athena",
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
            provider_name="databricks",
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
            provider_name="cockroachdb",
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
            provider_name="duckdb",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            notes="Tenant enforcement uses SQL rewrite v1.",
            execution_model="sync",
            supports_cancel=True,
            supports_pagination=supports_pagination,
            supports_arrays=True,
            supports_json_ops=True,
            supports_transactions=True,
            supports_fk_enforcement=False,
            supports_schema_cache=True,
        )
    if normalized == "clickhouse":
        return BackendCapabilities(
            provider_name="clickhouse",
            execution_model="sync",
            supports_pagination=supports_pagination,
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
            supports_schema_cache=False,
        )
    return BackendCapabilities(
        provider_name=normalized or "unspecified",
        supports_row_cap=False,
        supports_timeout=False,
        supports_byte_cap=False,
    )
