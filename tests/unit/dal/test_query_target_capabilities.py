import pytest

from dal.capabilities import PAGINATION_PROVIDERS, BackendCapabilities, capabilities_for_provider
from dal.util.env import PROVIDER_ALIASES

# All known providers that should be registered in capabilities
QUERY_TARGET_ALLOWED = {value for value in PROVIDER_ALIASES.values() if value != "memgraph"}
KNOWN_PROVIDERS = set(QUERY_TARGET_ALLOWED)

# Async warehouse providers (execution_model == "async")
ASYNC_PROVIDERS = {"snowflake", "bigquery", "athena", "databricks"}

# Providers without transaction support
NO_TRANSACTION_PROVIDERS = {
    "snowflake",
    "redshift",
    "bigquery",
    "athena",
    "databricks",
    "cockroachdb",
    "clickhouse",
}


class TestCapabilitiesCompleteness:
    """Verify capabilities_for_provider covers all known providers."""

    def test_all_providers_return_capabilities(self):
        """Every known provider should return a BackendCapabilities instance."""
        for provider in KNOWN_PROVIDERS:
            caps = capabilities_for_provider(provider)
            assert isinstance(
                caps, BackendCapabilities
            ), f"{provider} did not return BackendCapabilities"

    def test_known_providers_match_query_target_allowlist(self):
        """Known provider set should match query-target allowlist."""
        assert KNOWN_PROVIDERS == QUERY_TARGET_ALLOWED

    def test_unknown_provider_returns_defaults(self):
        """Unknown providers should return safe default capability flags."""
        caps = capabilities_for_provider("unknown-provider")
        assert isinstance(caps, BackendCapabilities)
        assert caps.provider_name == "unknown-provider"
        assert caps.execution_model == "sync"
        assert caps.supports_arrays is True
        assert caps.tenant_enforcement_mode == "unsupported"

    @pytest.mark.parametrize("provider", KNOWN_PROVIDERS)
    def test_known_providers_expose_stable_provider_name(self, provider: str):
        """Provider identity should be stable and normalized in capability payloads."""
        caps = capabilities_for_provider(provider)
        assert caps.provider_name == provider

    @pytest.mark.parametrize("provider", ASYNC_PROVIDERS)
    def test_async_providers_have_async_execution_model(self, provider: str):
        """Warehouses using job-based execution should report async model."""
        caps = capabilities_for_provider(provider)
        assert caps.execution_model == "async", f"{provider} should be async"

    @pytest.mark.parametrize("provider", KNOWN_PROVIDERS - ASYNC_PROVIDERS)
    def test_sync_providers_have_sync_execution_model(self, provider: str):
        """OLTP/embedded providers should report sync model."""
        caps = capabilities_for_provider(provider)
        assert caps.execution_model == "sync", f"{provider} should be sync"

    @pytest.mark.parametrize("provider", NO_TRANSACTION_PROVIDERS)
    def test_no_transaction_providers(self, provider: str):
        """Providers without ACID transactions should report supports_transactions=False."""
        caps = capabilities_for_provider(provider)
        assert caps.supports_transactions is False, f"{provider} should not support transactions"


class TestProviderSpecificCapabilities:
    """Test specific capability combinations for each provider."""

    def test_postgres_capabilities_defaults(self):
        """Ensure Postgres defaults include full capability support."""
        caps = capabilities_for_provider("postgres")
        assert caps.execution_model == "sync"
        assert caps.supports_arrays is True
        assert caps.supports_json_ops is True
        assert caps.supports_transactions is True
        assert caps.supports_fk_enforcement is True
        assert caps.supports_cost_estimation is False
        assert caps.supports_restricted_session is True
        assert caps.supports_execution_role is True
        assert caps.supports_offset_pagination_wrapper is True
        assert caps.supports_query_wrapping_subselect is True

    def test_sqlite_capabilities(self):
        """The SQLite backend has transactions but no arrays/JSON ops."""
        caps = capabilities_for_provider("sqlite")
        assert caps.execution_model == "sync"
        assert caps.supports_arrays is False
        assert caps.supports_json_ops is False
        assert caps.supports_transactions is True
        assert caps.supports_fk_enforcement is False
        assert caps.supports_restricted_session is False
        assert caps.supports_execution_role is False

    def test_mysql_capabilities(self):
        """The MySQL backend has transactions but limited feature flags."""
        caps = capabilities_for_provider("mysql")
        assert caps.execution_model == "sync"
        assert caps.supports_arrays is False
        assert caps.supports_json_ops is False
        assert caps.supports_transactions is True
        assert caps.supports_fk_enforcement is False

    def test_redshift_capabilities(self):
        """Ensure Redshift disables unsupported capability flags."""
        caps = capabilities_for_provider("redshift")
        assert caps.execution_model == "sync"
        assert caps.supports_arrays is False
        assert caps.supports_json_ops is False
        assert caps.supports_transactions is False
        assert caps.supports_fk_enforcement is False
        assert caps.supports_db_readonly_session is True
        assert caps.enforces_statement_read_only is True

    def test_snowflake_capabilities(self):
        """Snowflake is async with limited feature support."""
        caps = capabilities_for_provider("snowflake")
        assert caps.execution_model == "async"
        assert caps.supports_arrays is False
        assert caps.supports_json_ops is False
        assert caps.supports_transactions is False
        assert caps.supports_cost_estimation is False
        assert caps.supports_db_readonly_session is False
        assert caps.enforces_statement_read_only is True

    def test_bigquery_capabilities(self):
        """Ensure BigQuery reports async execution and cost estimation."""
        caps = capabilities_for_provider("bigquery")
        assert caps.execution_model == "async"
        assert caps.supports_arrays is True
        assert caps.supports_json_ops is False
        assert caps.supports_transactions is False
        assert caps.supports_cost_estimation is True
        assert caps.supports_db_readonly_session is False
        assert caps.enforces_statement_read_only is True
        assert caps.supports_offset_pagination_wrapper is False
        assert caps.supports_query_wrapping_subselect is False

    def test_athena_capabilities(self):
        """Ensure Athena reports async execution."""
        caps = capabilities_for_provider("athena")
        assert caps.execution_model == "async"
        assert caps.supports_arrays is False
        assert caps.supports_transactions is False

    def test_databricks_capabilities(self):
        """Databricks supports arrays and JSON ops via Unity Catalog."""
        caps = capabilities_for_provider("databricks")
        assert caps.execution_model == "async"
        assert caps.supports_arrays is True
        assert caps.supports_json_ops is True
        assert caps.supports_transactions is False

    def test_cockroachdb_capabilities(self):
        """The CockroachDB backend is sync but without transaction support."""
        caps = capabilities_for_provider("cockroachdb")
        assert caps.execution_model == "sync"
        assert caps.supports_arrays is True
        assert caps.supports_json_ops is True
        assert caps.supports_transactions is False
        assert caps.supports_fk_enforcement is False

    def test_duckdb_capabilities(self):
        """The DuckDB backend supports arrays, JSON ops, and transactions."""
        caps = capabilities_for_provider("duckdb")
        assert caps.execution_model == "sync"
        assert caps.supports_arrays is True
        assert caps.supports_json_ops is True
        assert caps.supports_transactions is True
        assert caps.supports_fk_enforcement is False
        assert caps.supports_offset_pagination_wrapper is True
        assert caps.supports_query_wrapping_subselect is True

    def test_clickhouse_capabilities(self):
        """The ClickHouse backend is append-only OLAP with minimal feature support."""
        caps = capabilities_for_provider("clickhouse")
        assert caps.execution_model == "sync"
        assert caps.supports_arrays is False
        assert caps.supports_json_ops is False
        assert caps.supports_transactions is False
        assert caps.supports_fk_enforcement is False


class TestPaginationCapability:
    """Verify pagination capability is registered consistently."""

    @pytest.mark.parametrize("provider", KNOWN_PROVIDERS)
    def test_pagination_capability_registration(self, provider: str):
        """Providers should report pagination support deterministically."""
        caps = capabilities_for_provider(provider)
        expected = provider in PAGINATION_PROVIDERS
        assert caps.supports_pagination is expected


class TestTenantEnforcementCapabilities:
    """Tenant enforcement mode registration should be deterministic."""

    def test_postgres_uses_rls_session_mode(self):
        """Postgres should report RLS session tenant enforcement."""
        caps = capabilities_for_provider("postgres")
        assert caps.supports_tenant_enforcement is True
        assert caps.tenant_enforcement_mode == "rls_session"

    @pytest.mark.parametrize("provider", ["sqlite", "duckdb"])
    def test_sql_rewrite_providers(self, provider: str):
        """SQLite/DuckDB should opt in to tenant SQL rewrite mode."""
        caps = capabilities_for_provider(provider)
        assert caps.supports_tenant_enforcement is True
        assert caps.tenant_enforcement_mode == "sql_rewrite"

    @pytest.mark.parametrize(
        "provider",
        sorted(KNOWN_PROVIDERS - {"postgres", "sqlite", "duckdb"}),
    )
    def test_remaining_providers_are_unsupported(self, provider: str):
        """Providers outside the allowlist should fail tenant enforcement."""
        caps = capabilities_for_provider(provider)
        assert caps.supports_tenant_enforcement is False
        assert caps.tenant_enforcement_mode == "unsupported"
