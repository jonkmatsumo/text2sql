"""Tests for execute_sql_query tool."""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from mcp_server.tools.execute_sql_query import TOOL_NAME, handler
from tests._support.tenant_enforcement_contract import assert_tenant_enforcement_contract

_TENANT_CONTRACT_FIXTURE_DIR = (
    Path(__file__).resolve().parent / "fixtures" / "execute_sql_query_tenant_enforcement"
)


class _ToolFakeConn:
    def __init__(self):
        self.execute_calls = []
        self.fetch_calls = []
        self.fetchrow_calls = []
        self.events = []

    @asynccontextmanager
    async def transaction(self, readonly=False):
        self.events.append(("transaction", readonly))
        yield

    async def execute(self, sql, *args):
        self.execute_calls.append((sql, args))
        self.events.append(("execute", sql))

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        self.events.append(("fetch", sql))
        return [{"ok": 1}]

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        self.events.append(("fetchrow", sql))
        return {"dblink_installed": False, "dblink_accessible": False}


class _ToolFakePool:
    def __init__(self, conn):
        self._conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


class TestExecuteSqlQuery:
    """Tests for execute_sql_query tool."""

    def setup_method(self, method):
        """Initialize Database capabilities for tests."""
        from dal.capabilities import BackendCapabilities
        from dal.database import Database

        Database._query_target_capabilities = BackendCapabilities(
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="rls_session",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        )
        Database._query_target_provider = "postgres"

    @pytest.fixture(autouse=True)
    def mock_policy_enforcer(self):
        """Mock PolicyEnforcer to bypass validation."""
        with patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"):
            yield

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "execute_sql_query"

    @pytest.mark.asyncio
    async def test_execute_sql_query_requires_tenant_id(self):
        """Test that execute_sql_query requires tenant_id."""
        result = await handler("SELECT * FROM film", tenant_id=None)

        data = json.loads(result)
        assert "error" in data
        assert data["error"]["message"] and (
            "Tenant ID" in data["error"]["message"] or "Unauthorized" in data["error"]["message"]
        )

    @pytest.mark.asyncio
    async def test_execute_sql_query_valid_select(self):
        """Test executing a valid SELECT query."""
        mock_conn = AsyncMock()
        mock_rows = [{"count": 1000}]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT COUNT(*) as count FROM film", tenant_id=1)

            mock_get.assert_called_once()
            mock_conn.fetch.assert_called_once_with("SELECT COUNT(*) as count FROM film")

            data = json.loads(result)
            # New envelope structure check
            assert data["schema_version"] == "1.0"
            assert data["rows"][0]["count"] == 1000
            assert data["metadata"]["tool_version"] == "v1"
            assert data["metadata"]["provider"] == "postgres"
            assert data["metadata"]["is_truncated"] is False
            assert data["metadata"]["rows_returned"] == 1
            assert data["metadata"]["tenant_enforcement_mode"] == "rls_session"
            assert data["metadata"]["tenant_enforcement_applied"] is True
            assert data["metadata"]["tenant_rewrite_outcome"] == "APPLIED"
            assert data["metadata"].get("tenant_rewrite_reason_code") is None

    @pytest.mark.asyncio
    async def test_execute_sql_query_passes_read_only_connection_flag(self):
        """Tool should always request read-only DAL connections."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"ok": 1}])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            await handler("SELECT 1 AS ok", tenant_id=1, include_columns=False)

        mock_get.assert_called_once_with(tenant_id=1, read_only=True)

    @pytest.mark.asyncio
    async def test_execute_sql_query_runs_postgres_least_privilege_hook(self, monkeypatch):
        """Tool should traverse DAL connection path that applies restricted session + role."""
        from dal.capabilities import capabilities_for_provider
        from dal.database import Database

        fake_conn = _ToolFakeConn()
        monkeypatch.setenv("POSTGRES_RESTRICTED_SESSION_ENABLED", "true")
        monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")
        monkeypatch.setenv("POSTGRES_EXECUTION_ROLE", "text2sql_readonly")
        monkeypatch.setattr(Database, "_pool", _ToolFakePool(fake_conn))
        monkeypatch.setattr(Database, "_query_target_provider", "postgres")
        monkeypatch.setattr(
            Database, "_query_target_capabilities", capabilities_for_provider("postgres")
        )
        monkeypatch.setattr(Database, "_query_target_sync_max_rows", 0)
        monkeypatch.setattr(Database, "_postgres_extension_capability_cache", {})
        monkeypatch.setattr(Database, "_postgres_extension_warning_emitted", set())

        with patch("mcp_server.utils.auth.validate_role", return_value=None):
            result = await handler("SELECT 1 AS ok", tenant_id=1, include_columns=False)

        data = json.loads(result)
        assert data.get("error") is None
        assert data["rows"] == [{"ok": 1}]
        assert (
            "SELECT set_config('default_transaction_read_only', 'on', true)",
            (),
        ) in fake_conn.execute_calls
        assert ('SET LOCAL ROLE "text2sql_readonly"', ()) in fake_conn.execute_calls

        role_event_index = fake_conn.events.index(("execute", 'SET LOCAL ROLE "text2sql_readonly"'))
        fetch_event_index = fake_conn.events.index(("fetch", "SELECT 1 AS ok"))
        assert role_event_index < fetch_event_index

    @pytest.mark.asyncio
    async def test_execute_sql_query_include_columns_opt_in(self):
        """Opt-in include_columns returns wrapper with rows and columns."""
        mock_conn = AsyncMock()
        mock_rows = [{"count": 1000, "created_at": "2024-01-01T00:00:00Z"}]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler(
                "SELECT COUNT(*) as count, NOW() as created_at FROM film",
                tenant_id=1,
                include_columns=True,
            )

            data = json.loads(result)
            assert data["schema_version"] == "1.0"
            assert data["rows"] == mock_rows
            assert data["metadata"]["tool_version"] == "v1"
            assert data["metadata"]["is_truncated"] is False
            assert data["metadata"]["rows_returned"] == 1
            assert data["columns"] == [
                {
                    "name": "count",
                    "type": "unknown",
                    "db_type": None,
                    "nullable": None,
                    "precision": None,
                    "scale": None,
                    "timezone": None,
                },
                {
                    "name": "created_at",
                    "type": "unknown",
                    "db_type": None,
                    "nullable": None,
                    "precision": None,
                    "scale": None,
                    "timezone": None,
                },
            ]

    @pytest.mark.asyncio
    async def test_execute_sql_query_empty_result(self):
        """Test handling empty result set."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT * FROM film WHERE film_id = -1", tenant_id=1)

            data = json.loads(result)
            assert data["rows"] == []
            assert data["metadata"]["is_truncated"] is False
            assert data["metadata"]["rows_returned"] == 0

    @pytest.mark.asyncio
    async def test_execute_sql_query_size_limit(self):
        """Test enforcing 1000 row limit."""
        mock_conn = AsyncMock()
        mock_rows = [{"id": i} for i in range(1001)]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT * FROM film", tenant_id=1)

            data = json.loads(result)
            assert len(data["rows"]) == 1000
            assert data["metadata"]["is_truncated"] is True
            assert data["metadata"]["row_limit"] == 1000
            assert data["metadata"]["rows_returned"] == 1000

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_drop(self):
        """Test rejecting DROP keyword."""
        result = await handler("DROP TABLE film", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["message"]
        assert "Forbidden statement type" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_delete(self):
        """Test rejecting DELETE keyword."""
        result = await handler("DELETE FROM film WHERE film_id = 1", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["message"]
        assert "Forbidden statement type" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_insert(self):
        """Test rejecting INSERT keyword."""
        result = await handler("INSERT INTO film VALUES (1, 'Test')", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["message"]
        assert "Forbidden statement type" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_update(self):
        """Test rejecting UPDATE keyword."""
        result = await handler("UPDATE film SET title = 'Test'", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["message"]
        assert "Forbidden statement type" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_alter(self):
        """Test rejecting ALTER keyword."""
        result = await handler("ALTER TABLE film ADD COLUMN test INT", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["message"]
        assert "Forbidden statement type" in data["error"]["message"]
        assert "ALTER" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_multi_statement(self):
        """Test rejecting multiple statements."""
        result = await handler("SELECT 1; DROP TABLE film", tenant_id=1)
        data = json.loads(result)
        assert "Multi-statement queries are forbidden" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_security_case_insensitive(self):
        """Test case-insensitive security matching."""
        result1 = await handler("drop table film", tenant_id=1)
        data1 = json.loads(result1)
        assert "Forbidden statement type" in data1["error"]["message"]

        result2 = await handler("DeLeTe FrOm film", tenant_id=1)
        data2 = json.loads(result2)
        assert "Forbidden statement type" in data2["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_database_error(self):
        """Test handling PostgresError."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("Syntax error"))
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT * FROM nonexistent", tenant_id=1)

            data = json.loads(result)
            assert "error" in data
            assert "Syntax error" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_general_error(self):
        """Test handling general exceptions."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Unexpected error"))
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT * FROM film", tenant_id=1)

            data = json.loads(result)
            assert "error" in data
            assert "Unexpected error" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_with_params(self):
        """Test executing query with bind parameters."""
        mock_conn = AsyncMock()
        mock_rows = [{"id": 1}]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT * FROM film WHERE film_id = $1", tenant_id=1, params=[1])

            mock_conn.fetch.assert_called_once_with("SELECT * FROM film WHERE film_id = $1", 1)
            data = json.loads(result)
            assert len(data["rows"]) == 1

    @pytest.mark.asyncio
    async def test_execute_sql_query_none_enforcement_mode_sets_skip_metadata(self):
        """Non-enforced mode should set SKIPPED_NOT_REQUIRED contract metadata."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"id": 1}])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
        ):
            mock_caps.return_value.tenant_enforcement_mode = "none"
            mock_caps.return_value.provider_name = "sqlite"
            result = await handler("SELECT 1 AS id", tenant_id=1)

            data = json.loads(result)
            assert data.get("error") is None
            assert data["metadata"]["tenant_enforcement_mode"] == "none"
            assert data["metadata"]["tenant_enforcement_applied"] is False
            assert data["metadata"]["tenant_rewrite_outcome"] == "SKIPPED_NOT_REQUIRED"
            assert data["metadata"].get("tenant_rewrite_reason_code") is None

    @pytest.mark.asyncio
    async def test_execute_sql_query_respects_timeout_seconds(self):
        """Timeouts should return a classified timeout error."""
        mock_conn = AsyncMock()

        async def slow_fetch(*_args, **_kwargs):
            await asyncio.sleep(0.01)
            return [{"id": 1}]

        mock_conn.fetch = AsyncMock(side_effect=slow_fetch)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler(
                "SELECT * FROM film",
                tenant_id=1,
                timeout_seconds=0.001,
            )

            data = json.loads(result)
            assert data["error"]["category"] == "timeout"

    @pytest.mark.asyncio
    async def test_execute_sql_query_max_length_exceeded(self):
        """Test rejecting a query that exceeds MCP_MAX_SQL_LENGTH."""
        with (
            patch("mcp_server.tools.execute_sql_query.get_env_int", return_value=10),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT 1234567890", tenant_id=1)

            data = json.loads(result)
            assert data["error"]["category"] == "invalid_request"
            assert "exceeds maximum length" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_blocked_function(self):
        """Test rejecting blocked functions like pg_sleep."""
        with patch("mcp_server.utils.auth.validate_role", return_value=None):
            result = await handler("SELECT pg_sleep(5)", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert "Forbidden function" in data["error"]["message"]
        assert "PG_SLEEP" in data["error"]["message"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "provider,sql_query",
        [
            ("snowflake", "UPDATE users SET name = 'x' WHERE id = 1"),
            ("bigquery", "INSERT INTO dataset.users(id) VALUES (1)"),
            ("redshift", "DELETE FROM users WHERE id = 1"),
        ],
    )
    async def test_provider_mutation_policy_rejects_before_execution(
        self, provider: str, sql_query: str
    ):
        """Mutations must be blocked deterministically before provider execution starts."""
        from dal.database import Database

        Database._query_target_provider = provider
        mock_get_connection = MagicMock()

        with (
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_connection", mock_get_connection
            ),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler(sql_query, tenant_id=7)

        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert data["error"]["provider"] == provider
        assert "Forbidden statement type" in data["error"]["message"]
        mock_get_connection.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_sql_query_tenant_rewrite_failure_span(self):
        """Test that transformer failures set bounded span attributes without leaking details."""
        from common.sql.tenant_sql_rewriter import TenantSQLTransformerError, TransformerErrorKind

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True

        with (
            patch("opentelemetry.trace.get_current_span", return_value=mock_span),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
            patch(
                "common.sql.tenant_sql_rewriter.transform_tenant_scoped_sql",
                side_effect=TenantSQLTransformerError(
                    TransformerErrorKind.PARAM_LIMIT_EXCEEDED,
                    "Internal detailed error",
                ),
            ),
        ):
            mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
            mock_caps.return_value.provider_name = "sqlite"

            result = await handler("SELECT * FROM film", tenant_id=1)

            data = json.loads(result)
            assert data["error"]["category"].upper() == "TENANT_ENFORCEMENT_UNSUPPORTED"
            assert "Internal detailed error" not in data["error"]["message"]
            assert (
                data["error"]["message"] == "Tenant isolation is not supported for this provider."
            )
            assert (
                data["error"]["details_safe"]["reason_code"]
                == "tenant_rewrite_param_limit_exceeded"
            )
            assert data["metadata"]["tenant_enforcement_mode"] == "sql_rewrite"
            assert data["metadata"]["tenant_enforcement_applied"] is False
            assert data["metadata"]["tenant_rewrite_outcome"] == "REJECTED_LIMIT"
            assert (
                data["metadata"]["tenant_rewrite_reason_code"]
                == "tenant_rewrite_param_limit_exceeded"
            )
            assert "sql" not in data["metadata"]

            mock_span.set_attribute.assert_any_call(
                "tenant_rewrite.failure_reason", "PARAM_LIMIT_EXCEEDED"
            )
            mock_span.set_attribute.assert_any_call(
                "tenant_rewrite.failure_reason_code",
                "tenant_rewrite_param_limit_exceeded",
            )
            assert not any(
                call.args and call.args[0] == "rewrite.target_count"
                for call in mock_span.set_attribute.call_args_list
            )

    @pytest.mark.asyncio
    async def test_execute_sql_query_tenant_enforcement_observability_success_attributes(self):
        """Success response should emit bounded tenant enforcement span + metric attributes."""
        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"ok": 1}])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("opentelemetry.trace.get_current_span", return_value=mock_span),
            patch("mcp_server.tools.execute_sql_query.mcp_metrics.add_counter") as mock_add_counter,
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_connection", return_value=mock_conn
            ),
        ):
            mock_caps.return_value.tenant_enforcement_mode = "rls_session"
            result = await handler("SELECT 1 AS ok", tenant_id=1)

        data = json.loads(result)
        assert data.get("error") is None
        assert data["metadata"]["tenant_enforcement_mode"] == "rls_session"
        assert data["metadata"]["tenant_rewrite_outcome"] == "APPLIED"
        mock_span.set_attribute.assert_any_call("tenant.enforcement.mode", "rls_session")
        mock_span.set_attribute.assert_any_call("tenant.enforcement.outcome", "APPLIED")
        mock_span.set_attribute.assert_any_call("tenant.enforcement.applied", True)
        assert not any(
            call.args and call.args[0] == "tenant.enforcement.reason_code"
            for call in mock_span.set_attribute.call_args_list
        )
        mock_add_counter.assert_any_call(
            "mcp.tenant_enforcement.outcome_total",
            description="Count of execute_sql_query tenant enforcement outcomes",
            attributes={
                "tool_name": "execute_sql_query",
                "mode": "rls_session",
                "outcome": "APPLIED",
                "applied": True,
            },
        )

    @pytest.mark.asyncio
    async def test_execute_sql_query_tenant_enforcement_observability_failure_attributes(self):
        """Rejected response should emit bounded tenant enforcement reason telemetry."""
        from common.sql.tenant_sql_rewriter import TenantRewriteSettings

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        with (
            patch("opentelemetry.trace.get_current_span", return_value=mock_span),
            patch("mcp_server.tools.execute_sql_query.mcp_metrics.add_counter") as mock_add_counter,
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
            patch(
                "common.sql.tenant_sql_rewriter.load_tenant_rewrite_settings",
                return_value=TenantRewriteSettings(
                    enabled=False,
                    strict_mode=True,
                    max_targets=25,
                    max_params=50,
                    max_ast_nodes=1000,
                    warn_ms=50,
                    hard_timeout_ms=200,
                    assert_invariants=False,
                ),
            ),
        ):
            mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
            result = await handler("SELECT * FROM orders", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
        assert data["metadata"]["tenant_rewrite_outcome"] == "REJECTED_DISABLED"
        mock_span.set_attribute.assert_any_call("tenant.enforcement.mode", "sql_rewrite")
        mock_span.set_attribute.assert_any_call("tenant.enforcement.outcome", "REJECTED_DISABLED")
        mock_span.set_attribute.assert_any_call("tenant.enforcement.applied", False)
        mock_span.set_attribute.assert_any_call(
            "tenant.enforcement.reason_code",
            "tenant_rewrite_rewrite_disabled",
        )
        mock_add_counter.assert_any_call(
            "mcp.tenant_enforcement.outcome_total",
            description="Count of execute_sql_query tenant enforcement outcomes",
            attributes={
                "tool_name": "execute_sql_query",
                "mode": "sql_rewrite",
                "outcome": "REJECTED_DISABLED",
                "applied": False,
                "reason_code": "tenant_rewrite_rewrite_disabled",
            },
        )

    @pytest.mark.asyncio
    async def test_execute_sql_query_tenant_rewrite_timeout_emits_duration_and_fails_closed(self):
        """Long rewrite duration should emit timing span data and fail with bounded reason."""
        from common.sql.tenant_sql_rewriter import TenantSQLRewriteResult

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_connection = MagicMock()

        with (
            patch("opentelemetry.trace.get_current_span", return_value=mock_span),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
            patch(
                "common.sql.tenant_sql_rewriter.transform_tenant_scoped_sql",
                return_value=TenantSQLRewriteResult(
                    rewritten_sql="SELECT * FROM film WHERE film.tenant_id = ?",
                    params=[1],
                    tables_rewritten=["film"],
                    tenant_predicates_added=1,
                ),
            ),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_connection",
                mock_connection,
            ),
            patch(
                "common.security.tenant_enforcement_policy.time.perf_counter",
                side_effect=[10.0, 10.25],
            ),
            patch.dict(
                "os.environ",
                {
                    "TENANT_REWRITE_WARN_MS": "50",
                    "TENANT_REWRITE_HARD_TIMEOUT_MS": "200",
                },
                clear=False,
            ),
        ):
            mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
            mock_caps.return_value.provider_name = "sqlite"
            result = await handler("SELECT * FROM film", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["category"].upper() == "TENANT_ENFORCEMENT_UNSUPPORTED"
        assert data["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
        assert data["error"]["details_safe"]["reason_code"] == "tenant_rewrite_rewrite_timeout"
        assert data["metadata"]["tenant_enforcement_mode"] == "sql_rewrite"
        assert data["metadata"]["tenant_enforcement_applied"] is False
        assert data["metadata"]["tenant_rewrite_outcome"] == "REJECTED_TIMEOUT"
        assert data["metadata"]["tenant_rewrite_reason_code"] == "tenant_rewrite_rewrite_timeout"
        mock_connection.assert_not_called()
        mock_span.set_attribute.assert_any_call("rewrite.duration_ms", 250.0)
        mock_span.set_attribute.assert_any_call("tenant_rewrite.failure_reason", "REWRITE_TIMEOUT")

    @pytest.mark.asyncio
    async def test_execute_sql_query_uses_shared_tenant_rewrite_settings_for_timeout(self):
        """execute_sql_query should source rewrite timeout thresholds from shared settings."""
        from common.sql.tenant_sql_rewriter import TenantRewriteSettings, TenantSQLRewriteResult

        mock_connection = MagicMock()
        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
            patch(
                "common.sql.tenant_sql_rewriter.load_tenant_rewrite_settings",
                return_value=TenantRewriteSettings(
                    enabled=True,
                    strict_mode=True,
                    max_targets=25,
                    max_params=50,
                    max_ast_nodes=1000,
                    warn_ms=5,
                    hard_timeout_ms=10,
                    assert_invariants=False,
                ),
            ) as mock_settings_loader,
            patch(
                "common.sql.tenant_sql_rewriter.transform_tenant_scoped_sql",
                return_value=TenantSQLRewriteResult(
                    rewritten_sql="SELECT * FROM film WHERE film.tenant_id = ?",
                    params=[1],
                    tables_rewritten=["film"],
                    tenant_predicates_added=1,
                ),
            ),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_connection",
                mock_connection,
            ),
            patch(
                "common.security.tenant_enforcement_policy.time.perf_counter",
                side_effect=[100.0, 100.015],
            ),
        ):
            mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
            mock_caps.return_value.provider_name = "sqlite"
            result = await handler("SELECT * FROM film", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
        assert data["error"]["details_safe"]["reason_code"] == "tenant_rewrite_rewrite_timeout"
        assert data["metadata"]["tenant_rewrite_outcome"] == "REJECTED_TIMEOUT"
        mock_connection.assert_not_called()
        mock_settings_loader.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_sql_query_tenant_rewrite_success_emits_explainability_metadata(self):
        """Successful rewrite should emit bounded explainability attributes on the span."""
        from common.sql.tenant_sql_rewriter import TenantSQLRewriteResult

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"ok": 1}])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get_connection = MagicMock(return_value=mock_conn)

        mock_store = MagicMock()
        mock_store.get_table_definition = AsyncMock(
            return_value=json.dumps(
                {
                    "table_name": "orders",
                    "columns": [{"name": "id"}, {"name": "tenant_id"}],
                    "foreign_keys": [],
                }
            )
        )

        with (
            patch("opentelemetry.trace.get_current_span", return_value=mock_span),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
            patch(
                "common.sql.tenant_sql_rewriter.transform_tenant_scoped_sql",
                return_value=TenantSQLRewriteResult(
                    rewritten_sql="SELECT * FROM orders WHERE orders.tenant_id = ?",
                    params=[1],
                    tables_rewritten=["orders"],
                    tenant_predicates_added=1,
                    target_count=1,
                    scope_depth=2,
                    has_cte=False,
                    has_subquery=True,
                ),
            ),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
                return_value=mock_store,
            ),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_connection", mock_get_connection
            ),
        ):
            mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
            mock_caps.return_value.provider_name = "sqlite"
            result = await handler("SELECT * FROM orders", tenant_id=1)

        data = json.loads(result)
        assert data.get("error") is None
        assert data["rows"] == [{"ok": 1}]
        assert data["metadata"]["tenant_enforcement_mode"] == "sql_rewrite"
        assert data["metadata"]["tenant_enforcement_applied"] is True
        assert data["metadata"]["tenant_rewrite_outcome"] == "APPLIED"
        assert data["metadata"].get("tenant_rewrite_reason_code") is None
        assert "sql" not in data["metadata"]
        mock_span.set_attribute.assert_any_call("rewrite.target_count", 1)
        mock_span.set_attribute.assert_any_call("rewrite.param_count", 1)
        mock_span.set_attribute.assert_any_call("rewrite.scope_depth", 2)
        mock_span.set_attribute.assert_any_call("rewrite.has_cte", False)
        mock_span.set_attribute.assert_any_call("rewrite.has_subquery", True)

    @pytest.mark.asyncio
    async def test_execute_sql_query_tenant_rewrite_no_eligible_targets_sets_skip_metadata(self):
        """No-target rewrite path should skip enforcement metadata instead of rejecting."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"ok": 1}])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get_connection = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_connection", mock_get_connection
            ),
        ):
            mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
            mock_caps.return_value.provider_name = "sqlite"
            result = await handler("SELECT 1 AS ok", tenant_id=1)

        data = json.loads(result)
        assert data.get("error") is None
        assert data["rows"] == [{"ok": 1}]
        assert data["metadata"]["tenant_enforcement_mode"] == "sql_rewrite"
        assert data["metadata"]["tenant_enforcement_applied"] is False
        assert data["metadata"]["tenant_rewrite_outcome"] == "SKIPPED_NOT_REQUIRED"
        assert data["metadata"].get("tenant_rewrite_reason_code") is None

    @pytest.mark.asyncio
    async def test_execute_sql_query_tenant_rewrite_disabled_sets_rejected_disabled_outcome(self):
        """Disabled rewrite should expose deterministic outcome and bounded reason metadata."""
        from common.sql.tenant_sql_rewriter import TenantRewriteSettings

        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
            patch(
                "common.sql.tenant_sql_rewriter.load_tenant_rewrite_settings",
                return_value=TenantRewriteSettings(
                    enabled=False,
                    strict_mode=True,
                    max_targets=25,
                    max_params=50,
                    max_ast_nodes=1000,
                    warn_ms=50,
                    hard_timeout_ms=200,
                    assert_invariants=False,
                ),
            ),
        ):
            mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
            mock_caps.return_value.provider_name = "sqlite"
            result = await handler("SELECT * FROM film", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
        assert data["error"]["details_safe"]["reason_code"] == "tenant_rewrite_rewrite_disabled"
        assert data["metadata"]["tenant_enforcement_mode"] == "sql_rewrite"
        assert data["metadata"]["tenant_enforcement_applied"] is False
        assert data["metadata"]["tenant_rewrite_outcome"] == "REJECTED_DISABLED"
        assert data["metadata"]["tenant_rewrite_reason_code"] == "tenant_rewrite_rewrite_disabled"
        assert "sql" not in data["metadata"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_tenant_rewrite_unsupported_sets_rejected_unsupported_outcome(
        self,
    ):
        """Unsupported rewrite shape should expose deterministic unsupported outcome metadata."""
        from common.sql.tenant_sql_rewriter import TenantSQLTransformerError, TransformerErrorKind

        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
            patch(
                "common.sql.tenant_sql_rewriter.transform_tenant_scoped_sql",
                side_effect=TenantSQLTransformerError(
                    TransformerErrorKind.SUBQUERY_UNSUPPORTED,
                    "unsupported subquery shape",
                ),
            ),
        ):
            mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
            mock_caps.return_value.provider_name = "sqlite"
            result = await handler("SELECT * FROM film", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
        assert data["error"]["details_safe"]["reason_code"] == "tenant_rewrite_subquery_unsupported"
        assert data["metadata"]["tenant_enforcement_mode"] == "sql_rewrite"
        assert data["metadata"]["tenant_enforcement_applied"] is False
        assert data["metadata"]["tenant_rewrite_outcome"] == "REJECTED_UNSUPPORTED"
        assert (
            data["metadata"]["tenant_rewrite_reason_code"] == "tenant_rewrite_subquery_unsupported"
        )
        assert "sql" not in data["metadata"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("outcome", "applied", "bounded_reason_code", "should_execute"),
        [
            ("APPLIED", True, None, True),
            ("SKIPPED_NOT_REQUIRED", False, None, True),
            ("REJECTED_DISABLED", False, "tenant_rewrite_rewrite_disabled", False),
            ("REJECTED_UNSUPPORTED", False, "tenant_rewrite_subquery_unsupported", False),
            ("REJECTED_LIMIT", False, "tenant_rewrite_target_limit_exceeded", False),
            ("REJECTED_TIMEOUT", False, "tenant_rewrite_rewrite_timeout", False),
        ],
    )
    async def test_execute_sql_query_uses_policy_decision_for_tenant_metadata(
        self,
        outcome: str,
        applied: bool,
        bounded_reason_code: str | None,
        should_execute: bool,
    ):
        """Tool metadata should mirror policy decision outcomes without local remapping."""
        from common.security.tenant_enforcement_policy import (
            PolicyDecision,
            TenantEnforcementResult,
        )

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"ok": 1}])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get_connection = MagicMock(return_value=mock_conn)

        envelope_metadata = {
            "tenant_enforcement_applied": applied,
            "tenant_enforcement_mode": "sql_rewrite",
            "tenant_rewrite_outcome": outcome,
        }
        if bounded_reason_code is not None:
            envelope_metadata["tenant_rewrite_reason_code"] = bounded_reason_code

        policy_decision = PolicyDecision(
            result=TenantEnforcementResult(
                applied=applied,
                mode="sql_rewrite",
                outcome=outcome,
                reason_code=bounded_reason_code,
            ),
            sql_to_execute=(
                "SELECT * FROM orders WHERE orders.tenant_id = ?"
                if outcome == "APPLIED"
                else "SELECT * FROM orders"
            ),
            params_to_bind=[1] if outcome == "APPLIED" else [],
            should_execute=should_execute,
            envelope_metadata=envelope_metadata,
            telemetry_attributes={},
            metric_attributes={},
            bounded_reason_code=bounded_reason_code,
            tenant_required=applied,
            would_apply_rewrite=applied,
        )

        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_connection", mock_get_connection
            ),
            patch(
                "common.security.tenant_enforcement_policy.TenantEnforcementPolicy.evaluate",
                new=AsyncMock(return_value=policy_decision),
            ),
        ):
            mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
            mock_caps.return_value.provider_name = "sqlite"
            result = await handler("SELECT * FROM orders", tenant_id=1)

        data = json.loads(result)
        assert_tenant_enforcement_contract(
            data,
            {
                "tenant_enforcement_mode": "sql_rewrite",
                "tenant_enforcement_applied": applied,
                "tenant_rewrite_outcome": outcome,
                "tenant_rewrite_reason_code": bounded_reason_code,
            },
        )
        if should_execute:
            assert data.get("error") is None
            mock_get_connection.assert_called_once()
        else:
            assert data["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
            if bounded_reason_code is not None:
                assert data["error"]["details_safe"]["reason_code"] == bounded_reason_code
            mock_get_connection.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_sql_query_tenant_rewrite_ast_limit_sets_rejected_limit_outcome(
        self, monkeypatch
    ):
        """AST complexity rejections should map to REJECTED_LIMIT outcome."""
        monkeypatch.setenv("MAX_SQL_AST_NODES", "80")
        mock_connection = MagicMock()
        sql = "SELECT * FROM orders o WHERE " + " AND ".join(
            f"o.id > {index}" for index in range(100)
        )
        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_connection),
        ):
            mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
            mock_caps.return_value.provider_name = "sqlite"
            result = await handler(sql, tenant_id=1)

        data = json.loads(result)
        assert data["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
        assert (
            data["error"]["details_safe"]["reason_code"] == "tenant_rewrite_ast_complexity_exceeded"
        )
        assert data["metadata"]["tenant_rewrite_outcome"] == "REJECTED_LIMIT"
        mock_connection.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_sql_query_tenant_rewrite_target_limit_sets_rejected_limit_outcome(
        self, monkeypatch
    ):
        """Target-limit failures should return canonical REJECTED_LIMIT outcome."""
        monkeypatch.setenv("TENANT_REWRITE_MAX_TARGETS", "1")
        mock_connection = MagicMock()
        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_connection),
        ):
            mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
            mock_caps.return_value.provider_name = "sqlite"
            result = await handler(
                "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id",
                tenant_id=1,
            )

        data = json.loads(result)
        assert data["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
        assert (
            data["error"]["details_safe"]["reason_code"] == "tenant_rewrite_target_limit_exceeded"
        )
        assert data["metadata"]["tenant_rewrite_outcome"] == "REJECTED_LIMIT"
        mock_connection.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_sql_query_tenant_rewrite_param_limit_sets_rejected_limit_outcome(
        self, monkeypatch
    ):
        """Param-limit failures should return canonical REJECTED_LIMIT outcome."""
        monkeypatch.setenv("TENANT_REWRITE_MAX_PARAMS", "1")
        mock_connection = MagicMock()
        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
            ) as mock_caps,
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_connection),
        ):
            mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
            mock_caps.return_value.provider_name = "sqlite"
            result = await handler(
                "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id",
                tenant_id=1,
            )

        data = json.loads(result)
        assert data["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
        assert data["error"]["details_safe"]["reason_code"] == "tenant_rewrite_param_limit_exceeded"
        assert data["metadata"]["tenant_rewrite_outcome"] == "REJECTED_LIMIT"
        mock_connection.assert_not_called()

    @staticmethod
    def _load_tenant_contract_fixture(name: str) -> dict:
        fixture_path = _TENANT_CONTRACT_FIXTURE_DIR / name
        return json.loads(fixture_path.read_text())

    @staticmethod
    def _assert_tenant_contract_payload(
        payload: dict,
        *,
        fixture: dict,
        source_sql: str,
    ) -> None:
        metadata = payload["metadata"]
        fixture_metadata = fixture["metadata"]
        expected_error_code = fixture.get("error_code")

        assert payload["schema_version"] == "1.0"
        assert "rows" in payload
        assert "metadata" in payload
        assert metadata["tool_version"] == "v1"
        assert "rows_returned" in metadata
        assert "is_truncated" in metadata
        assert "provider" in metadata
        assert_tenant_enforcement_contract(payload, fixture_metadata)

        for field_name, expected in fixture_metadata.items():
            assert metadata.get(field_name) == expected

        if expected_error_code is None:
            assert payload.get("error") is None
        else:
            assert payload["error"]["error_code"] == expected_error_code

        metadata_blob = json.dumps(metadata, sort_keys=True).lower()
        assert source_sql.strip().lower() not in metadata_blob
        assert "select * from" not in metadata_blob

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("fixture_name", "scenario_name"),
        [
            ("success_rewrite_applied.json", "rewrite_applied"),
            ("success_skip_not_required.json", "skip_not_required"),
            ("reject_disabled.json", "reject_disabled"),
            ("reject_unsupported_shape.json", "reject_unsupported_shape"),
        ],
    )
    async def test_execute_sql_query_tenant_enforcement_contract_fixture_snapshots(
        self,
        fixture_name: str,
        scenario_name: str,
    ):
        """Tenant enforcement metadata must stay backward compatible and bounded."""
        from common.models.tool_envelopes import ExecuteSQLQueryResponseEnvelope
        from common.sql.tenant_sql_rewriter import (
            TenantRewriteSettings,
            TenantSQLRewriteResult,
            TenantSQLTransformerError,
            TransformerErrorKind,
        )

        fixture = self._load_tenant_contract_fixture(fixture_name)

        if scenario_name == "rewrite_applied":
            source_sql = "SELECT * FROM orders"
            mock_conn = AsyncMock()
            mock_conn.fetch = AsyncMock(return_value=[{"ok": 1}])
            mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_conn.__aexit__ = AsyncMock(return_value=False)
            mock_store = MagicMock()
            mock_store.get_table_definition = AsyncMock(
                return_value=json.dumps(
                    {
                        "table_name": "orders",
                        "columns": [{"name": "id"}, {"name": "tenant_id"}],
                        "foreign_keys": [],
                    }
                )
            )

            with (
                patch("mcp_server.utils.auth.validate_role", return_value=None),
                patch(
                    "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
                ) as mock_caps,
                patch(
                    "common.sql.tenant_sql_rewriter.transform_tenant_scoped_sql",
                    return_value=TenantSQLRewriteResult(
                        rewritten_sql="SELECT * FROM orders WHERE orders.tenant_id = ?",
                        params=[1],
                        tables_rewritten=["orders"],
                        tenant_predicates_added=1,
                    ),
                ),
                patch(
                    "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
                    return_value=mock_store,
                ),
                patch(
                    "mcp_server.tools.execute_sql_query.Database.get_connection",
                    return_value=mock_conn,
                ),
            ):
                mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
                mock_caps.return_value.provider_name = "sqlite"
                payload = json.loads(await handler(source_sql, tenant_id=1))

        elif scenario_name == "skip_not_required":
            source_sql = "SELECT 1 AS ok"
            mock_conn = AsyncMock()
            mock_conn.fetch = AsyncMock(return_value=[{"ok": 1}])
            mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_conn.__aexit__ = AsyncMock(return_value=False)
            with (
                patch("mcp_server.utils.auth.validate_role", return_value=None),
                patch(
                    "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
                ) as mock_caps,
                patch(
                    "mcp_server.tools.execute_sql_query.Database.get_connection",
                    return_value=mock_conn,
                ),
            ):
                mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
                mock_caps.return_value.provider_name = "sqlite"
                payload = json.loads(await handler(source_sql, tenant_id=1))

        elif scenario_name == "reject_disabled":
            source_sql = "SELECT * FROM orders"
            with (
                patch("mcp_server.utils.auth.validate_role", return_value=None),
                patch(
                    "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
                ) as mock_caps,
                patch(
                    "common.sql.tenant_sql_rewriter.load_tenant_rewrite_settings",
                    return_value=TenantRewriteSettings(
                        enabled=False,
                        strict_mode=True,
                        max_targets=25,
                        max_params=50,
                        max_ast_nodes=1000,
                        warn_ms=50,
                        hard_timeout_ms=200,
                        assert_invariants=False,
                    ),
                ),
            ):
                mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
                mock_caps.return_value.provider_name = "sqlite"
                payload = json.loads(await handler(source_sql, tenant_id=1))

        elif scenario_name == "reject_unsupported_shape":
            source_sql = "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM customers c)"
            with (
                patch("mcp_server.utils.auth.validate_role", return_value=None),
                patch(
                    "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
                ) as mock_caps,
                patch(
                    "common.sql.tenant_sql_rewriter.transform_tenant_scoped_sql",
                    side_effect=TenantSQLTransformerError(
                        TransformerErrorKind.SUBQUERY_UNSUPPORTED,
                        "unsupported subquery shape",
                    ),
                ),
            ):
                mock_caps.return_value.tenant_enforcement_mode = "sql_rewrite"
                mock_caps.return_value.provider_name = "sqlite"
                payload = json.loads(await handler(source_sql, tenant_id=1))

        else:  # pragma: no cover
            raise AssertionError(f"Unknown scenario: {scenario_name}")

        ExecuteSQLQueryResponseEnvelope.model_validate(payload)
        self._assert_tenant_contract_payload(payload, fixture=fixture, source_sql=source_sql)
