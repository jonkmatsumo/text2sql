"""Provider tenant-enforcement behavior for execute_sql_query."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mcp_server.tools.execute_sql_query import handler


def _caps(provider: str, mode: str) -> SimpleNamespace:
    return SimpleNamespace(
        provider_name=provider,
        tenant_enforcement_mode=mode,
        supports_tenant_enforcement=mode != "unsupported",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )


def _metadata_store_with_columns(table_columns: dict[str, list[str]]):
    store = MagicMock()
    normalized_map = {
        (table_name or "").strip().lower(): columns for table_name, columns in table_columns.items()
    }

    async def _get_table_definition(table_name: str, tenant_id: int | None = None) -> str:
        del tenant_id
        normalized = (table_name or "").strip().lower()
        columns = normalized_map.get(normalized)
        if columns is None:
            raise KeyError(normalized)
        return json.dumps(
            {
                "table_name": normalized,
                "columns": [{"name": col} for col in columns],
                "foreign_keys": [],
            }
        )

    store.get_table_definition = AsyncMock(side_effect=_get_table_definition)
    return store


@asynccontextmanager
async def _conn_ctx(rows: list[dict] | None = None, recorder: dict | None = None):
    class _Conn:
        async def fetch(self, sql, *params):
            if recorder is not None:
                recorder["sql"] = sql
                recorder["params"] = list(params)
            return list(rows or [])

    yield _Conn()


@pytest.mark.asyncio
async def test_postgres_provider_allows_tenant_scoped_execution():
    """Allow tenant-scoped execution when provider supports tenant enforcement."""
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_caps("postgres", "rls_session"),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(rows=[{"ok": 1}]),
        ),
    ):
        payload = await handler("SELECT 1 AS ok", tenant_id=1)

    result = json.loads(payload)
    assert result.get("error") is None
    assert result["rows"] == [{"ok": 1}]


@pytest.mark.asyncio
async def test_sqlite_provider_rewrites_sql_and_binds_tenant_param():
    """Verify SQLite sql_rewrite mode injects tenant predicate and binds tenant_id."""
    observed: dict[str, object] = {}
    mock_store = _metadata_store_with_columns({"orders": ["order_id", "tenant_id", "status"]})
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_caps("sqlite", "sql_rewrite"),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
            return_value=mock_store,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(rows=[{"ok": 1}], recorder=observed),
        ),
    ):
        payload = await handler("SELECT * FROM orders", tenant_id=9)

    result = json.loads(payload)
    assert result.get("error") is None
    assert result["rows"] == [{"ok": 1}]
    assert "tenant_id = ?" in str(observed.get("sql", ""))
    assert observed.get("params") == [9]


@pytest.mark.asyncio
async def test_duckdb_provider_rewrites_sql_and_preserves_existing_params():
    """Verify DuckDB rewrite appends tenant_id after user-supplied params."""
    observed: dict[str, object] = {}
    mock_store = _metadata_store_with_columns({"orders": ["order_id", "tenant_id", "status"]})
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_caps("duckdb", "sql_rewrite"),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
            return_value=mock_store,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(rows=[{"ok": 1}], recorder=observed),
        ),
    ):
        payload = await handler(
            "SELECT * FROM orders o WHERE o.status = $1",
            tenant_id=12,
            params=["active"],
        )

    result = json.loads(payload)
    assert result.get("error") is None
    assert result["rows"] == [{"ok": 1}]
    assert "o.tenant_id = ?" in str(observed.get("sql", ""))
    assert observed.get("params") == ["active", 12]


@pytest.mark.asyncio
async def test_sql_rewrite_rejects_when_no_table_predicate_can_be_added():
    """Table-less SELECT should fail when rewrite mode cannot inject tenant predicates."""
    mock_connection = MagicMock()
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_caps("sqlite", "sql_rewrite"),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            mock_connection,
        ),
    ):
        payload = await handler("SELECT 1", tenant_id=5)

    result = json.loads(payload)
    assert result["error"]["category"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
    mock_connection.assert_not_called()


@pytest.mark.asyncio
async def test_sql_rewrite_rejects_when_schema_lacks_tenant_column():
    """Schema-aware guard should fail when tenant column is missing."""
    mock_connection = MagicMock()
    mock_store = MagicMock()
    mock_store.get_table_definition = AsyncMock(
        return_value=json.dumps(
            {
                "table_name": "orders",
                "columns": [{"name": "order_id"}, {"name": "status"}],
                "foreign_keys": [],
            }
        )
    )
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_caps("sqlite", "sql_rewrite"),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
            return_value=mock_store,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            mock_connection,
        ),
    ):
        payload = await handler("SELECT * FROM orders", tenant_id=5)

    result = json.loads(payload)
    assert result["error"]["category"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
    mock_connection.assert_not_called()


@pytest.mark.asyncio
async def test_sql_rewrite_rejects_when_schema_metadata_is_unavailable():
    """Missing schema metadata should fail closed."""
    mock_connection = MagicMock()
    mock_store = MagicMock()
    mock_store.get_table_definition = AsyncMock(side_effect=RuntimeError("not found"))
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_caps("sqlite", "sql_rewrite"),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
            return_value=mock_store,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            mock_connection,
        ),
    ):
        payload = await handler("SELECT * FROM orders", tenant_id=5)

    result = json.loads(payload)
    assert result["error"]["category"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
    mock_connection.assert_not_called()


@pytest.mark.asyncio
async def test_sql_rewrite_allows_allowlisted_table_without_tenant_predicate(monkeypatch):
    """Allowlisted global table should execute without injected tenant predicate."""
    monkeypatch.setenv("GLOBAL_TABLE_ALLOWLIST", "global_reference")
    observed: dict[str, object] = {}
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_caps("sqlite", "sql_rewrite"),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(rows=[{"ok": 1}], recorder=observed),
        ),
    ):
        payload = await handler("SELECT * FROM global_reference", tenant_id=9)

    result = json.loads(payload)
    assert result.get("error") is None
    assert "tenant_id = ?" not in str(observed.get("sql", ""))
    assert observed.get("params") == []


@pytest.mark.asyncio
async def test_sql_rewrite_allowlist_exempts_only_listed_tables(monkeypatch):
    """Allowlist should skip global tables while still scoping tenant tables."""
    monkeypatch.setenv("GLOBAL_TABLE_ALLOWLIST", "global_reference")
    observed: dict[str, object] = {}
    mock_store = _metadata_store_with_columns({"orders": ["order_id", "tenant_id", "status"]})
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_caps("sqlite", "sql_rewrite"),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
            return_value=mock_store,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(rows=[{"ok": 1}], recorder=observed),
        ),
    ):
        payload = await handler(
            "SELECT o.id FROM global_reference g JOIN orders o ON g.order_id = o.id",
            tenant_id=9,
        )

    result = json.loads(payload)
    assert result.get("error") is None
    sql = str(observed.get("sql", ""))
    assert "g.tenant_id = ?" not in sql
    assert "o.tenant_id = ?" in sql
    assert observed.get("params") == [9]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider",
    [
        "mysql",
        "snowflake",
        "clickhouse",
        "redshift",
        "bigquery",
        "athena",
        "databricks",
    ],
)
async def test_non_postgres_provider_rejects_tenant_scoped_execution(provider: str):
    """Reject tenant-scoped execution for providers without tenant enforcement."""
    mock_connection = MagicMock()
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_caps(provider, "unsupported"),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            mock_connection,
        ),
    ):
        payload = await handler("SELECT * FROM orders", tenant_id=1)

    result = json.loads(payload)
    assert result["error"]["category"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error"]["message"] == "Tenant isolation is not supported for this provider."
    mock_connection.assert_not_called()


@pytest.mark.asyncio
async def test_non_postgres_tenant_bypass_flag_no_longer_allows_execution(monkeypatch):
    """Unsupported providers should reject even if legacy bypass flag is set."""
    monkeypatch.setenv("ALLOW_NON_POSTGRES_TENANT_BYPASS", "true")
    mock_connection = MagicMock()
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_caps("mysql", "unsupported"),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            mock_connection,
        ),
    ):
        payload = await handler("SELECT * FROM orders", tenant_id=7)

    result = json.loads(payload)
    assert result["error"]["category"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
    mock_connection.assert_not_called()
