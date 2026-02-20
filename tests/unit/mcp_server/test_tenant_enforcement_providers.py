"""Provider tenant-enforcement behavior for execute_sql_query."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from mcp_server.tools.execute_sql_query import handler


def _caps() -> SimpleNamespace:
    return SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )


@asynccontextmanager
async def _conn_ctx(rows: list[dict] | None = None):
    class _Conn:
        async def fetch(self, _sql, *_params):
            return list(rows or [])

    yield _Conn()


@pytest.mark.asyncio
async def test_postgres_provider_allows_tenant_scoped_execution():
    """Allow tenant-scoped execution when provider supports tenant enforcement."""
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.supports_tenant_scope_enforcement",
            return_value=True,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_caps(),
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
@pytest.mark.parametrize(
    "provider",
    [
        "sqlite",
        "mysql",
        "snowflake",
        "duckdb",
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
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value=provider,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.supports_tenant_scope_enforcement",
            return_value=False,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            mock_connection,
        ),
    ):
        payload = await handler("SELECT 1", tenant_id=1)

    result = json.loads(payload)
    assert result["error"]["category"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error"]["message"] == "Tenant isolation is not supported for this provider."
    mock_connection.assert_not_called()


@pytest.mark.asyncio
async def test_non_postgres_tenant_bypass_flag_allows_legacy_behavior(monkeypatch):
    """Allow legacy non-Postgres behavior when bypass flag is explicitly enabled."""
    monkeypatch.setenv("ALLOW_NON_POSTGRES_TENANT_BYPASS", "true")
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="sqlite",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.supports_tenant_scope_enforcement",
            return_value=False,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_caps(),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(rows=[{"ok": 1}]),
        ),
    ):
        payload = await handler("SELECT 1 AS ok", tenant_id=7)

    result = json.loads(payload)
    assert result.get("error") is None
    assert result["rows"] == [{"ok": 1}]
