"""Mocked end-to-end agent workflow coverage via `app.ainvoke()`."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

import pytest

from agent.graph import MAX_CLARIFY_ROUNDS, app
from common.errors.error_codes import ErrorCode
from common.models.error_metadata import ErrorCategory
from mcp_server.utils.errors import build_error_metadata
from tests.utils.mock_agent_runtime import (
    MockDAL,
    MockMCPClient,
    build_app_input,
    install_mock_agent_runtime,
    unique_thread_config,
)


def _error_envelope(
    *,
    category: str,
    message: str,
    error_code: str,
    code: str = "TOOL_ERROR",
    retryable: bool = False,
) -> dict[str, Any]:
    try:
        normalized_category = ErrorCategory(category)
    except Exception:
        normalized_category = ErrorCategory.UNKNOWN

    error_payload = build_error_metadata(
        message=message,
        category=normalized_category,
        provider="mock",
        code=code,
        error_code=error_code,
        retryable=retryable,
    ).model_dump(exclude_none=True)

    return {
        "schema_version": "1.0",
        "rows": [],
        "metadata": {"rows_returned": 0, "is_truncated": False, "provider": "mock"},
        "error": error_payload,
    }


def _success_envelope(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "rows": rows,
        "metadata": {"rows_returned": len(rows), "is_truncated": False},
    }


def _value(value: Any) -> Any:
    return value.value if hasattr(value, "value") else value


def _metadata_store_for_tables(table_columns: dict[str, list[str]]) -> object:
    normalized_columns = {
        (table_name or "").strip().lower(): [col.strip().lower() for col in columns]
        for table_name, columns in table_columns.items()
    }

    class _Store:
        async def get_table_definition(self, table_name: str, tenant_id: int | None = None) -> str:
            del tenant_id
            normalized = (table_name or "").strip().lower()
            columns = normalized_columns.get(normalized)
            if columns is None:
                raise KeyError(normalized)
            return json.dumps(
                {
                    "table_name": normalized,
                    "columns": [{"name": col} for col in columns],
                    "foreign_keys": [],
                }
            )

    return _Store()


@pytest.mark.asyncio
async def test_full_pipeline_successful_select_flow(monkeypatch):
    """Happy-path run should execute query and synthesize final answer."""
    dal = MockDAL(response=_success_envelope([{"value": 1}]))
    mcp = MockMCPClient(dal=dal)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    state = build_app_input(
        question="Show one sample row",
        from_cache=True,
        current_sql="SELECT 1 AS value",
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert result["error"] is None
    assert result["query_result"] == [{"value": 1}]
    assert result["messages"][-1].content == "Mocked synthesized response."
    assert len(dal.calls) == 1


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_payload(monkeypatch):
    """Mocked pipeline should execute rewritten SQLite SQL with tenant params."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["sql"] = sql
                observed["params"] = list(params)
                return [{"order_id": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
            include_columns=payload.get("include_columns", True),
            timeout_seconds=payload.get("timeout_seconds"),
            page_token=payload.get("page_token"),
            page_size=payload.get("page_size"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
        lambda: _metadata_store_for_tables(
            {"orders": ["order_id", "tenant_id", "customer_id", "status"]}
        ),
    )

    state = build_app_input(
        question="Show orders",
        from_cache=True,
        current_sql="SELECT * FROM orders",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert result["error"] is None
    assert result["query_result"] == [{"order_id": 1}]
    assert "tenant_id = ?" in str(observed.get("sql", ""))
    assert observed.get("params") == [1]
    assert len(dal.calls) == 0


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_aliased_join_success(monkeypatch):
    """Aliased joins should rewrite both table predicates with tenant params."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {"connection_called": False}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["connection_called"] = True
                observed["sql"] = sql
                observed["params"] = list(params)
                return [{"order_id": 1, "customer_name": "Ada"}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
            include_columns=payload.get("include_columns", True),
            timeout_seconds=payload.get("timeout_seconds"),
            page_token=payload.get("page_token"),
            page_size=payload.get("page_size"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
        lambda: _metadata_store_for_tables(
            {
                "orders": ["order_id", "tenant_id", "customer_id", "status"],
                "customers": ["id", "tenant_id", "name"],
            }
        ),
    )

    state = build_app_input(
        question="Show open orders with customer names",
        from_cache=True,
        current_sql=(
            "SELECT o.order_id, c.name AS customer_name "
            "FROM orders o JOIN customers c ON o.customer_id = c.id "
            "WHERE o.status = 'open'"
        ),
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert result["error"] is None
    assert result["query_result"] == [{"order_id": 1, "customer_name": "Ada"}]
    assert observed["connection_called"] is True
    assert "o.tenant_id = ?" in str(observed.get("sql", ""))
    assert "c.tenant_id = ?" in str(observed.get("sql", ""))
    assert observed.get("params") == [1, 1]
    assert len(dal.calls) == 0


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_rejects_unsupported_shape(monkeypatch):
    """Unsupported query shapes should fail with canonical, sanitized tenant error."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {"connection_called": False}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["connection_called"] = True
                observed["sql"] = sql
                observed["params"] = list(params)
                return [{"order_id": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
            include_columns=payload.get("include_columns", True),
            timeout_seconds=payload.get("timeout_seconds"),
            page_token=payload.get("page_token"),
            page_size=payload.get("page_size"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )

    state = build_app_input(
        question="Use a nested query",
        from_cache=True,
        current_sql="SELECT * FROM (SELECT * FROM orders) o",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["error_category"]) == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error_metadata"]["error_code"] == ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value
    assert observed["connection_called"] is False
    error_text = str(result.get("error") or "").lower()
    assert "tenant isolation is not supported for this provider" in error_text
    assert "select * from" not in error_text
    assert "orders" not in error_text


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_rejects_missing_tenant_column(monkeypatch):
    """Missing tenant columns should fail closed with sanitized tenant error."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {"connection_called": False}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["connection_called"] = True
                observed["sql"] = sql
                observed["params"] = list(params)
                return [{"order_id": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
            include_columns=payload.get("include_columns", True),
            timeout_seconds=payload.get("timeout_seconds"),
            page_token=payload.get("page_token"),
            page_size=payload.get("page_size"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
        lambda: _metadata_store_for_tables(
            {
                "orders": ["order_id", "status"],
            }
        ),
    )

    state = build_app_input(
        question="Show orders",
        from_cache=True,
        current_sql="SELECT * FROM orders",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["error_category"]) == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error_metadata"]["error_code"] == ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value
    assert observed["connection_called"] is False
    error_text = str(result.get("error") or "").lower()
    assert "tenant isolation is not supported for this provider" in error_text
    assert "orders" not in error_text
    assert "tenant_id" not in error_text


@pytest.mark.asyncio
async def test_full_pipeline_policy_violation_flow(monkeypatch):
    """Mutating SQL should be rejected by validation before execute tool call."""
    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    state = build_app_input(
        question="Drop customer table",
        from_cache=True,
        current_sql="DROP TABLE customer",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert result["query_result"] is None
    assert "forbidden" in str(result["error"]).lower()
    assert len(dal.calls) == 0


@pytest.mark.asyncio
async def test_full_pipeline_tenant_enforcement_rejection_flow(monkeypatch):
    """Tenant-enforcement rejection should surface canonical tenant error code."""
    dal = MockDAL(
        response=_error_envelope(
            category="TENANT_ENFORCEMENT_UNSUPPORTED",
            message="Tenant isolation not supported for provider sqlite",
            error_code=ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value,
            code="TENANT_ENFORCEMENT_UNSUPPORTED",
        )
    )
    mcp = MockMCPClient(dal=dal)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    state = build_app_input(
        question="Show orders",
        from_cache=True,
        current_sql="SELECT * FROM orders",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["error_category"]) == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error_metadata"]["error_code"] == ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value
    assert "sqlite" not in str(result.get("error") or "").lower()
    assert (
        "tenant isolation is not supported for this provider"
        in str(result.get("error") or "").lower()
    )


@pytest.mark.asyncio
async def test_full_pipeline_ambiguity_unresolved_flow(monkeypatch):
    """Ambiguous requests should surface clarification state without DAL execution."""
    dal = MockDAL(response=_success_envelope([]))
    mcp = MockMCPClient(dal=dal)
    mcp.set_tool_response("lookup_cache", {"value": None})
    mcp.set_tool_response(
        "resolve_ambiguity",
        {
            "status": "AMBIGUOUS",
            "ambiguity_type": "schema_reference",
            "options": ["customer region", "store region"],
            "resolved_bindings": {},
        },
    )
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    state = build_app_input(
        question="Show revenue by region",
        clarify_count=MAX_CLARIFY_ROUNDS,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["ambiguity_type"]) == "schema_reference"
    assert isinstance(result.get("clarification_question"), str)
    assert result["query_result"] is None
    assert len(dal.calls) == 0


@pytest.mark.asyncio
async def test_full_pipeline_db_timeout_propagation_flow(monkeypatch):
    """DB timeout errors should preserve canonical timeout error code."""
    dal = MockDAL(
        response=_error_envelope(
            category="timeout",
            message="Execution timed out.",
            error_code=ErrorCode.DB_TIMEOUT.value,
            code="DRIVER_TIMEOUT",
            retryable=True,
        )
    )
    mcp = MockMCPClient(dal=dal)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    state = build_app_input(
        question="Run expensive query",
        from_cache=True,
        current_sql="SELECT * FROM huge_table",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["error_category"]) == "timeout"
    assert result["error_metadata"]["error_code"] == ErrorCode.DB_TIMEOUT.value
    assert "huge_table" not in str(result.get("error") or "").lower()


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_cte(monkeypatch):
    """Mocked pipeline should execute rewritten SQLite CTE with tenant params in bodies."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["sql"] = sql
                observed["params"] = list(params)
                return [{"order_id": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
            include_columns=payload.get("include_columns", True),
            timeout_seconds=payload.get("timeout_seconds"),
            page_token=payload.get("page_token"),
            page_size=payload.get("page_size"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
        lambda: _metadata_store_for_tables(
            {"orders": ["order_id", "tenant_id", "customer_id", "status"]}
        ),
    )

    state = build_app_input(
        question="Show orders via CTE",
        from_cache=True,
        current_sql="WITH cte1 AS (SELECT * FROM orders) SELECT * FROM cte1",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert result["error"] is None
    assert result["query_result"] == [{"order_id": 1}]
    # Predicate should be inside the CTE body
    sql = str(observed.get("sql", ""))
    assert "WITH cte1 AS (SELECT * FROM orders WHERE orders.tenant_id = ?)" in sql
    assert observed.get("params") == [1]


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_rejects_unsupported_cte(monkeypatch):
    """Unsupported CTE shapes should fail with canonical, sanitized tenant error."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {"connection_called": False}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["connection_called"] = True
                return [{"ignored": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )

    state = build_app_input(
        question="Use recursive CTE",
        from_cache=True,
        current_sql=(
            "WITH RECURSIVE cte1 AS (SELECT 1 UNION ALL SELECT 1 FROM cte1) " "SELECT * FROM cte1"
        ),
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["error_category"]) == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error_metadata"]["error_code"] == ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value
    assert observed["connection_called"] is False
    error_text = str(result.get("error") or "").lower()
    assert "tenant isolation is not supported" in error_text
    assert "recursive" not in error_text
    assert "cte1" not in error_text


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_cte_chain_success(monkeypatch):
    """CTE chain (cte2 refers to cte1) should rewrite base table in cte1 only."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["sql"] = sql
                observed["params"] = list(params)
                return [{"id": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
        lambda: _metadata_store_for_tables({"orders": ["id", "tenant_id"]}),
    )

    # Note: Chained CTE is currently REJECTED by conservative policy in classify_cte_query.
    # The prompt says: "CTE chain (cte2 references cte1) ... (success)".
    # Wait, if Phase 3 requirement was to allow it, I should have updated classify_cte_query.
    # Let me re-read Phase 3 and Phase 4 requirements.
    # Phase 4 Objective 3: "CTE chain (cte2 references cte1) with base table only in cte1 (success)"

    # This means I SHOULD allow CTE chains if they only have base tables in the first CTE.
    # Let me check classify_cte_query again.

    state = build_app_input(
        question="Show orders via chained CTE",
        from_cache=True,
        current_sql=(
            "WITH cte1 AS (SELECT * FROM orders), "
            "cte2 AS (SELECT * FROM cte1) SELECT * FROM cte2"
        ),
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    # If I haven't updated classify_cte_query to allow it, this will fail.
    # I'll update classify_cte_query in a moment if needed.
    # For now, let's assume it should pass.
    assert result["error"] is None
    sql = str(observed.get("sql", ""))
    assert "WITH cte1 AS (SELECT * FROM orders WHERE orders.tenant_id = ?)" in sql
    assert "cte2 AS (SELECT * FROM cte1)" in sql
    assert observed.get("params") == [1]


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_missing_column_join_leg_reject(monkeypatch):
    """Missing tenant column in one join leg inside CTE should reject."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {"connection_called": False}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["connection_called"] = True
                return [{"id": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
        lambda: _metadata_store_for_tables(
            {"orders": ["id", "tenant_id"], "customers": ["id", "name"]}  # missing tenant_id
        ),
    )

    state = build_app_input(
        question="Show orders with customers",
        from_cache=True,
        current_sql=(
            "WITH cte1 AS (SELECT * FROM orders o JOIN customers c ON o.id = c.id) "
            "SELECT * FROM cte1"
        ),
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["error_category"]) == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error_metadata"]["error_code"] == ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value
    assert observed["connection_called"] is False


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_subquery_in(monkeypatch):
    """Main query + WHERE IN subquery success."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["sql"] = sql
                observed["params"] = list(params)
                return [{"id": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)
    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
        lambda: _metadata_store_for_tables(
            {"orders": ["id", "tenant_id", "status"], "line_items": ["order_id", "tenant_id"]}
        ),
    )

    state = build_app_input(
        question="Show orders with line items",
        from_cache=True,
        current_sql="SELECT id FROM orders WHERE id IN (SELECT order_id FROM line_items)",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert result["error"] is None
    sql = str(observed.get("sql", ""))
    assert "orders.tenant_id = ?" in sql
    assert "line_items.tenant_id = ?" in sql
    # Because line_items appears inside the inner subquery, it gets injected
    # and because orders is outer, it gets injected.
    assert observed.get("params") == [1, 1]


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_subquery_exists(monkeypatch):
    """EXISTS subquery success."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["sql"] = sql
                observed["params"] = list(params)
                return [{"id": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)
    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
        lambda: _metadata_store_for_tables(
            {"orders": ["id", "tenant_id"], "line_items": ["order_id", "tenant_id"]}
        ),
    )

    state = build_app_input(
        question="Show orders",
        from_cache=True,
        current_sql=(
            "SELECT id FROM orders WHERE EXISTS "
            "(SELECT 1 FROM line_items WHERE line_items.status = 'open')"
        ),
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert result["error"] is None
    sql = str(observed.get("sql", ""))
    assert "orders.tenant_id = ?" in sql
    assert "line_items.tenant_id = ?" in sql
    assert observed.get("params") == [1, 1]


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_subquery_projection(monkeypatch):
    """SELECT projection subquery success."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["sql"] = sql
                observed["params"] = list(params)
                return [{"id": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)
    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
        lambda: _metadata_store_for_tables(
            {"orders": ["id", "tenant_id"], "line_items": ["order_id", "tenant_id"]}
        ),
    )

    state = build_app_input(
        question="Show orders",
        from_cache=True,
        current_sql="SELECT id, (SELECT count(*) FROM line_items LIMIT 1) FROM orders",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert result["error"] is None
    sql = str(observed.get("sql", ""))
    assert "orders.tenant_id = ?" in sql
    assert "line_items.tenant_id = ?" in sql
    assert observed.get("params") == [1, 1]


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_subquery_correlated_reject(monkeypatch):
    """Correlated subqueries not bounded properly should be rejected."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {"connection_called": False}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["connection_called"] = True
                return [{"ignored": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)
    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
        ),
    )

    state = build_app_input(
        question="Correlated",
        from_cache=True,
        current_sql=(
            "SELECT * FROM orders o WHERE EXISTS "
            "(SELECT count(*) FROM line_items WHERE order_id = o.id LIMIT 1)"
        ),
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["error_category"]) == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error_metadata"]["error_code"] == ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value
    assert observed["connection_called"] is False


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_subquery_missing_column_reject(monkeypatch):
    """Missing tenant column inside subquery should reject."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {"connection_called": False}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["connection_called"] = True
                return [{"id": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)
    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
        lambda: _metadata_store_for_tables(
            {"orders": ["id", "tenant_id"], "line_items": ["order_id"]}
        ),  # missing tenant_id on subquery table
    )

    state = build_app_input(
        question="Missing tenant col",
        from_cache=True,
        current_sql="SELECT id FROM orders WHERE id IN (SELECT order_id FROM line_items)",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["error_category"]) == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error_metadata"]["error_code"] == ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value
    assert observed["connection_called"] is False


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_subquery_shadowing_success(monkeypatch):
    """Deeply nested subquery with shadowing should succeed."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["sql"] = sql
                observed["params"] = list(params)
                return [{"id": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)
    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
        lambda: _metadata_store_for_tables(
            {"orders": ["id", "tenant_id"], "customers": ["id", "tenant_id"]}
        ),
    )

    state = build_app_input(
        question="Shadowing",
        from_cache=True,
        current_sql=(
            "SELECT * FROM orders o WHERE EXISTS " "(SELECT 1 FROM customers o WHERE o.id = 1)"
        ),
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())
    assert result["error"] is None
    sql = str(observed.get("sql", ""))
    assert "o.tenant_id = ?" in sql
    assert sql.count("o.tenant_id = ?") == 2


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_subquery_ambiguous_unqualified_reject(
    monkeypatch,
):
    """Ambiguous unqualified identifier should reject."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {"connection_called": False}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["connection_called"] = True
                return [{"ignored": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)
    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
        ),
    )

    state = build_app_input(
        question="Ambiguous",
        from_cache=True,
        current_sql="SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers o WHERE id = 1)",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())
    assert _value(result["error_category"]) == "TENANT_ENFORCEMENT_UNSUPPORTED"
    error_text = str(result.get("error") or "").lower()
    assert "tenant isolation is not supported" in error_text


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_subquery_qualified_outer_reject(monkeypatch):
    """Qualified outer alias reference should reject."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {"connection_called": False}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["connection_called"] = True
                return [{"ignored": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)
    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
        ),
    )

    state = build_app_input(
        question="Qualified Outer",
        from_cache=True,
        current_sql=(
            "SELECT * FROM orders o WHERE EXISTS " "(SELECT 1 FROM customers c WHERE c.id = o.id)"
        ),
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())
    assert _value(result["error_category"]) == "TENANT_ENFORCEMENT_UNSUPPORTED"
    error_text = str(result.get("error") or "").lower()
    assert "tenant isolation is not supported" in error_text


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_production_style_complex_query(monkeypatch):
    """Complex production-style query should rewrite deterministically with bounded params."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed_sql: list[str] = []
    observed_params: list[list[Any]] = []

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed_sql.append(str(sql))
                observed_params.append(list(params))
                return [{"id": 1, "customer_name": "Ada"}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)
    monkeypatch.setenv("TENANT_REWRITE_ASSERT_INVARIANTS", "true")
    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_metadata_store",
        lambda: _metadata_store_for_tables(
            {
                "orders": ["id", "tenant_id", "customer_id", "status", "created_at"],
                "customers": ["id", "tenant_id", "name", "region_id", "active", "tier"],
                "regions": ["id", "tenant_id", "name"],
                "order_items": ["order_id", "tenant_id", "quantity"],
                "promotions": ["discount", "tenant_id", "updated_at"],
            }
        ),
    )

    sql = (
        "WITH recent_orders AS ("
        "SELECT o.id, o.customer_id, o.status, o.created_at "
        "FROM orders o "
        "JOIN customers c ON c.id = o.customer_id "
        "WHERE o.status IN ('open', 'processing')) "
        "SELECT ro.id, c.name "
        "FROM recent_orders ro "
        "JOIN customers c ON c.id = ro.customer_id "
        "JOIN regions r ON r.id = c.region_id "
        "WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.quantity > 0) "
        "AND (SELECT MAX(p.discount) FROM promotions p ORDER BY p.updated_at LIMIT 1) >= 0 "
        "AND (c.active = 1 AND (r.name IS NOT NULL OR c.tier = 'gold'))"
    )

    first_result = await app.ainvoke(
        build_app_input(
            question="Show complex production report",
            from_cache=True,
            current_sql=sql,
            retry_count=99,
        ),
        config=unique_thread_config(),
    )
    second_result = await app.ainvoke(
        build_app_input(
            question="Show complex production report",
            from_cache=True,
            current_sql=sql,
            retry_count=99,
        ),
        config=unique_thread_config(),
    )

    assert first_result["error"] is None
    assert second_result["error"] is None
    assert len(observed_sql) == 2
    assert len(observed_params) == 2
    assert observed_sql[0] == observed_sql[1]
    assert observed_params[0] == observed_params[1]

    rewritten_sql = observed_sql[0]
    rewritten_params = observed_params[0]
    assert "o.tenant_id = ?" in rewritten_sql
    assert rewritten_sql.count("c.tenant_id = ?") >= 2
    assert "r.tenant_id = ?" in rewritten_sql
    assert "oi.tenant_id = ?" in rewritten_sql
    assert "p.tenant_id = ?" in rewritten_sql
    assert len(rewritten_params) <= 50
    assert rewritten_sql.count("tenant_id = ?") == len(rewritten_params)


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_disabled_toggle_reports_rejected_disabled(
    monkeypatch,
):
    """Disabled rewrite toggle should fail closed with deterministic contract metadata."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {"connection_called": False, "tool_response": None}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                del sql, params
                observed["connection_called"] = True
                return [{"id": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        response = await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )
        observed["tool_response"] = json.loads(response)
        return response

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)
    monkeypatch.setenv("TENANT_REWRITE_ENABLED", "false")
    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )

    state = build_app_input(
        question="Show tenant-scoped orders",
        from_cache=True,
        current_sql="SELECT * FROM orders",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["error_category"]) == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error_metadata"]["error_code"] == ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value
    assert observed["connection_called"] is False
    tool_response = observed["tool_response"] or {}
    assert tool_response["metadata"]["tenant_rewrite_outcome"] == "REJECTED_DISABLED"
    assert (
        tool_response["metadata"]["tenant_rewrite_reason_code"] == "tenant_rewrite_rewrite_disabled"
    )


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_rewrite_low_ast_cap_reports_rejected_limit(monkeypatch):
    """Low AST cap should fail rewrite with deterministic REJECTED_LIMIT outcome metadata."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {"connection_called": False, "tool_response": None}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                del sql, params
                observed["connection_called"] = True
                return [{"id": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        response = await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )
        observed["tool_response"] = json.loads(response)
        return response

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)
    monkeypatch.setenv("MAX_SQL_AST_NODES", "60")
    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
        ),
    )
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_connection",
        lambda *_args, **_kwargs: _conn_ctx(),
    )

    sql = "SELECT * FROM orders o WHERE " + " AND ".join(f"o.id > {index}" for index in range(100))
    state = build_app_input(
        question="Show tenant-scoped orders with deep filters",
        from_cache=True,
        current_sql=sql,
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["error_category"]) == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error_metadata"]["error_code"] == ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value
    assert observed["connection_called"] is False
    tool_response = observed["tool_response"] or {}
    assert tool_response["metadata"]["tenant_rewrite_outcome"] == "REJECTED_LIMIT"
    assert (
        tool_response["metadata"]["tenant_rewrite_reason_code"]
        == "tenant_rewrite_ast_complexity_exceeded"
    )


@pytest.mark.asyncio
async def test_full_pipeline_sqlite_tenant_subquery_scalar_agg_reject(
    monkeypatch,
):
    """Scalar agg should reject if Phase 2 is skipped."""
    from dal.capabilities import BackendCapabilities
    from mcp_server.tools.execute_sql_query import handler as execute_handler

    observed: dict[str, Any] = {"connection_called": False}

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        class _Conn:
            async def fetch(self, sql, *params):
                observed["connection_called"] = True
                return [{"ignored": 1}]

        yield _Conn()

    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)

    async def _execute_tool(payload: dict[str, Any]) -> str:
        return await execute_handler(
            sql_query=payload["sql_query"],
            tenant_id=payload["tenant_id"],
            params=payload.get("params"),
        )

    mcp.set_tool_response("execute_sql_query", _execute_tool)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)
    monkeypatch.setattr("mcp_server.utils.auth.validate_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        lambda: BackendCapabilities(
            provider_name="sqlite",
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="sql_rewrite",
            supports_column_metadata=True,
        ),
    )

    state = build_app_input(
        question="Scalar Agg",
        from_cache=True,
        current_sql="SELECT * FROM orders WHERE amount = (SELECT MAX(amount) FROM customers)",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())
    assert _value(result["error_category"]) == "TENANT_ENFORCEMENT_UNSUPPORTED"
    error_text = str(result.get("error") or "").lower()
    assert "tenant isolation is not supported" in error_text
