"""Parity/regression coverage for federated pagination classification contracts."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState
from dal.capabilities import BackendCapabilities
from dal.keyset_pagination import encode_keyset_cursor
from mcp_server.tools.execute_sql_query import _validate_sql_ast_failure
from mcp_server.tools.execute_sql_query import handler as mcp_execute_sql_query_handler

pytestmark = pytest.mark.pagination

_TEST_SECRET = "test-pagination-secret"
_BUDGET_SNAPSHOT = {
    "max_total_rows": 1000,
    "max_total_bytes": 1_000_000,
    "max_total_duration_ms": 60_000,
    "consumed_rows": 0,
    "consumed_bytes": 0,
    "consumed_duration_ms": 0,
}


class _BackendSetConn:
    def __init__(
        self,
        backend_set: list[dict[str, str]] | None,
        rows_by_call: list[list[dict[str, int | str]]] | None = None,
    ):
        self.session_guardrail_metadata = {}
        self.backend_set = backend_set
        self._rows_by_call = rows_by_call or [[{"id": 1}, {"id": 2}, {"id": 3}]]
        self._call_count = 0

    async def fetch(self, *_args, **_kwargs):
        index = min(self._call_count, len(self._rows_by_call) - 1)
        self._call_count += 1
        return list(self._rows_by_call[index])


def _policy_mock() -> MagicMock:
    policy = MagicMock()
    policy.evaluate = AsyncMock(
        return_value=MagicMock(
            should_execute=True,
            sql_to_execute="SELECT id FROM users ORDER BY id",
            params_to_bind=[],
            envelope_metadata={},
            telemetry_attributes={},
        )
    )
    policy.default_decision.return_value = MagicMock(telemetry_attributes={}, envelope_metadata={})
    return policy


async def _invoke_mcp_federated_keyset(
    caps: BackendCapabilities,
    *,
    keyset_cursor: str | None = None,
    backend_set: list[dict[str, str]] | None = None,
    sql: str = "SELECT id FROM users ORDER BY id",
    page_size: int = 1,
    rows_by_call: list[list[dict[str, int | str]]] | None = None,
) -> dict:
    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _BackendSetConn(backend_set, rows_by_call=rows_by_call)

    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="federated-db",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection", return_value=_conn_ctx()
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=False),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.tools.execute_sql_query._validate_sql_ast", return_value=None),
        patch(
            "mcp_server.tools.execute_sql_query._validate_sql_complexity", return_value=(None, {})
        ),
        patch("dal.util.read_only.enforce_read_only_sql", return_value=None),
        patch(
            "common.security.tenant_enforcement_policy.TenantEnforcementPolicy",
            return_value=_policy_mock(),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.normalize_sqlglot_dialect", return_value="postgres"
        ),
    ):
        payload = await mcp_execute_sql_query_handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=keyset_cursor,
            page_size=page_size,
        )
    return json.loads(payload)


async def _run_agent_with_tool_payload(payload: dict) -> dict:
    state = AgentState(
        messages=[HumanMessage(content="test")],
        current_sql="SELECT id FROM users ORDER BY id",
        tenant_id=1,
        retry_count=0,
    )
    with (
        patch("agent.nodes.execute.get_mcp_tools") as mock_get_tools,
        patch("agent.nodes.execute.PolicyEnforcer.validate_sql", return_value=None),
        patch(
            "agent.nodes.execute.TenantRewriter.rewrite_sql",
            side_effect=lambda sql, _tenant_id: sql,
        ),
    ):
        tool = MagicMock()
        tool.name = "execute_sql_query"
        tool.ainvoke = AsyncMock(return_value=json.dumps(payload))
        mock_get_tools.return_value = [tool]
        return await validate_and_execute_node(state)


def _reason_code_from_agent_error_metadata(error_metadata: dict) -> str | None:
    if not isinstance(error_metadata, dict):
        return None
    if isinstance(error_metadata.get("reason_code"), str):
        return error_metadata["reason_code"]
    details_safe = error_metadata.get("details_safe")
    if isinstance(details_safe, dict) and isinstance(details_safe.get("reason_code"), str):
        return details_safe.get("reason_code")
    return None


def _tamper_keyset_cursor(
    cursor: str,
    *,
    issued_at: int | None = None,
    max_age_s: int | None = None,
    secret: str = _TEST_SECRET,
) -> str:
    """Mutate keyset cursor payload fields and re-sign for regression coverage."""
    import hashlib
    import hmac as _hmac

    padded = cursor + "=" * (-len(cursor) % 4)
    payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    payload.pop("s", None)
    if issued_at is not None:
        payload["issued_at"] = issued_at
    if max_age_s is not None:
        payload["max_age_s"] = max_age_s
    if secret:
        sig_data = json.dumps(payload, sort_keys=True)
        payload["s"] = _hmac.new(secret.encode(), sig_data.encode(), hashlib.sha256).hexdigest()
    raw = json.dumps(payload, sort_keys=True)
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


async def _assert_agent_reason_parity(mcp_result: dict, expected_reason_code: str) -> None:
    assert mcp_result["error"]["details_safe"]["reason_code"] == expected_reason_code
    agent_result = await _run_agent_with_tool_payload(mcp_result)
    assert agent_result["error_category"] == mcp_result["error"]["category"]
    assert (
        _reason_code_from_agent_error_metadata(agent_result["error_metadata"])
        == expected_reason_code
    )
    assert agent_result["error_metadata"]["error_code"] == mcp_result["error"]["error_code"]


@pytest.mark.asyncio
async def test_federated_keyset_rejection_classification_parity_between_mcp_and_agent():
    """Agent must preserve MCP federated-keyset rejection classification metadata."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_federated_deterministic_ordering=False,
        supports_keyset=True,
        supports_keyset_with_containment=True,
        supports_pagination=True,
    )

    mcp_result = await _invoke_mcp_federated_keyset(caps)
    assert (
        mcp_result["error"]["details_safe"]["reason_code"] == "PAGINATION_FEDERATED_ORDERING_UNSAFE"
    )

    agent_result = await _run_agent_with_tool_payload(mcp_result)
    assert agent_result["error_category"] == mcp_result["error"]["category"]
    assert (
        _reason_code_from_agent_error_metadata(agent_result["error_metadata"])
        == mcp_result["error"]["details_safe"]["reason_code"]
    )
    assert agent_result["error_metadata"]["error_code"] == mcp_result["error"]["error_code"]


@pytest.mark.asyncio
async def test_backend_set_mismatch_classification_parity_between_mcp_and_agent():
    """Agent must preserve MCP backend-set drift rejection classification metadata."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_federated_deterministic_ordering=True,
        supports_keyset=True,
        supports_keyset_with_containment=True,
        supports_column_metadata=True,
        supports_pagination=True,
    )
    backend_set_ab = [
        {"backend_id": "db-a", "region": "us-east-1", "role": "primary"},
        {"backend_id": "db-b", "region": "us-east-1", "role": "replica"},
    ]
    page_one = await _invoke_mcp_federated_keyset(caps, backend_set=backend_set_ab)
    assert "error" not in page_one
    cursor = page_one["metadata"]["next_keyset_cursor"]
    assert cursor

    page_two = await _invoke_mcp_federated_keyset(
        caps,
        keyset_cursor=cursor,
        backend_set=[{"backend_id": "db-a", "region": "us-east-1", "role": "primary"}],
    )
    assert page_two["error"]["details_safe"]["reason_code"] == "PAGINATION_BACKEND_SET_CHANGED"

    agent_result = await _run_agent_with_tool_payload(page_two)
    assert agent_result["error_category"] == page_two["error"]["category"]
    assert (
        _reason_code_from_agent_error_metadata(agent_result["error_metadata"])
        == page_two["error"]["details_safe"]["reason_code"]
    )
    assert agent_result["error_metadata"]["error_code"] == page_two["error"]["error_code"]


@pytest.mark.asyncio
async def test_agent_allows_federated_keyset_when_ordering_support_enabled():
    """Regression: federated keyset should succeed when ordering capability is explicit."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_federated_deterministic_ordering=True,
        supports_keyset=True,
        supports_keyset_with_containment=True,
        supports_column_metadata=True,
        supports_pagination=True,
    )
    backend_set_ab = [
        {"backend_id": "db-a", "region": "us-east-1", "role": "primary"},
        {"backend_id": "db-b", "region": "us-east-1", "role": "replica"},
    ]
    mcp_result = await _invoke_mcp_federated_keyset(caps, backend_set=backend_set_ab)
    assert "error" not in mcp_result

    agent_result = await _run_agent_with_tool_payload(mcp_result)
    assert agent_result.get("error") is None
    assert agent_result.get("error_category") is None
    assert isinstance(agent_result.get("query_result"), list)


@pytest.mark.asyncio
async def test_cursor_expired_classification_parity_between_mcp_and_agent():
    """Agent must preserve MCP classification for expired keyset cursors."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_federated_deterministic_ordering=True,
        supports_keyset=True,
        supports_keyset_with_containment=True,
        supports_column_metadata=True,
        supports_pagination=True,
    )
    page_one = await _invoke_mcp_federated_keyset(caps)
    assert "error" not in page_one
    cursor = page_one["metadata"]["next_keyset_cursor"]
    assert cursor

    expired_cursor = _tamper_keyset_cursor(cursor, issued_at=0, max_age_s=1)
    page_two = await _invoke_mcp_federated_keyset(caps, keyset_cursor=expired_cursor)
    assert page_two["error"]["details_safe"]["reason_code"] == "PAGINATION_CURSOR_EXPIRED"

    agent_result = await _run_agent_with_tool_payload(page_two)
    assert agent_result["error_category"] == page_two["error"]["category"]
    assert (
        _reason_code_from_agent_error_metadata(agent_result["error_metadata"])
        == page_two["error"]["details_safe"]["reason_code"]
    )
    assert agent_result["error_metadata"]["error_code"] == page_two["error"]["error_code"]


@pytest.mark.asyncio
async def test_cursor_query_mismatch_classification_parity_between_mcp_and_agent():
    """Agent must preserve MCP classification for strict query-fingerprint mismatches."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_federated_deterministic_ordering=True,
        supports_keyset=True,
        supports_keyset_with_containment=True,
        supports_column_metadata=True,
        supports_pagination=True,
    )
    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "stable-fingerprint",
        query_fp="cursor-query-fp",
        secret=_TEST_SECRET,
        budget_snapshot=_BUDGET_SNAPSHOT,
    )
    with (
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint",
            return_value="stable-fingerprint",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.build_cursor_query_fingerprint",
            return_value="expected-query-fp",
        ),
    ):
        mcp_result = await _invoke_mcp_federated_keyset(
            caps,
            keyset_cursor=cursor,
            sql="SELECT id FROM users WHERE note = 'LEAK_SENTINEL_CURSOR_999' ORDER BY id",
        )

    assert mcp_result["error"]["details_safe"]["reason_code"] == "PAGINATION_CURSOR_QUERY_MISMATCH"
    serialized = json.dumps(mcp_result)
    assert "LEAK_SENTINEL_CURSOR_999" not in serialized
    assert cursor not in serialized

    agent_result = await _run_agent_with_tool_payload(mcp_result)
    assert agent_result["error_category"] == mcp_result["error"]["category"]
    assert (
        _reason_code_from_agent_error_metadata(agent_result["error_metadata"])
        == mcp_result["error"]["details_safe"]["reason_code"]
    )
    assert agent_result["error_metadata"]["error_code"] == mcp_result["error"]["error_code"]


@pytest.mark.asyncio
async def test_global_row_budget_classification_parity_between_mcp_ast_and_agent(monkeypatch):
    """Row-budget rejection classification should match across AST, MCP envelope, and agent."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_federated_deterministic_ordering=True,
        supports_keyset=True,
        supports_keyset_with_containment=True,
        supports_column_metadata=True,
        supports_pagination=True,
    )
    sql = "SELECT id FROM users ORDER BY id"
    assert _validate_sql_ast_failure(sql, "postgres") is None
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_ROWS", "100")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_BYTES", "1000000")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_EXECUTION_MS", "100000")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_TIMEOUT", "true")

    page_one = await _invoke_mcp_federated_keyset(
        caps,
        sql=sql,
        page_size=80,
        rows_by_call=[[{"id": i} for i in range(81)]],
    )
    assert "error" not in page_one
    cursor = page_one["metadata"]["next_keyset_cursor"]
    assert cursor

    page_two = await _invoke_mcp_federated_keyset(
        caps,
        sql=sql,
        keyset_cursor=cursor,
        page_size=30,
        rows_by_call=[[{"id": 1000 + i} for i in range(31)]],
    )
    await _assert_agent_reason_parity(page_two, "PAGINATION_GLOBAL_ROW_BUDGET_EXCEEDED")


@pytest.mark.asyncio
async def test_global_byte_budget_classification_parity_between_mcp_ast_and_agent(monkeypatch):
    """Byte-budget rejection classification should match across AST, MCP envelope, and agent."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_federated_deterministic_ordering=True,
        supports_keyset=True,
        supports_keyset_with_containment=True,
        supports_column_metadata=True,
        supports_pagination=True,
    )
    sql = "SELECT id, blob FROM users ORDER BY id"
    assert _validate_sql_ast_failure(sql, "postgres") is None
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_ROWS", "1000")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_BYTES", "300")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_EXECUTION_MS", "100000")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_TIMEOUT", "true")

    page_one = await _invoke_mcp_federated_keyset(
        caps,
        sql=sql,
        page_size=1,
        rows_by_call=[[{"id": 1, "blob": "x" * 128}, {"id": 2, "blob": "y" * 128}]],
    )
    assert "error" not in page_one
    cursor = page_one["metadata"]["next_keyset_cursor"]
    assert cursor

    page_two = await _invoke_mcp_federated_keyset(
        caps,
        sql=sql,
        keyset_cursor=cursor,
        page_size=1,
        rows_by_call=[[{"id": 3, "blob": "x" * 128}, {"id": 4, "blob": "y" * 128}]],
    )
    await _assert_agent_reason_parity(page_two, "PAGINATION_GLOBAL_BYTE_BUDGET_EXCEEDED")


@pytest.mark.asyncio
async def test_global_time_budget_classification_parity_between_mcp_ast_and_agent(monkeypatch):
    """Time-budget rejection classification should match across AST, MCP envelope, and agent."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_federated_deterministic_ordering=True,
        supports_keyset=True,
        supports_keyset_with_containment=True,
        supports_column_metadata=True,
        supports_pagination=True,
    )
    sql = "SELECT id FROM users ORDER BY id"
    assert _validate_sql_ast_failure(sql, "postgres") is None
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_ROWS", "1000")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_BYTES", "1000000")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_EXECUTION_MS", "100")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_TIMEOUT", "true")

    async def _run_without_timeout(operation, timeout_seconds, cancel=None, **_kwargs):
        _ = (timeout_seconds, cancel)
        return await operation()

    monotonic_values = iter([0.0, 0.06, 1.0, 1.07, 1.08])
    with (
        patch(
            "mcp_server.tools.execute_sql_query.run_with_timeout",
            side_effect=_run_without_timeout,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.time.monotonic",
            side_effect=lambda: next(monotonic_values),
        ),
    ):
        page_one = await _invoke_mcp_federated_keyset(
            caps,
            sql=sql,
            page_size=1,
            rows_by_call=[[{"id": 1}, {"id": 2}]],
        )
        assert "error" not in page_one
        cursor = page_one["metadata"]["next_keyset_cursor"]
        assert cursor

        page_two = await _invoke_mcp_federated_keyset(
            caps,
            sql=sql,
            keyset_cursor=cursor,
            page_size=1,
            rows_by_call=[[{"id": 3}, {"id": 4}]],
        )
    await _assert_agent_reason_parity(page_two, "PAGINATION_GLOBAL_TIME_BUDGET_EXCEEDED")


@pytest.mark.asyncio
async def test_budget_snapshot_invalid_classification_parity_between_mcp_ast_and_agent():
    """Budget-snapshot rejection classification should match across AST, MCP envelope, and agent."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_federated_deterministic_ordering=True,
        supports_keyset=True,
        supports_keyset_with_containment=True,
        supports_column_metadata=True,
        supports_pagination=True,
    )
    sql = "SELECT id FROM users ORDER BY id"
    assert _validate_sql_ast_failure(sql, "postgres") is None

    page_one = await _invoke_mcp_federated_keyset(
        caps,
        sql=sql,
        page_size=2,
        rows_by_call=[[{"id": 1}, {"id": 2}, {"id": 3}]],
    )
    assert "error" not in page_one
    cursor = page_one["metadata"]["next_keyset_cursor"]
    assert cursor

    padded = cursor + "=" * (-len(cursor) % 4)
    payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    payload["budget_snapshot"]["consumed_rows"] = (
        int(payload["budget_snapshot"]["consumed_rows"]) + 1
    )
    payload.pop("s", None)
    signature_data = json.dumps(payload, sort_keys=True)
    payload["s"] = hmac.new(
        _TEST_SECRET.encode("utf-8"), signature_data.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    tampered_cursor = (
        base64.urlsafe_b64encode(json.dumps(payload, sort_keys=True).encode("utf-8"))
        .decode("ascii")
        .rstrip("=")
    )

    page_two = await _invoke_mcp_federated_keyset(
        caps,
        sql=sql,
        keyset_cursor=tampered_cursor,
        page_size=2,
        rows_by_call=[[{"id": 3}]],
    )
    await _assert_agent_reason_parity(page_two, "PAGINATION_BUDGET_SNAPSHOT_INVALID")
