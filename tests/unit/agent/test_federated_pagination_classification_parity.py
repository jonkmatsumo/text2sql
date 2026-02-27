"""Parity/regression coverage for federated pagination classification contracts."""

from __future__ import annotations

import base64
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState
from dal.capabilities import BackendCapabilities
from dal.keyset_pagination import encode_keyset_cursor
from mcp_server.tools.execute_sql_query import handler as mcp_execute_sql_query_handler


class _BackendSetConn:
    def __init__(self, backend_set: list[dict[str, str]] | None):
        self.session_guardrail_metadata = {}
        self.backend_set = backend_set

    async def fetch(self, *_args, **_kwargs):
        return [{"id": 1}, {"id": 2}, {"id": 3}]


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
) -> dict:
    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _BackendSetConn(backend_set)

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
            page_size=1,
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
    cursor: str, *, issued_at: int | None = None, max_age_s: int | None = None
) -> str:
    """Mutate keyset cursor payload fields for replay/ttl regression coverage."""
    padded = cursor + "=" * (-len(cursor) % 4)
    payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    if issued_at is not None:
        payload["issued_at"] = issued_at
    if max_age_s is not None:
        payload["max_age_s"] = max_age_s
    return base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii").rstrip("=")


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
