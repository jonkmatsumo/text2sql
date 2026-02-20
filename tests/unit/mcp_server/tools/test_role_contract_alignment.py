"""Tests for role-enforcement and doc/contract alignment in MCP tools."""

import json

import pytest

from mcp_server.tools.conversation.load_conversation_state import (
    handler as load_conversation_state_handler,
)
from mcp_server.tools.conversation.save_conversation_state import (
    handler as save_conversation_state_handler,
)
from mcp_server.tools.feedback.submit_feedback import handler as submit_feedback_handler
from mcp_server.tools.get_few_shot_examples import handler as get_few_shot_examples_handler
from mcp_server.tools.get_semantic_definitions import handler as get_semantic_definitions_handler
from mcp_server.tools.get_semantic_subgraph import handler as get_semantic_subgraph_handler
from mcp_server.tools.interaction.create_interaction import handler as create_interaction_handler
from mcp_server.tools.interaction.update_interaction import handler as update_interaction_handler
from mcp_server.tools.lookup_cache import handler as lookup_cache_handler
from mcp_server.tools.recommend_examples import handler as recommend_examples_handler
from mcp_server.tools.resolve_ambiguity import handler as resolve_ambiguity_handler
from mcp_server.tools.update_cache import handler as update_cache_handler

ENFORCED_SQL_USER_ROLE_HANDLERS = [
    ("lookup_cache", lookup_cache_handler, {"query": "test", "tenant_id": 1}),
    (
        "get_semantic_definitions",
        get_semantic_definitions_handler,
        {"terms": ["Revenue"], "tenant_id": 1},
    ),
    (
        "recommend_examples",
        recommend_examples_handler,
        {"query": "test", "tenant_id": 1, "limit": 1},
    ),
    ("update_cache", update_cache_handler, {"query": "test", "sql": "SELECT 1", "tenant_id": 1}),
    (
        "get_few_shot_examples",
        get_few_shot_examples_handler,
        {"query": "test", "tenant_id": 1, "limit": 1},
    ),
    (
        "get_semantic_subgraph",
        get_semantic_subgraph_handler,
        {"query": "test", "tenant_id": 1},
    ),
    (
        "submit_feedback",
        submit_feedback_handler,
        {"interaction_id": "i-1", "thumb": "UP", "comment": None},
    ),
    (
        "create_interaction",
        create_interaction_handler,
        {
            "conversation_id": "c-1",
            "schema_snapshot_id": "s-1",
            "user_nlq_text": "test",
            "tenant_id": 1,
        },
    ),
    (
        "update_interaction",
        update_interaction_handler,
        {"interaction_id": "i-1", "tenant_id": 1},
    ),
    (
        "save_conversation_state",
        save_conversation_state_handler,
        {
            "conversation_id": "c-1",
            "user_id": "u-1",
            "tenant_id": 1,
            "state_json": {"messages": []},
            "version": 1,
            "ttl_minutes": 10,
        },
    ),
    (
        "load_conversation_state",
        load_conversation_state_handler,
        {"conversation_id": "c-1", "user_id": "u-1", "tenant_id": 1},
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("tool_name", "handler", "kwargs"), ENFORCED_SQL_USER_ROLE_HANDLERS)
async def test_enforced_sql_user_role_tools_reject_missing_role(
    monkeypatch,
    tool_name: str,
    handler,
    kwargs: dict,
):
    """Enforced SQL_USER_ROLE tools should reject requests without required role."""
    monkeypatch.setenv("MCP_USER_ROLE", "")

    response = await handler(**kwargs)
    payload = json.loads(response)

    assert payload["error"]["category"] == "unauthorized"
    assert payload["error"]["sql_state"] == "UNAUTHORIZED_ROLE"
    assert tool_name in payload["error"]["message"]


@pytest.mark.asyncio
async def test_non_enforced_resolve_ambiguity_remains_callable_without_role(
    monkeypatch,
):
    """resolve_ambiguity should remain callable when role enforcement is upstream-only."""
    monkeypatch.setenv("MCP_USER_ROLE", "")

    class _DummyResolver:
        def resolve(self, query, schema_context):
            _ = (query, schema_context)
            return {"status": "CLEAR", "resolved_bindings": {}, "ambiguities": []}

    monkeypatch.setattr("mcp_server.tools.resolve_ambiguity.get_resolver", lambda: _DummyResolver())

    response = await resolve_ambiguity_handler(query="find records", schema_context=[])
    payload = json.loads(response)

    assert payload.get("error") is None
    assert payload["result"]["status"] == "CLEAR"
