"""Contract tests for MCP tool response envelope shapes."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _assert_generic_envelope_shape(payload: dict) -> None:
    assert payload["schema_version"] == "1.0"
    assert "result" in payload
    assert "metadata" in payload
    assert isinstance(payload["metadata"], dict)
    assert payload["metadata"]["tool_version"] == "v1"
    assert "provider" in payload["metadata"]


def _assert_execute_envelope_shape(payload: dict) -> None:
    assert payload["schema_version"] == "1.0"
    assert "rows" in payload
    assert "metadata" in payload
    assert isinstance(payload["metadata"], dict)
    assert payload["metadata"]["tool_version"] == "v1"
    assert "rows_returned" in payload["metadata"]
    assert "is_truncated" in payload["metadata"]


@pytest.fixture(autouse=True)
def _allow_roles(monkeypatch):
    """Grant all required MCP roles for contract tests."""
    monkeypatch.setenv("MCP_USER_ROLE", "SQL_ADMIN_ROLE,TABLE_ADMIN_ROLE,SQL_USER_ROLE")


@pytest.mark.asyncio
async def test_core_schema_tool_contracts():
    """Core schema retrieval tools should keep the generic response envelope contract."""
    from mcp_server.tools.get_sample_data import handler as sample_handler
    from mcp_server.tools.get_table_schema import handler as schema_handler
    from mcp_server.tools.list_tables import handler as list_handler

    mock_store = AsyncMock()
    mock_store.list_tables.return_value = ["users"]
    mock_store.get_table_definition = AsyncMock(
        return_value=json.dumps({"table_name": "users", "columns": []})
    )

    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[{"id": 1}])
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("mcp_server.tools.list_tables.Database.get_metadata_store", return_value=mock_store),
        patch(
            "mcp_server.tools.get_table_schema.Database.get_metadata_store", return_value=mock_store
        ),
        patch("dal.database.Database.get_connection", return_value=mock_conn),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
    ):
        list_payload = json.loads(await list_handler(tenant_id=1))
        schema_payload = json.loads(await schema_handler(["users"], tenant_id=1))
        sample_payload = json.loads(await sample_handler("users", tenant_id=1))

    _assert_generic_envelope_shape(list_payload)
    _assert_generic_envelope_shape(schema_payload)
    _assert_generic_envelope_shape(sample_payload)


@pytest.mark.asyncio
async def test_semantic_retrieval_tool_contracts():
    """Semantic retrieval tools should preserve generic envelope keys and metadata."""
    from mcp_server.tools.get_semantic_definitions import handler as definitions_handler
    from mcp_server.tools.get_semantic_subgraph import handler as subgraph_handler
    from mcp_server.tools.search_relevant_tables import handler as search_handler

    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(
        return_value=[
            {
                "term_name": "gmv",
                "definition": "Gross merchandise value",
                "sql_logic": "SUM(amount)",
            }
        ]
    )
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    mock_cache = MagicMock()
    mock_cache.lookup = AsyncMock(return_value=None)
    mock_cache.store = AsyncMock(return_value=None)

    introspector = MagicMock()
    introspector.get_table_def = AsyncMock(
        return_value=SimpleNamespace(
            description="orders table",
            columns=[SimpleNamespace(name="id", data_type="int", is_nullable=False)],
        )
    )

    with (
        patch(
            "mcp_server.tools.get_semantic_definitions.Database.get_connection",
            return_value=mock_conn,
        ),
        patch(
            "mcp_server.tools.get_semantic_subgraph.Database.get_cache_store",
            return_value=mock_cache,
        ),
        patch(
            "mcp_server.tools.get_semantic_subgraph.Database.get_graph_store", return_value=object()
        ),
        patch(
            "mcp_server.tools.get_semantic_subgraph.Database.get_query_target_provider",
            return_value="postgres",
        ),
        patch(
            "mcp_server.tools.get_semantic_subgraph.RagEngine.embed_text",
            AsyncMock(return_value=[0.1]),
        ),
        patch(
            "mcp_server.tools.get_semantic_subgraph._get_mini_graph",
            AsyncMock(return_value={"nodes": [], "relationships": []}),
        ),
        patch(
            "mcp_server.tools.search_relevant_tables.RagEngine.embed_text",
            AsyncMock(return_value=[0.2]),
        ),
        patch(
            "mcp_server.tools.search_relevant_tables.search_similar_tables",
            AsyncMock(
                return_value=[
                    {"table_name": "orders", "schema_text": "orders table", "distance": 0.1}
                ]
            ),
        ),
        patch(
            "mcp_server.tools.search_relevant_tables.Database.get_schema_introspector",
            return_value=introspector,
        ),
        patch(
            "mcp_server.tools.search_relevant_tables.Database.get_query_target_provider",
            return_value="postgres",
        ),
    ):
        definitions_payload = json.loads(await definitions_handler(["gmv"], tenant_id=1))
        subgraph_payload = json.loads(await subgraph_handler("orders by day", tenant_id=1))
        search_payload = json.loads(await search_handler("orders by day", tenant_id=1))

    _assert_generic_envelope_shape(definitions_payload)
    _assert_generic_envelope_shape(subgraph_payload)
    _assert_generic_envelope_shape(search_payload)


@pytest.mark.asyncio
async def test_execution_and_resolution_tool_contracts():
    """Execution and ambiguity tools should keep their expected envelopes."""
    from dal.capabilities import BackendCapabilities
    from dal.database import Database
    from mcp_server.tools.execute_sql_query import handler as execute_handler
    from mcp_server.tools.resolve_ambiguity import handler as resolve_handler

    Database._query_target_capabilities = BackendCapabilities(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
        supports_schema_cache=False,
    )
    Database._query_target_provider = "postgres"

    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[{"one": 1}])
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    resolver = MagicMock()
    resolver.resolve.return_value = {
        "status": "OK",
        "ambiguities": [],
        "resolved_bindings": {},
    }

    with (
        patch("mcp_server.tools.execute_sql_query.Database.get_connection", return_value=mock_conn),
        patch("mcp_server.tools.resolve_ambiguity.get_resolver", return_value=resolver),
    ):
        execute_payload = json.loads(await execute_handler("SELECT 1 AS one", tenant_id=1))
        resolve_payload = json.loads(await resolve_handler("top orders", [{"name": "orders"}]))

    _assert_execute_envelope_shape(execute_payload)
    _assert_generic_envelope_shape(resolve_payload)


@pytest.mark.asyncio
async def test_cache_and_recommendation_tool_contracts():
    """Cache and recommendation tools should keep generic envelope shape compatibility."""
    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from mcp_server.tools.get_few_shot_examples import handler as fewshot_handler
    from mcp_server.tools.lookup_cache import handler as lookup_handler
    from mcp_server.tools.recommend_examples import handler as recommend_handler
    from mcp_server.tools.update_cache import handler as update_handler

    recommend_result = MagicMock()
    recommend_result.model_dump.return_value = {"examples": [], "fallback_used": False}
    fewshot_payload = ToolResponseEnvelope(
        result=[{"question": "q", "sql": "SELECT 1"}],
        metadata=GenericToolMetadata(provider="registry"),
    ).model_dump_json(exclude_none=True)

    with (
        patch("mcp_server.tools.lookup_cache.lookup_cache_svc", AsyncMock(return_value=None)),
        patch("mcp_server.tools.update_cache.update_cache_svc", AsyncMock(return_value=None)),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "mcp_server.tools.recommend_examples.RecommendationService.recommend_examples",
            AsyncMock(return_value=recommend_result),
        ),
        patch(
            "mcp_server.tools.get_few_shot_examples.get_relevant_examples",
            AsyncMock(return_value=fewshot_payload),
        ),
    ):
        lookup_payload = json.loads(await lookup_handler("show orders", tenant_id=1))
        update_payload = json.loads(await update_handler("show orders", "SELECT 1", tenant_id=1))
        recommend_payload = json.loads(await recommend_handler("show orders", tenant_id=1))
        fewshot_result_payload = json.loads(await fewshot_handler("show orders", tenant_id=1))

    _assert_generic_envelope_shape(lookup_payload)
    _assert_generic_envelope_shape(update_payload)
    _assert_generic_envelope_shape(recommend_payload)
    _assert_generic_envelope_shape(fewshot_result_payload)


@pytest.mark.asyncio
async def test_persistence_and_feedback_tool_contracts():
    """Interaction, conversation, and feedback tools should keep generic response contracts."""
    from mcp_server.tools.conversation.load_conversation_state import handler as load_handler
    from mcp_server.tools.conversation.save_conversation_state import handler as save_handler
    from mcp_server.tools.feedback.submit_feedback import handler as feedback_handler
    from mcp_server.tools.interaction.create_interaction import handler as create_handler
    from mcp_server.tools.interaction.update_interaction import handler as update_handler

    conversation_store = MagicMock()
    conversation_store.save_state_async = AsyncMock(return_value=None)
    conversation_store.load_state_async = AsyncMock(return_value={"turns": []})

    interaction_store = MagicMock()
    interaction_store.create_interaction = AsyncMock(return_value="interaction-1")
    interaction_store.update_interaction_result = AsyncMock(return_value=None)

    feedback_store = MagicMock()
    feedback_store.create_feedback = AsyncMock(return_value=None)
    feedback_store.ensure_review_queue = AsyncMock(return_value=None)

    with (
        patch(
            "mcp_server.tools.conversation.save_conversation_state.get_conversation_store",
            return_value=conversation_store,
        ),
        patch(
            "mcp_server.tools.conversation.load_conversation_state.get_conversation_store",
            return_value=conversation_store,
        ),
        patch(
            "mcp_server.tools.interaction.create_interaction.get_interaction_store",
            return_value=interaction_store,
        ),
        patch(
            "mcp_server.tools.interaction.update_interaction.get_interaction_store",
            return_value=interaction_store,
        ),
        patch(
            "mcp_server.tools.feedback.submit_feedback.get_feedback_store",
            return_value=feedback_store,
        ),
    ):
        save_payload = json.loads(
            await save_handler(
                conversation_id="c1",
                user_id="u1",
                tenant_id=1,
                state_json={"turns": []},
                version=1,
            )
        )
        load_payload = json.loads(await load_handler("c1", "u1", tenant_id=1))
        create_payload = json.loads(
            await create_handler(
                conversation_id="c1",
                schema_snapshot_id="snap-1",
                user_nlq_text="show orders",
                tenant_id=1,
            )
        )
        update_payload = json.loads(await update_handler("interaction-1", tenant_id=1))
        feedback_payload = json.loads(await feedback_handler("interaction-1", "UP"))

    _assert_generic_envelope_shape(save_payload)
    _assert_generic_envelope_shape(load_payload)
    _assert_generic_envelope_shape(create_payload)
    _assert_generic_envelope_shape(update_payload)
    _assert_generic_envelope_shape(feedback_payload)
