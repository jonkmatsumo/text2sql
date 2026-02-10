"""Tests for semantic subgraph retrieval tool."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mcp_server.tools.get_semantic_subgraph import TOOL_NAME, _apply_column_guardrails
from mcp_server.tools.get_semantic_subgraph import handler as get_semantic_subgraph


class TestGetSemanticSubgraph:
    """Unit tests for get_semantic_subgraph tool."""

    def _mock_introspector(self):
        table_def = SimpleNamespace(
            description="",
            columns=[
                SimpleNamespace(name="id", data_type="integer", is_nullable=False),
            ],
            foreign_keys=[
                SimpleNamespace(
                    column_name="id",
                    foreign_table_name="orders",
                    foreign_column_name="id",
                )
            ],
        )

        introspector = MagicMock()
        introspector.get_table_def = AsyncMock(return_value=table_def)
        return introspector

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "get_semantic_subgraph"

    @pytest.mark.asyncio
    async def test_get_semantic_subgraph_success(self):
        """Test successful subgraph retrieval with 2-step strategy."""
        mock_indexer = MagicMock()

        # Tables-first: returns table hits
        mock_indexer.search_nodes_with_metadata = AsyncMock(
            return_value=(
                [{"node": {"name": "customers"}, "score": 0.9}],
                {"threshold": 0.75, "timing_ms": {"embedding": 1.0, "search": 2.0}},
            )
        )

        # Mock store and session
        mock_store = MagicMock()
        mock_session = MagicMock()
        mock_store.driver.session.return_value = mock_session

        # Mock result for Step 1 (Tables/Columns)
        mock_table = MagicMock()
        mock_table.element_id = "t1"
        mock_table.get = lambda k, d=None: {"name": "customers"}.get(k, d)
        mock_table.__iter__ = lambda s: iter([("name", "customers")])

        mock_col = MagicMock()
        mock_col.element_id = "c1"
        mock_col.get = lambda k, d=None: {"name": "customer_id", "type": "integer"}.get(k, d)
        mock_col.__iter__ = lambda s: iter([("name", "customer_id"), ("type", "integer")])

        record1 = {"t": mock_table, "columns": [mock_col]}

        # Mock result for Step 2 (Join Discovery)
        mock_sc = MagicMock()
        mock_sc.element_id = "c1"

        mock_tc = MagicMock()
        mock_tc.element_id = "c2"

        mock_rt = MagicMock()
        mock_rt.element_id = "t2"
        mock_rt.get = lambda k, d=None: {"name": "orders"}.get(k, d)
        mock_rt.__iter__ = lambda s: iter([("name", "orders")])

        record2 = {"source_col": mock_sc, "target_table": mock_rt, "target_col": mock_tc}

        # Mock result for Step 2.5 (Dimension Table Column Expansion)
        mock_order_col = MagicMock()
        mock_order_col.element_id = "c3"
        mock_order_col.get = lambda k, d=None: {"name": "order_date", "type": "date"}.get(k, d)
        mock_order_col.__iter__ = lambda s: iter([("name", "order_date"), ("type", "date")])

        record2_5 = {"t": mock_rt, "c": mock_order_col}

        mock_session.run.side_effect = [
            [record1],
            [record2],
            [record2_5],
        ]

        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = None

        with patch(
            "mcp_server.tools.get_semantic_subgraph.Database.get_graph_store",
            return_value=mock_store,
        ):
            with patch(
                "mcp_server.tools.get_semantic_subgraph.Database.get_schema_introspector",
                return_value=self._mock_introspector(),
            ):
                with patch(
                    "mcp_server.tools.get_semantic_subgraph.VectorIndexer",
                    return_value=mock_indexer,
                ):
                    result_json = await get_semantic_subgraph(query="find customers", tenant_id=1)
                result = json.loads(result_json)["result"]

                assert "nodes" in result
                assert "relationships" in result
                assert len(result["nodes"]) >= 1

                # Check for join relationships
                rels = result["relationships"]
                has_fk = any(r["type"] == "FOREIGN_KEY_TO" for r in rels)
                assert has_fk

    @pytest.mark.asyncio
    async def test_get_semantic_subgraph_no_seeds(self):
        """Test handling no search results."""
        mock_indexer = MagicMock()
        mock_indexer.search_nodes_with_metadata = AsyncMock(
            return_value=([], {"threshold": 0.0, "timing_ms": {}})
        )

        with patch(
            "mcp_server.tools.get_semantic_subgraph.Database.get_graph_store",
            return_value=MagicMock(),
        ):
            with patch(
                "mcp_server.tools.get_semantic_subgraph.Database.get_schema_introspector",
                return_value=self._mock_introspector(),
            ):
                with patch(
                    "mcp_server.tools.get_semantic_subgraph.VectorIndexer",
                    return_value=mock_indexer,
                ):
                    result = await get_semantic_subgraph(query="query", tenant_id=1)
                data = json.loads(result)["result"]
                assert data["nodes"] == []
                assert data["relationships"] == []

    @pytest.mark.asyncio
    async def test_get_semantic_subgraph_error(self):
        """Test error handling."""
        mock_indexer = MagicMock()
        mock_indexer.search_nodes_with_metadata = AsyncMock(side_effect=Exception("Search failed"))

        with patch(
            "mcp_server.tools.get_semantic_subgraph.Database.get_graph_store",
            return_value=MagicMock(),
        ):
            with patch(
                "mcp_server.tools.get_semantic_subgraph.Database.get_schema_introspector",
                return_value=self._mock_introspector(),
            ):
                with patch(
                    "mcp_server.tools.get_semantic_subgraph.VectorIndexer",
                    return_value=mock_indexer,
                ):
                    result = await get_semantic_subgraph(query="query", tenant_id=1)
                data = json.loads(result)

                assert "error" in data
                assert data["error"]["message"] == "Failed to retrieve semantic subgraph."
                assert data["error"]["sql_state"] == "SEMANTIC_SUBGRAPH_QUERY_FAILED"
                assert data["error"]["category"] == "invalid_request"

    @pytest.mark.asyncio
    async def test_get_semantic_subgraph_oversized_query_rejected(self):
        """Oversized query should be rejected before embedding/cache work."""
        oversized_query = "x" * ((10 * 1024) + 1)
        with patch(
            "mcp_server.tools.get_semantic_subgraph.RagEngine.embed_text",
            AsyncMock(return_value=[0.1]),
        ) as mock_embed:
            result = await get_semantic_subgraph(query=oversized_query, tenant_id=1)

        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert data["error"]["sql_state"] == "INPUT_TOO_LARGE"
        mock_embed.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_tables_first_strategy(self):
        """Test that tables are searched before columns."""
        mock_indexer = MagicMock()

        call_order = []

        def track_calls(query_text, label, k, apply_threshold=True, use_column_cache=False):
            call_order.append(label)
            return [], {"threshold": 0.0, "timing_ms": {}}

        mock_indexer.search_nodes_with_metadata = AsyncMock(side_effect=track_calls)

        with patch(
            "mcp_server.tools.get_semantic_subgraph.Database.get_graph_store",
            return_value=MagicMock(),
        ):
            with patch(
                "mcp_server.tools.get_semantic_subgraph.Database.get_schema_introspector",
                return_value=self._mock_introspector(),
            ):
                with patch(
                    "mcp_server.tools.get_semantic_subgraph.VectorIndexer",
                    return_value=mock_indexer,
                ):
                    await get_semantic_subgraph(query="test query", tenant_id=1)

                # Should search tables first
                assert call_order[0] == "Table"

    @pytest.mark.asyncio
    async def test_seed_selection_telemetry_column_fallback(self):
        """Telemetry records column fallback only when table hits are empty."""
        mock_indexer = MagicMock()
        mock_indexer.search_nodes_with_metadata = AsyncMock(
            side_effect=[
                ([], {"threshold": 0.0, "timing_ms": {"embedding": 1.0, "search": 2.0}}),
                (
                    [{"node": {"name": "email", "table": "users"}, "score": 0.8}],
                    {"threshold": 0.6, "timing_ms": {"embedding": 1.5, "search": 2.5}},
                ),
            ]
        )

        spans = []

        class FakeSpan:
            def __init__(self, attributes=None):
                self.attributes = dict(attributes or {})

            def set_attribute(self, key, value):
                self.attributes[key] = value

        from contextlib import contextmanager

        @contextmanager
        def fake_start_span(name, attributes=None):
            span = FakeSpan(attributes)
            spans.append((name, span))
            yield span

        with patch(
            "mcp_server.tools.get_semantic_subgraph.Telemetry.start_span",
            side_effect=fake_start_span,
        ):
            with patch(
                "mcp_server.tools.get_semantic_subgraph.Database.get_graph_store",
                return_value=MagicMock(),
            ):
                with patch(
                    "mcp_server.tools.get_semantic_subgraph.Database.get_schema_introspector",
                    return_value=self._mock_introspector(),
                ):
                    with patch(
                        "mcp_server.tools.get_semantic_subgraph.VectorIndexer",
                        return_value=mock_indexer,
                    ):
                        await get_semantic_subgraph(query="email addresses", tenant_id=1)

        seed_span = next(span for name, span in spans if name == "seed_selection")
        assert seed_span.attributes["seed_selection.path"] == "column_fallback"
        assert seed_span.attributes["seed_selection.table_hit_count"] == 0
        assert seed_span.attributes["seed_selection.column_hit_count"] == 1
        assert seed_span.attributes["seed_selection.k_tables"] == 5
        assert seed_span.attributes["seed_selection.k_columns"] == 3
        assert seed_span.attributes["seed_selection.similarity_threshold_column"] == 0.6

    @pytest.mark.asyncio
    async def test_seed_selection_telemetry_table_path(self):
        """Telemetry records table path when table hits are present."""
        mock_indexer = MagicMock()
        mock_indexer.search_nodes_with_metadata = AsyncMock(
            return_value=(
                [{"node": {"name": "users"}, "score": 0.9}],
                {"threshold": 0.7, "timing_ms": {"embedding": 1.0, "search": 2.0}},
            )
        )

        spans = []

        class FakeSpan:
            def __init__(self, attributes=None):
                self.attributes = dict(attributes or {})

            def set_attribute(self, key, value):
                self.attributes[key] = value

        from contextlib import contextmanager

        @contextmanager
        def fake_start_span(name, attributes=None):
            span = FakeSpan(attributes)
            spans.append((name, span))
            yield span

        with patch(
            "mcp_server.tools.get_semantic_subgraph.Telemetry.start_span",
            side_effect=fake_start_span,
        ):
            with patch(
                "mcp_server.tools.get_semantic_subgraph.Database.get_graph_store",
                return_value=MagicMock(),
            ):
                with patch(
                    "mcp_server.tools.get_semantic_subgraph.Database.get_schema_introspector",
                    return_value=self._mock_introspector(),
                ):
                    with patch(
                        "mcp_server.tools.get_semantic_subgraph.VectorIndexer",
                        return_value=mock_indexer,
                    ):
                        await get_semantic_subgraph(query="find users", tenant_id=1)

        seed_span = next(span for name, span in spans if name == "seed_selection")
        assert seed_span.attributes["seed_selection.path"] == "table"
        assert seed_span.attributes["seed_selection.table_hit_count"] == 1

    def test_column_guardrails_filter_generic(self):
        """Generic column names should be filtered without strong separation."""
        seeds = [
            {"node": {"name": "id", "table": "t1"}, "score": 0.65},
            {"node": {"name": "status", "table": "t2"}, "score": 0.64},
            {"node": {"name": "email", "table": "t3"}, "score": 0.63},
        ]
        filtered, relaxed = _apply_column_guardrails(seeds)
        assert relaxed is False
        assert any(s["node"]["name"] == "email" for s in filtered)
        assert all(s["node"]["name"] != "id" for s in filtered)

    def test_column_guardrails_relax_backstop(self):
        """Guardrails should relax when everything would be dropped."""
        seeds = [
            {"node": {"name": "id", "table": "t1"}, "score": 0.65},
            {"node": {"name": "status", "table": "t2"}, "score": 0.64},
            {"node": {"name": "amount", "table": "t3"}, "score": 0.63},
        ]
        filtered, relaxed = _apply_column_guardrails(seeds)
        assert relaxed is True
        assert filtered

    @pytest.mark.asyncio
    async def test_table_hits_suppress_column_fallback(self):
        """Table hits should prevent column fallback for column-style queries."""
        mock_indexer = MagicMock()
        mock_indexer.search_nodes_with_metadata = AsyncMock(
            return_value=(
                [{"node": {"name": "users"}, "score": 0.9}],
                {"threshold": 0.7, "timing_ms": {"embedding": 1.0, "search": 2.0}},
            )
        )

        with patch(
            "mcp_server.tools.get_semantic_subgraph.Database.get_graph_store",
            return_value=MagicMock(),
        ):
            with patch(
                "mcp_server.tools.get_semantic_subgraph.Database.get_schema_introspector",
                return_value=self._mock_introspector(),
            ):
                with patch(
                    "mcp_server.tools.get_semantic_subgraph.VectorIndexer",
                    return_value=mock_indexer,
                ):
                    await get_semantic_subgraph(query="email addresses", tenant_id=1)

        assert mock_indexer.search_nodes_with_metadata.call_count == 1
