import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.tools.get_semantic_subgraph import handler as get_semantic_subgraph


class TestSemanticTraversalGuardrail:
    """Guardrail tests for traversal logic equivalence given identical seeds."""

    def _create_mock_table_def(self, name, columns, fks=None):
        cols = []
        for c in columns:
            cols.append(SimpleNamespace(name=c, data_type="text", is_nullable=False))

        foreign_keys = []
        if fks:
            for fk in fks:
                foreign_keys.append(
                    SimpleNamespace(
                        column_name=fk["col"],
                        foreign_table_name=fk["ref_table"],
                        foreign_column_name=fk["ref_col"],
                    )
                )

        return SimpleNamespace(
            description=f"Description for {name}", columns=cols, foreign_keys=foreign_keys
        )

    @pytest.mark.asyncio
    async def test_traversal_equivalence_fixed_seeds(self):
        """Prove that given a fixed seed set (mocked), traversal output is unchanged.

        This ensures ANN refactoring didn't break graph expansion.
        """
        # 1. FIXED SEEDS
        # We mock VectorIndexer to return exactly one table: "users"
        mock_indexer = MagicMock()
        mock_indexer.search_nodes = AsyncMock(
            return_value=[{"node": {"name": "users"}, "score": 1.0}]
        )

        # 2. FIXED SCHEMA (Introspector)
        mock_introspector = MagicMock()

        def get_table_def_side_effect(name):
            if name == "users":
                return self._create_mock_table_def("users", ["id", "name"])
            elif name == "posts":
                # users FK discovery might trigger lookup of posts
                return self._create_mock_table_def(
                    "posts",
                    ["id", "user_id"],
                    [{"col": "user_id", "ref_table": "users", "ref_col": "id"}],
                )
            return Exception(f"Table {name} not found")

        mock_introspector.get_table_def = AsyncMock(side_effect=get_table_def_side_effect)

        # 4. EXECUTION
        mock_store = MagicMock()
        mock_session = MagicMock()
        mock_store.driver.session.return_value = mock_session

        with patch(
            "mcp_server.tools.get_semantic_subgraph.Database.get_graph_store",
            return_value=mock_store,
        ), patch(
            "mcp_server.tools.get_semantic_subgraph.Database.get_schema_introspector",
            return_value=mock_introspector,
        ), patch(
            "mcp_server.tools.get_semantic_subgraph.VectorIndexer", return_value=mock_indexer
        ), patch(
            "mcp_server.services.rag.linker.SchemaLinker.rank_and_filter_columns",
            new_callable=AsyncMock,
        ):

            result_json = await get_semantic_subgraph("query is irrelevant with fixed seeds")
            result = json.loads(result_json)

            # 5. VERIFICATION
            nodes = result["nodes"]
            node_ids = set(n["id"] for n in nodes)

            assert "users" in node_ids
            assert "users.id" in node_ids
            assert "users.name" in node_ids

    @pytest.mark.asyncio
    async def test_traversal_follows_fks(self):
        """Prove that traversal follows outgoing FKs exactly as before."""
        # 1. FIXED SEEDS: "orders"
        mock_indexer = MagicMock()
        mock_indexer.search_nodes = AsyncMock(
            return_value=[{"node": {"name": "orders"}, "score": 1.0}]
        )

        # 2. SCHEMA
        mock_introspector = MagicMock()

        def get_table_def_side_effect(name):
            if name == "orders":
                return self._create_mock_table_def(
                    "orders",
                    ["id", "user_id"],
                    [{"col": "user_id", "ref_table": "users", "ref_col": "id"}],
                )
            elif name == "users":
                return self._create_mock_table_def("users", ["id", "name"])
            return Exception(f"Table {name} not found")

        mock_introspector.get_table_def = AsyncMock(side_effect=get_table_def_side_effect)

        mock_store = MagicMock()

        with patch(
            "mcp_server.tools.get_semantic_subgraph.Database.get_graph_store",
            return_value=mock_store,
        ), patch(
            "mcp_server.tools.get_semantic_subgraph.Database.get_schema_introspector",
            return_value=mock_introspector,
        ), patch(
            "mcp_server.tools.get_semantic_subgraph.VectorIndexer", return_value=mock_indexer
        ), patch(
            "mcp_server.services.rag.linker.SchemaLinker.rank_and_filter_columns",
            new_callable=AsyncMock,
        ):

            result_json = await get_semantic_subgraph("find orders")
            result = json.loads(result_json)

            node_ids = set(n["id"] for n in result["nodes"])

            assert "orders" in node_ids
            assert "users" in node_ids
            assert "orders.user_id" in node_ids
            assert "users.id" in node_ids

            rels = result["relationships"]
            fk_rels = [r for r in rels if r["type"] == "FOREIGN_KEY_TO"]
            assert len(fk_rels) > 0
            assert any(
                r["source"] == "orders.user_id" and r["target"] == "users.id" for r in fk_rels
            )
