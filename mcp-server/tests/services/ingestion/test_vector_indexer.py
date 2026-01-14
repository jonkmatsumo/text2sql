from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
from mcp_server.services.ingestion.vector_indexer import VectorIndexer, apply_adaptive_threshold


class TestVectorIndexerCharacterization:
    """Characterization tests for VectorIndexer Phase 2a."""

    @pytest.fixture
    def mock_store(self):
        """Fixture for MemgraphStore mock."""
        store = MagicMock()
        store.driver.session.return_value.__enter__.return_value = MagicMock()
        return store

    @pytest.fixture
    def indexer(self, mock_store):
        """Fixture for VectorIndexer with patched dependencies."""
        # Patch AsyncOpenAI to prevent initialization error
        # mock_openai is used to suppress the init, valid use even if variable unused
        with patch("mcp_server.services.ingestion.vector_indexer.AsyncOpenAI"):
            indexer = VectorIndexer(store=mock_store)
            # Mock embedding service method to strictly avoid API calls
            indexer.embedding_service.embed_text = AsyncMock(return_value=[0.1] * 1536)
            return indexer

    @pytest.mark.asyncio
    async def test_search_nodes_output_shape(self, indexer):
        """Validate output is list of dicts with node and score."""
        mock_session = indexer.store.driver.session.return_value.__enter__.return_value

        # Mock node as a plain dict (Memgraph Node behaves like dict)
        mock_node = {"name": "test_table", "embedding": [0.1] * 1536}

        # Record structure matches ANN return: {"node": node, "score": score}
        mock_record = {"node": mock_node, "score": 0.95}
        mock_session.run.return_value = [mock_record]

        # Mocks
        indexer._build_ann_query = MagicMock(return_value="CALL vector_search...")
        indexer._map_ann_results = MagicMock(
            return_value={"node": {"name": "test_table"}, "score": 0.95}
        )

        results = await indexer.search_nodes("query", k=1)

        assert isinstance(results, list)
        assert len(results) == 1
        item = results[0]
        assert item["score"] == 0.95
        assert item["node"]["name"] == "test_table"

    @pytest.mark.asyncio
    async def test_search_nodes_delegates_to_ann_result_order(self, indexer):
        """Validate results are returned in the order provided by the DB (ANN/Scan)."""
        mock_session = indexer.store.driver.session.return_value.__enter__.return_value
        indexer.embedding_service.embed_text = AsyncMock(return_value=[1.0, 0.0])

        # Mock DB returning sorted results
        rec1 = {"node": {"name": "good"}, "score": 0.9}
        rec2 = {"node": {"name": "bad"}, "score": 0.1}

        mock_session.run.return_value = [rec1, rec2]

        # We need _map_ann_results to work for real or be mocked to pass through
        indexer._map_ann_results = lambda r: {"node": r["node"], "score": r["score"]}

        results = await indexer.search_nodes("query", k=2, apply_threshold=False)

        assert len(results) == 2
        assert results[0]["node"]["name"] == "good"
        assert results[0]["score"] > results[1]["score"]


class TestAdaptiveThresholdCharacterization:
    """Characterization tests for apply_adaptive_threshold logic."""

    def test_threshold_filtering(self):
        """Validate basic filtering based on best score."""
        # best_score = 0.9. threshold = max(0.45, 0.9 - 0.15) = 0.75
        hits = [
            {"score": 0.9, "id": 1},
            {"score": 0.8, "id": 2},  # keep (> 0.75)
            {"score": 0.7, "id": 3},  # drop (< 0.75)
        ]
        filtered, threshold = apply_adaptive_threshold(hits)
        assert len(filtered) == 2
        ids = [h["id"] for h in filtered]
        assert ids == [1, 2]

    def test_threshold_fallback(self):
        """Validate fallback when all items are filtered out."""
        # best_score = 0.4. threshold = max(0.45, 0.4 - 0.15) = 0.45
        # all items below 0.45
        hits = [
            {"score": 0.40, "id": 1},
            {"score": 0.39, "id": 2},
            {"score": 0.38, "id": 3},
            {"score": 0.37, "id": 4},
        ]
        # Should return top 3 fallback
        filtered, threshold = apply_adaptive_threshold(hits)
        assert len(filtered) == 3
        ids = [h["id"] for h in filtered]
        assert ids == [1, 2, 3]

    def test_min_score_absolute(self):
        """Validate MIN_SCORE_ABSOLUTE floor."""
        # best_score = 0.55. threshold = max(0.45, 0.55 - 0.15 = 0.40) = 0.45
        # So threshold is 0.45
        hits = [
            {"score": 0.55, "id": 1},  # keep
            {"score": 0.46, "id": 2},  # keep
            {"score": 0.44, "id": 3},  # drop
        ]
        filtered, threshold = apply_adaptive_threshold(hits)
        assert len(filtered) == 2


class TestVectorIndexerANNHelpers:
    """Tests for Phase 2b ANN helpers."""

    @pytest.fixture
    def indexer(self):
        """Fixture for VectorIndexer (mock store not needed for these helpers)."""
        # We only need instances for helper methods, mock store to avoid init errors
        store = MagicMock()
        with patch("mcp_server.services.ingestion.vector_indexer.AsyncOpenAI"):
            return VectorIndexer(store=store)

    def test_build_ann_query_table(self, indexer):
        """Validate query construction for Table label."""
        query = indexer._build_ann_query("Table", "embedding", "$emb", "$k")

        assert "call vector_search.search" in query.lower()
        assert "'table_embedding_index'" in query
        assert "'Table'" in query
        assert "'embedding'" in query
        assert "$emb" in query
        assert "$k" in query
        assert "YIELD node, score" in query
        assert "RETURN node, score" in query

    def test_build_ann_query_no_inline_vector(self, indexer):
        """Validate query uses parameters, avoiding inline vector injection."""
        query = indexer._build_ann_query("Table", "embedding", "$emb", "$k")
        # Should not contain any brackets or large lists suggestive of inline vectors
        assert "[" not in query
        assert "]" not in query

    def test_build_ann_query_column(self, indexer):
        """Validate query construction for Column label uses fallback Cypher scan."""
        query = indexer._build_ann_query("Column", "embedding", "$emb", "$k")

        query_lower = query.lower()
        assert "call vector_search.search" not in query_lower
        assert "match (node:column)" in query_lower
        assert "vector.similarity.cosine" in query_lower
        assert "limit $k" in query_lower

    def test_map_ann_results_success(self, indexer):
        """Validate mapping of correct record."""
        mock_node = {"name": "test", "embedding": [0.1]}
        record = {"node": mock_node, "score": 0.95}

        result = indexer._map_ann_results(record)

        assert result["score"] == 0.95
        assert result["node"]["name"] == "test"
        assert "embedding" not in result["node"]

    def test_map_ann_results_score_type_handling(self, indexer):
        """Validate robustness to score types."""
        mock_node = {"name": "test"}
        # integer score
        record = {"node": mock_node, "score": 1}
        result = indexer._map_ann_results(record)
        assert isinstance(result["score"], float)
        assert result["score"] == 1.0

        # string score (shouldn't happen but good defense)
        record = {"node": mock_node, "score": "0.5"}
        result = indexer._map_ann_results(record)
        assert result["score"] == 0.5


class TestVectorIndexerObservability:
    """Tests for Phase 2d observability."""

    @pytest.fixture
    def indexer(self):
        """Fixture for VectorIndexer with patched dependencies."""
        store = MagicMock()
        store.driver.session.return_value.__enter__.return_value = MagicMock()
        with patch("mcp_server.services.ingestion.vector_indexer.AsyncOpenAI"):
            indexer = VectorIndexer(store=store)
            indexer.embedding_service.embed_text = AsyncMock(return_value=[0.1] * 1536)
            return indexer

    @pytest.mark.asyncio
    async def test_search_nodes_logs_structured_event(self, indexer):
        """Validate search_nodes logs formatted event with expected keys."""
        mock_session = indexer.store.driver.session.return_value.__enter__.return_value
        mock_session.run.return_value = [{"node": {"name": "test"}, "score": 0.9}]

        # Mock mapper to return dict
        indexer._map_ann_results = lambda r: r

        with patch("mcp_server.services.ingestion.vector_indexer.logger") as mock_logger:
            await indexer.search_nodes("query", k=5, apply_threshold=True)

            mock_logger.info.assert_called()
            # Assert extra dict structure in the last call
            call_args = mock_logger.info.call_args
            assert call_args is not None

            # call_args.kwargs['extra'] or call_args[1]['extra'] depending on how called
            # It was called as logger.info(msg, extra={...})
            kwargs = call_args.kwargs
            extra = kwargs.get("extra")
            assert extra is not None
            assert extra["event"] == "memgraph_ann_seed_search"
            assert extra["label"] == "Table"
            assert extra["top_k"] == 5
            assert extra["returned_count"] == 1
            assert "elapsed_ms" in extra
            assert isinstance(extra["elapsed_ms"], float)
            assert extra["threshold_applied"] is True
            assert "threshold_value" in extra

    @pytest.mark.asyncio
    async def test_search_nodes_logs_error(self, indexer):
        """Validate search_nodes logs error event on failure."""
        mock_session = indexer.store.driver.session.return_value.__enter__.return_value
        mock_session.run.side_effect = Exception("DB Error")

        with patch("mcp_server.services.ingestion.vector_indexer.logger") as mock_logger:
            with patch("mcp_server.services.ingestion.vector_indexer.Telemetry") as mock_telemetry:
                # Mock context manager
                mock_span = MagicMock()
                mock_telemetry.start_span.return_value.__enter__.return_value = mock_span

                with pytest.raises(Exception):
                    await indexer.search_nodes("query", k=5)

                mock_logger.error.assert_called()
                call_args = mock_logger.error.call_args
                extra = call_args.kwargs.get("extra")
                assert extra["event"] == "memgraph_ann_seed_search_failed"
                assert extra["error_type"] == "Exception"
                assert extra["label"] == "Table"

                # Check span failure
                mock_telemetry.set_span_status.assert_called_with(
                    mock_span, success=False, error=ANY
                )

    @pytest.mark.asyncio
    async def test_search_nodes_otel_span_success(self, indexer):
        """Validate OTEL span creation and attributes on success."""
        mock_session = indexer.store.driver.session.return_value.__enter__.return_value
        mock_session.run.return_value = [{"node": {"name": "test"}, "score": 0.9}]
        indexer._map_ann_results = lambda r: r

        with patch("mcp_server.services.ingestion.vector_indexer.Telemetry") as mock_telemetry:
            # Mock context manager
            mock_span = MagicMock()
            mock_telemetry.start_span.return_value.__enter__.return_value = mock_span

            await indexer.search_nodes("query", k=5, apply_threshold=True)

            # Verify start_span call
            mock_telemetry.start_span.assert_called_once()
            args, kwargs = mock_telemetry.start_span.call_args
            assert args[0] == "vector_seed_selection.ann"
            assert kwargs["attributes"]["db.operation"] == "ANN_SEARCH"
            assert kwargs["attributes"]["vector.label"] == "Table"

            # Verify dynamic attributes
            mock_span.set_attribute.assert_any_call("vector.returned_count", 1)
            mock_span.set_attribute.assert_any_call("vector.threshold_applied", True)
            mock_span.set_attribute.assert_any_call("vector.threshold_value", ANY)

            # Verify status
            mock_telemetry.set_span_status.assert_called_with(mock_span, success=True)


class TestRegressionGuardrails:
    """Guardrails against performance regressions."""

    @pytest.fixture
    def indexer(self):
        """Fixture for VectorIndexer with patched dependencies."""
        store = MagicMock()
        store.driver.session.return_value.__enter__.return_value = MagicMock()
        with patch("mcp_server.services.ingestion.vector_indexer.AsyncOpenAI"):
            indexer = VectorIndexer(store=store)
            indexer.embedding_service.embed_text = AsyncMock(return_value=[0.1] * 1536)
            return indexer

    @pytest.mark.asyncio
    async def test_no_fetch_all_embeddings(self, indexer):
        """Ensure no O(N) fetch-all query is executed."""
        mock_session = indexer.store.driver.session.return_value.__enter__.return_value
        # Mock result to avoid processing errors
        mock_session.run.return_value = []

        await indexer.search_nodes("query", label="Table", k=5)

        # Verify only one query run
        assert mock_session.run.call_count == 1
        args, _ = mock_session.run.call_args
        query = args[0]

        # Assert strictly uses vector index procedure
        assert "call vector_search.search" in query.lower()

        # Guard against broad matches without vector search
        forbidden_patterns = ["match (n:table) return n", "match (n:table) return n, n.embedding"]
        for pattern in forbidden_patterns:
            assert pattern not in query.lower()

    @pytest.mark.asyncio
    async def test_column_fallback_uses_limit(self, indexer):
        """Ensure fallback scan uses LIMIT and doesn't fetch all."""
        mock_session = indexer.store.driver.session.return_value.__enter__.return_value
        mock_session.run.return_value = []

        await indexer.search_nodes("query", label="Column", k=5)

        args, _ = mock_session.run.call_args
        query = args[0].lower()

        assert "limit $k" in query
        assert "order by score desc" in query
        # Should not just return all embeddings
        assert "return n, n.embedding" not in query
