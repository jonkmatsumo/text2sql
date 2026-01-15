from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from ingestion.vector_indexer import VectorIndexer, apply_adaptive_threshold


class TestVectorIndexerCharacterization:
    """Characterization tests for VectorIndexer Phase 2a."""

    @pytest.fixture
    def mock_store(self):
        """Fixture for MemgraphStore mock."""
        store = MagicMock()
        # Mock search_ann_seeds to return empty list by default
        store.search_ann_seeds.return_value = []
        return store

    @pytest.fixture
    def indexer(self, mock_store):
        """Fixture for VectorIndexer with patched dependencies."""
        with patch("ingestion.vector_indexer.AsyncOpenAI"):
            indexer = VectorIndexer(store=mock_store)
            # Mock embedding service method to strictly avoid API calls
            indexer.embedding_service.embed_text = AsyncMock(return_value=[0.1] * 1536)
            return indexer

    @pytest.mark.asyncio
    async def test_search_nodes_output_shape(self, indexer):
        """Validate output is list of dicts with node and score."""
        # Mock DAL return using flat dicts
        indexer.store.search_ann_seeds.return_value = [
            {"node": {"name": "test_table"}, "score": 0.95}
        ]

        results = await indexer.search_nodes("query", k=1)

        assert isinstance(results, list)
        assert len(results) == 1
        item = results[0]
        assert item["score"] == 0.95
        assert item["node"]["name"] == "test_table"

        # Verify DAL call
        indexer.store.search_ann_seeds.assert_called_once()
        args = indexer.store.search_ann_seeds.call_args
        assert args[0][0] == "Table"  # label

    @pytest.mark.asyncio
    async def test_search_nodes_delegates_to_ann_result_order(self, indexer):
        """Validate results are returned in the order provided by the DAL."""
        indexer.embedding_service.embed_text = AsyncMock(return_value=[1.0, 0.0])

        # Mock DAL returning sorted results
        rec1 = {"node": {"name": "good"}, "score": 0.9}
        rec2 = {"node": {"name": "bad"}, "score": 0.1}

        indexer.store.search_ann_seeds.return_value = [rec1, rec2]

        results = await indexer.search_nodes("query", k=2, apply_threshold=False)

        assert len(results) == 2
        assert results[0]["node"]["name"] == "good"
        assert results[0]["score"] > results[1]["score"]


class TestAdaptiveThresholdCharacterization:
    """Characterization tests for apply_adaptive_threshold logic."""

    def test_threshold_filtering(self):
        """Validate basic filtering based on best score."""
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
        hits = [
            {"score": 0.40, "id": 1},
            {"score": 0.39, "id": 2},
            {"score": 0.38, "id": 3},
            {"score": 0.37, "id": 4},
        ]
        filtered, threshold = apply_adaptive_threshold(hits)
        assert len(filtered) == 3
        ids = [h["id"] for h in filtered]
        assert ids == [1, 2, 3]

    def test_min_score_absolute(self):
        """Validate MIN_SCORE_ABSOLUTE floor."""
        hits = [
            {"score": 0.55, "id": 1},
            {"score": 0.46, "id": 2},
            {"score": 0.44, "id": 3},
        ]
        filtered, threshold = apply_adaptive_threshold(hits)
        assert len(filtered) == 2


class TestVectorIndexerObservability:
    """Tests for Phase 2d observability."""

    @pytest.fixture
    def indexer(self):
        """Fixture for VectorIndexer with patched dependencies."""
        store = MagicMock()
        store.search_ann_seeds.return_value = []
        with patch("ingestion.vector_indexer.AsyncOpenAI"):
            indexer = VectorIndexer(store=store)
            indexer.embedding_service.embed_text = AsyncMock(return_value=[0.1] * 1536)
            return indexer

    @pytest.mark.asyncio
    async def test_search_nodes_logs_structured_event(self, indexer):
        """Validate search_nodes logs formatted event with expected keys."""
        indexer.store.search_ann_seeds.return_value = [{"node": {"name": "test"}, "score": 0.9}]

        with patch("ingestion.vector_indexer.logger") as mock_logger:
            await indexer.search_nodes("query", k=5, apply_threshold=True)

            mock_logger.info.assert_called()
            call_args = mock_logger.info.call_args
            kwargs = call_args.kwargs
            extra = kwargs.get("extra")
            assert extra is not None
            assert extra["event"] == "memgraph_ann_seed_search"
            assert extra["label"] == "Table"
            assert extra["top_k"] == 5
            assert extra["returned_count"] == 1
            assert "elapsed_ms" in extra

    @pytest.mark.asyncio
    async def test_search_nodes_logs_error(self, indexer):
        """Validate search_nodes logs error event on failure."""
        indexer.store.search_ann_seeds.side_effect = Exception("DB Error")

        with patch("ingestion.vector_indexer.logger") as mock_logger:
            with patch("ingestion.vector_indexer.Telemetry") as mock_telemetry:
                mock_span = MagicMock()
                mock_telemetry.start_span.return_value.__enter__.return_value = mock_span

                with pytest.raises(Exception):
                    await indexer.search_nodes("query", k=5)

                mock_logger.error.assert_called()
                call_args = mock_logger.error.call_args
                extra = call_args.kwargs.get("extra")
                assert extra["event"] == "memgraph_ann_seed_search_failed"

                mock_telemetry.set_span_status.assert_called_with(
                    mock_span, success=False, error=ANY
                )

    @pytest.mark.asyncio
    async def test_search_nodes_otel_span_success(self, indexer):
        """Validate OTEL span creation and attributes on success."""
        indexer.store.search_ann_seeds.return_value = [{"node": {"name": "test"}, "score": 0.9}]

        with patch("ingestion.vector_indexer.Telemetry") as mock_telemetry:
            mock_span = MagicMock()
            mock_telemetry.start_span.return_value.__enter__.return_value = mock_span

            await indexer.search_nodes("query", k=5, apply_threshold=True)

            mock_telemetry.start_span.assert_called_once()
            args, kwargs = mock_telemetry.start_span.call_args
            assert args[0] == "vector_seed_selection.ann"
            assert kwargs["attributes"]["db.operation"] == "ANN_SEARCH"

            mock_span.set_attribute.assert_any_call("vector.returned_count", 1)
            mock_span.set_attribute.assert_any_call("vector.threshold_applied", True)

            mock_telemetry.set_span_status.assert_called_with(mock_span, success=True)
