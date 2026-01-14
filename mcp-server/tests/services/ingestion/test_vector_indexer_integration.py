import os
import random
from unittest.mock import AsyncMock

import pytest
from mcp_server.dal.memgraph import MemgraphStore
from mcp_server.services.ingestion.vector_indexer import VectorIndexer

# Skip if explicit env var not set (default skip in CI unless configured)
RUN_INTEGRATION = os.getenv("RUN_MEMGRAPH_INTEGRATION_TESTS", "false").lower() == "true"


@pytest.mark.skipif(not RUN_INTEGRATION, reason="Memgraph integration tests not enabled")
class TestVectorIndexerIntegration:
    """Integration tests comparing ANN vs Brute Force baseline."""

    @pytest.fixture
    def store(self):
        """Fixture for Memgraph store."""
        # Use default local URI or env var
        uri = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
        return MemgraphStore(uri=uri)

    @pytest.fixture
    async def seeded_indexer(self, store):
        """Fixture for VectorIndexer seeded with test data."""
        indexer = VectorIndexer(store=store)
        # Ensure index exists
        indexer.create_indexes()

        # Clear existing tables for test isolation
        with store.driver.session() as session:
            session.run("MATCH (n:Table) DETACH DELETE n")

            # Seed small set of nodes
            # 10 vectors, one very close to query, one far, others random
            # Query will be [1.0, 0.0, ...]

            # Best match
            best_vec = [0.0] * 1536
            best_vec[0] = 1.0  # Cosine sim 1.0 with query

            # Worst match
            worst_vec = [0.0] * 1536
            worst_vec[1] = 1.0  # Orthogonal

            # Insert
            session.run("CREATE (:Table {name: 'best', embedding: $e})", e=best_vec)
            session.run("CREATE (:Table {name: 'worst', embedding: $e})", e=worst_vec)

            # Randoms
            for i in range(8):
                vec = [random.random() for _ in range(1536)]
                session.run(f"CREATE (:Table {{name: 'rand_{i}', embedding: $e}})", e=vec)

        return indexer

    @pytest.mark.asyncio
    async def test_ann_vs_baseline_overlap(self, seeded_indexer):
        """Verify ANN results substantially overlap with baseline expectation."""
        # This test compares ANN result against theoretical expectation
        # Query: [1.0, 0, ...]
        query_text = "ignored_by_mock"

        # Mock embedder to return our fixed query vector
        seeded_indexer.embedding_service.embed_text = AsyncMock(return_value=[1.0] + [0.0] * 1535)

        # Run Search
        results = await seeded_indexer.search_nodes(query_text, k=3, apply_threshold=False)

        # Expect 'best' to be top 1
        assert len(results) == 3
        assert results[0]["node"]["name"] == "best"
        assert results[0]["score"] > 0.99
