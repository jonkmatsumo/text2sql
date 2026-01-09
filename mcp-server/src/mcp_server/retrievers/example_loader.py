"""Loader for few-shot examples from database to vector index."""

import logging

import numpy as np
from mcp_server.db import Database
from mcp_server.graph_ingestion.vector_indexes.protocol import VectorIndex

logger = logging.getLogger(__name__)


class ExampleLoader:
    """Loads SQL examples from database into the vector index."""

    async def load_examples(self, index: VectorIndex) -> None:
        """
        Fetch all examples from the configured ExampleStore and add them to the provided index.

        Args:
            index: The VectorIndex instance to populate.
        """
        store = Database.get_example_store()
        examples = await store.fetch_all_examples()

        if not examples:
            logger.warning("No examples found in ExampleStore.")
            return

        vectors = []
        ids = []
        metadata = {}

        for ex in examples:
            vectors.append(ex.embedding)
            ids.append(ex.id)
            metadata[ex.id] = {"question": ex.question, "sql": ex.sql_query}

        # Convert to numpy array
        vectors_np = np.array(vectors, dtype=np.float32)

        # Add to index
        try:
            index.add_items(vectors_np, ids, metadata=metadata)
        except TypeError:
            # Fallback if protocol adapter doesn't support metadata
            index.add_items(vectors_np, ids)
            pass

        logger.info(f"✓ Loaded {len(ids)} examples into vector index.")
