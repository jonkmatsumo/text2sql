"""Loader for few-shot examples from database to vector index."""

import logging

import numpy as np

from common.interfaces import ExampleStore
from common.interfaces.vector_index import VectorIndex

logger = logging.getLogger(__name__)


class ExampleLoader:
    """Loads SQL examples from database into the vector index."""

    def __init__(self, store: ExampleStore):
        """Initialize with ExampleStore.

        Args:
            store: ExampleStore instance.
        """
        self.store = store

    async def load_examples(self, index: VectorIndex) -> None:
        """
        Fetch all examples from the configured ExampleStore and add them to the provided index.

        Args:
            index: The VectorIndex instance to populate.
        """
        examples = await self.store.fetch_all_examples()

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
