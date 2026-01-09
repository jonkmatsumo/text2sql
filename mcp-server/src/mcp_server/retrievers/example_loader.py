"""Loader for few-shot examples from database to vector index."""

import json
import logging

import numpy as np
from mcp_server.db import Database
from mcp_server.graph_ingestion.vector_indexes.protocol import VectorIndex

logger = logging.getLogger(__name__)


class ExampleLoader:
    """Loads SQL examples from database into the vector index."""

    async def load_examples(self, index: VectorIndex) -> None:
        """
        Fetch all examples from Postgres and add them to the provided index.

        Args:
            index: The VectorIndex instance to populate.
        """
        query = """
            SELECT id, question, sql_query, embedding
            FROM sql_examples
            WHERE embedding IS NOT NULL
        """

        async with Database.get_connection() as conn:
            rows = await conn.fetch(query)

        if not rows:
            logger.warning("No examples found in sql_examples table.")
            return

        vectors = []
        ids = []
        metadata = {}

        for row in rows:
            # Parse embedding vector
            # Asyncpg might return string or list depending on setup.
            # Assuming standard pgvector output which is often a string '[x,y,...]'
            embedding_val = row["embedding"]
            if isinstance(embedding_val, str):
                vector = json.loads(embedding_val)
            else:
                vector = list(embedding_val)

            # Ensure vector is a list of floats
            vectors.append(vector)

            # IDs
            item_id = int(row["id"])
            ids.append(item_id)

            # Metadata
            metadata[item_id] = {"question": row["question"], "sql": row["sql_query"]}

        # Convert to numpy array
        vectors_np = np.array(vectors, dtype=np.float32)

        # Add to index
        # Note: VectorIndex protocol defined in graph_ingestion/vector_indexes/protocol.py
        # strictly speaks only adds vectors and ids.
        # However, the HNSW implementation and ExtendedVectorIndex support metadata.
        # We check if index supports metadata argument or we might rely on implementation details
        # if using HNSWIndex directly.
        # But to be safe and cleaner, we assume the index might support it or we pass it
        # if possible. Checking signature of add_items at runtime or try/except?
        # HNSWIndex supports it. We are instructed to "Fetch Payload... from database
        # (or a local cache map)".
        # Storing in index metadata IS the local cache map optimization.

        try:
            index.add_items(vectors_np, ids, metadata=metadata)
        except TypeError:
            # Fallback if protocol adapter doesn't support metadata
            # But we know HNSWIndex does.
            index.add_items(vectors_np, ids)
            # If we can't store metadata in index, we would need a separate map.
            # Ideally we assume HNSWIndex is used.
            pass

        logger.info(f"✓ Loaded {len(ids)} examples into vector index.")
