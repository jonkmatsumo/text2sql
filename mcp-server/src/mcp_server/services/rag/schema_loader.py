"""Loader for table schema embeddings from database to vector index."""

import logging

import numpy as np
from mcp_server.config.database import Database

# from ingestion.vector_indexes import VectorIndex  # If needed
# Note: Check if generic usage or specific.
from common.interfaces.vector_index import VectorIndex

logger = logging.getLogger(__name__)


class SchemaLoader:
    """Loads schema embeddings from database into the vector index."""

    async def load_schema_embeddings(self, index: VectorIndex) -> None:
        """
        Fetch all schema embeddings from the configured SchemaStore and populate the index.

        Args:
            index: The VectorIndex instance to populate.
        """
        store = Database.get_schema_store()
        schemas = await store.fetch_schema_embeddings()

        if not schemas:
            logger.warning("No schema embeddings found in SchemaStore.")
            return

        vectors = []
        ids = []
        metadata = {}

        for schema in schemas:
            vectors.append(schema.embedding)
            ids.append(schema.table_name)

            metadata[schema.table_name] = {
                "table_name": schema.table_name,
                "schema_text": schema.schema_text,
            }

        # Convert to numpy array
        vectors_np = np.array(vectors, dtype=np.float32)

        # Add to index
        try:
            # ExtendedVectorIndex supports metadata
            index.add_items(vectors_np, ids, metadata=metadata)
        except TypeError:
            # Fallback for basic VectorIndex
            index.add_items(vectors_np, ids)

        logger.info(f"✓ Loaded {len(ids)} table schemas into vector index.")
