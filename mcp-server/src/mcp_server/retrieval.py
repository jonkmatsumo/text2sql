"""Retrieval module for dynamic few-shot learning."""

import json
import logging
from typing import Optional

import numpy as np
from mcp_server.graph_ingestion.vector_indexes.factory import create_vector_index
from mcp_server.graph_ingestion.vector_indexes.protocol import VectorIndex
from mcp_server.rag import RagEngine
from mcp_server.retrievers.example_loader import ExampleLoader

logger = logging.getLogger(__name__)

# BAAI/bge-small-en-v1.5 has 384 dimensions
EMBEDDING_DIM = 384

_index: Optional[VectorIndex] = None


async def _get_index() -> VectorIndex:
    """Lazy load and initialize the vector index with examples."""
    global _index
    if _index is None:
        # Initialize in-memory HNSW index
        logger.info("Initializing vector index for few-shot examples...")
        index = create_vector_index(dim=EMBEDDING_DIM)

        # Load examples from DB
        loader = ExampleLoader()
        await loader.load_examples(index)

        _index = index

    return _index


async def get_relevant_examples(
    user_query: str,
    limit: int = 3,
    tenant_id: Optional[int] = None,
) -> str:
    """
    Retrieve few-shot examples similar to the user's query.

    Uses in-memory HNSW vector index for search.

    Args:
        user_query: The user's natural language question
        limit: Maximum number of examples to retrieve (default: 3)
        tenant_id: Optional tenant ID (currently ignored as examples are global)

    Returns:
        Formatted string with examples, or empty string if none found
    """
    # 1. Embed the incoming question
    embedding = RagEngine.embed_text(user_query)

    # 2. Get Index (lazy init)
    index = await _get_index()

    # 3. Search
    # Ensure embedding is numpy array
    embedding_np = np.array(embedding, dtype=np.float32)

    results = index.search(embedding_np, k=limit)

    if not results:
        return ""

    # 4. Format results
    # We fetch content from metadata stored in the index
    examples = []
    for res in results:
        metadata = res.metadata or {}
        examples.append(
            {
                "question": metadata.get("question", ""),
                "sql": metadata.get("sql", ""),
                "similarity": float(res.score),
            }
        )

    return json.dumps(examples, separators=(",", ":"))
