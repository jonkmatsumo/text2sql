"""Utility for Memgraph Vector Index DDL operations.

This module isolates the logic for creating vector indexes in Memgraph,
ensuring idempotency and correct syntax.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def ensure_table_embedding_hnsw_index(session: Any, *, dims: int = 1536) -> bool:
    """Ensure that the HNSW vector index exists for :Table(embedding).

    This function is idempotent. It attempts to create the index and
    gracefully handles the case where it already exists.

    Args:
        session: A Memgraph/Neo4j driver session object.
        dims: The dimension of the vector embedding (default: 1536).

    Returns:
        bool: True if index was created, False if it already existed (or creation failed safely).
    """
    # Native Memgraph Vector Index Syntax
    # Ref: https://memgraph.com/docs/vector-search/
    index_name = "table_embedding_index"
    label = "Table"
    property_name = "embedding"
    metric = "cosine"

    query = (
        f"CREATE VECTOR INDEX {index_name} ON :{label}({property_name}) "
        f"WITH CONFIG {{'dimension': {dims}, 'metric': '{metric}'}}"
    )

    import time

    start_time = time.monotonic()

    try:
        logger.info(f"Ensuring vector index '{index_name}' on :{label}({property_name})...")
        session.run(query)
        elapsed_ms = (time.monotonic() - start_time) * 1000

        logger.info(
            f"✓ Created vector index '{index_name}'",
            extra={
                "event": "memgraph_vector_index_ensure",
                "index": index_name,
                "created": True,
                "elapsed_ms": elapsed_ms,
                "dims": dims,
            },
        )
        return True

    except Exception as e:
        elapsed_ms = (time.monotonic() - start_time) * 1000
        error_msg = str(e).lower()

        # Memgraph often raises ClientError if index exists.
        # Check for common "already exists" indicators.
        if "already exists" in error_msg:
            logger.debug(
                f"Vector index '{index_name}' already exists.",
                extra={
                    "event": "memgraph_vector_index_ensure",
                    "index": index_name,
                    "created": False,
                    "reason": "already_exists",
                    "elapsed_ms": elapsed_ms,
                },
            )
            return False

        # If it's a different error, we log warning but might re-raise depending on strictness.
        # For Phase 1, we want to be safe at startup, but failing to create index
        # usually suggests a configuration issue or unsupported Memgraph version.
        # Re-raising is safer to detect issues early, as long as we catch "already exists".
        logger.error(
            f"Failed to create vector index '{index_name}': {e}",
            extra={
                "event": "memgraph_vector_index_failure",
                "index": index_name,
                "error": str(e),
                "elapsed_ms": elapsed_ms,
            },
        )
        raise e
