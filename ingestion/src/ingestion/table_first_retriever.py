"""Table-First Retrieval Strategy.

Implements a two-phase retrieval approach:
1. Find relevant tables first
2. Retrieve columns scoped to those tables with diversity cap

This reduces context window flooding and ensures focused prompt context.
"""

import logging
from collections import defaultdict
from typing import List, Optional

import numpy as np

from common.interfaces.vector_index import SearchResult, VectorIndex

from .vector_indexes import search_with_rerank

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_TABLE_K = 10
DEFAULT_COLUMN_K = 30  # Per-table columns to fetch before diversity cap
DEFAULT_MAX_COLUMNS_PER_TABLE = 3
DEFAULT_RERANK_EXPANSION = 10


def format_table_text(table_name: str, description: Optional[str] = None) -> str:
    """Format table for embedding search.

    Args:
        table_name: Name of the table.
        description: Optional description.

    Returns:
        Formatted text for embedding.
    """
    text = f"Table: {table_name}"
    if description:
        text += f" | Description: {description}"
    return text


def format_column_text(
    table_name: str,
    column_name: str,
    description: Optional[str] = None,
    data_type: Optional[str] = None,
) -> str:
    """Format column for embedding search.

    Enriched format maximizes semantic signal:
    Table: {table} | Column: {col} | Desc: {desc}

    Args:
        table_name: Parent table name.
        column_name: Column name.
        description: Optional description.
        data_type: Optional data type.

    Returns:
        Formatted text for embedding.
    """
    text = f"Table: {table_name} | Column: {column_name}"
    if description:
        text += f" | Desc: {description}"
    if data_type:
        text += f" | Type: {data_type}"
    return text


class TableFirstRetriever:
    """Two-phase retrieval: tables first, then scoped columns.

    Implements the table-first strategy to reduce context window flooding:
    1. Query table_index to find top-K relevant tables
    2. Collect table IDs
    3. Query column_index filtered to only those tables
    4. Enforce diversity cap (max N columns per table)
    """

    def __init__(
        self,
        table_index: "VectorIndex",
        column_index: "VectorIndex",
        table_k: int = DEFAULT_TABLE_K,
        column_k: int = DEFAULT_COLUMN_K,
        max_columns_per_table: int = DEFAULT_MAX_COLUMNS_PER_TABLE,
        use_rerank: bool = True,
        rerank_expansion: int = DEFAULT_RERANK_EXPANSION,
    ) -> None:
        """Initialize TableFirstRetriever.

        Args:
            table_index: VectorIndex for table searches.
            column_index: VectorIndex for column searches.
            table_k: Number of tables to retrieve.
            column_k: Number of columns to retrieve (before diversity cap).
            max_columns_per_table: Maximum columns per table (diversity cap).
            use_rerank: Whether to use retrieve-and-rerank strategy.
            rerank_expansion: Expansion factor for reranking.
        """
        self.table_index = table_index
        self.column_index = column_index
        self.table_k = table_k
        self.column_k = column_k
        self.max_columns_per_table = max_columns_per_table
        self.use_rerank = use_rerank
        self.rerank_expansion = rerank_expansion

    def retrieve(
        self,
        query_vector: np.ndarray,
        table_k: Optional[int] = None,
        column_k: Optional[int] = None,
        max_columns_per_table: Optional[int] = None,
    ) -> dict:
        """Execute table-first retrieval.

        Args:
            query_vector: The query embedding.
            table_k: Override number of tables to retrieve.
            column_k: Override number of columns to retrieve.
            max_columns_per_table: Override diversity cap.

        Returns:
            Dict with 'tables' and 'columns' lists, each containing SearchResult.
        """
        table_k = table_k or self.table_k
        column_k = column_k or self.column_k
        max_per_table = max_columns_per_table or self.max_columns_per_table

        # Step 1: Query table_index to find top-K relevant tables
        logger.debug(f"Retrieving top-{table_k} tables...")
        if self.use_rerank:
            table_results = search_with_rerank(
                self.table_index,
                query_vector,
                k=table_k,
                expansion_factor=self.rerank_expansion,
            )
        else:
            table_results = self.table_index.search(query_vector, k=table_k)

        if not table_results:
            logger.debug("No tables found")
            return {"tables": [], "columns": []}

        logger.debug(f"Found {len(table_results)} tables")

        # Step 2: Collect table IDs and build lookup
        table_ids = set()
        table_name_to_id = {}
        for result in table_results:
            table_ids.add(result.id)
            if result.metadata and "name" in result.metadata:
                table_name_to_id[result.metadata["name"]] = result.id

        # Step 3: Query column_index (expanded to get enough for filtering)
        # We fetch more than needed to account for filtering
        expanded_column_k = column_k * 3
        logger.debug(f"Retrieving columns with expanded k={expanded_column_k}...")

        if self.use_rerank:
            all_column_results = search_with_rerank(
                self.column_index,
                query_vector,
                k=expanded_column_k,
                expansion_factor=self.rerank_expansion,
            )
        else:
            all_column_results = self.column_index.search(query_vector, k=expanded_column_k)

        # Step 3b: Filter to only columns belonging to retrieved tables
        filtered_columns = self._filter_columns_by_tables(
            all_column_results, table_ids, table_name_to_id
        )

        logger.debug(f"Filtered columns: {len(all_column_results)} -> {len(filtered_columns)}")

        # Step 4: Apply diversity cap (max N columns per table)
        diverse_columns = self._apply_diversity_cap(filtered_columns, max_per_table)

        # Limit to requested column_k
        diverse_columns = diverse_columns[:column_k]

        logger.info(
            f"TableFirstRetrieval: {len(table_results)} tables, "
            f"{len(diverse_columns)} columns (max {max_per_table}/table)"
        )

        return {
            "tables": table_results,
            "columns": diverse_columns,
        }

    def _filter_columns_by_tables(
        self,
        column_results: List[SearchResult],
        table_ids: set,
        table_name_to_id: dict,
    ) -> List[SearchResult]:
        """Filter columns to only those belonging to retrieved tables.

        Args:
            column_results: All column search results.
            table_ids: Set of table IDs from table search.
            table_name_to_id: Mapping of table names to IDs.

        Returns:
            Filtered list of column results.
        """
        filtered = []
        for col in column_results:
            if not col.metadata:
                continue

            # Check by table_id if present in metadata
            if "table_id" in col.metadata:
                if col.metadata["table_id"] in table_ids:
                    filtered.append(col)
                    continue

            # Check by table name
            if "table" in col.metadata:
                table_name = col.metadata["table"]
                if table_name in table_name_to_id:
                    filtered.append(col)
                    continue

        return filtered

    def _apply_diversity_cap(
        self,
        column_results: List[SearchResult],
        max_per_table: int,
    ) -> List[SearchResult]:
        """Apply diversity cap: max N columns per table.

        Preserves ranking order within each table.

        Args:
            column_results: Filtered column results (already sorted by score).
            max_per_table: Maximum columns per table.

        Returns:
            Diverse column list.
        """
        table_counts: dict[str | int, int] = defaultdict(int)
        diverse = []

        for col in column_results:
            if not col.metadata:
                continue

            # Get table identifier
            table_key = col.metadata.get("table_id") or col.metadata.get("table")
            if not table_key:
                continue

            # Apply cap
            if table_counts[table_key] < max_per_table:
                diverse.append(col)
                table_counts[table_key] += 1

        return diverse

    def retrieve_with_text_query(
        self,
        query_text: str,
        embed_func,
        **kwargs,
    ) -> dict:
        """Embed query text and retrieve results.

        Args:
            query_text: Natural language query.
            embed_func: Function to embed text -> np.ndarray.
            **kwargs: Passed to retrieve().

        Returns:
            Dict with 'tables' and 'columns'.
        """
        query_vector = np.array(embed_func(query_text), dtype=np.float32)
        return self.retrieve(query_vector, **kwargs)
