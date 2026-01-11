from typing import Any, Dict, List, Set

import numpy as np

from .engine import RagEngine


class SchemaLinker:
    """Implement Dense Schema Linking (Triple-Filter Strategy).

    Intelligently selects relevant columns for the LLM context.
    """

    @classmethod
    async def rank_and_filter_columns(
        cls,
        user_query: str,
        table_nodes: List[Dict[str, Any]],
        target_cols_per_table: int = 15,
    ) -> List[Dict[str, Any]]:
        """Filter columns for each table using Structural -> Value -> Semantic ranking.

        Args:
            user_query: The natural language query from the user.
            table_nodes: List of Table nodes, each containing 'columns' list and 'sample_data'.
            target_cols_per_table: Maximum columns to keep per table.

        Returns:
            The modified list of table nodes with filtered columns.

        """
        query_embedding = None

        for table in table_nodes:
            all_columns = table.get("columns", [])
            if not all_columns:
                continue

            # If table is small enough, keep all
            if len(all_columns) <= target_cols_per_table:
                continue

            kept_columns: List[Dict[str, Any]] = []
            kept_col_names: Set[str] = set()

            candidate_columns: List[Dict[str, Any]] = []

            # 1. Structural Filter & 2. Value Spy
            sample_data = table.get("sample_data")
            if isinstance(sample_data, str):
                import json

                try:
                    sample_data = json.loads(sample_data)
                except Exception:
                    sample_data = []

            for col in all_columns:
                # Structural: Always keep PK and FK
                is_structural = (
                    col.get("is_primary_key")
                    or col.get("is_foreign_key")
                    or col.get("name", "").lower().endswith("_id")
                )

                if is_structural:
                    kept_columns.append(col)
                    kept_col_names.add(col["name"])
                    continue

                # Value Spy: Check if query terms appear in sample data for this column
                if cls._is_value_match(user_query, col, sample_data):
                    kept_columns.append(col)
                    kept_col_names.add(col["name"])
                    continue

                # Candidates for semantic ranking
                candidate_columns.append(col)

            # Check budget
            remaining_slots = target_cols_per_table - len(kept_columns)

            if remaining_slots > 0 and candidate_columns:
                # 3. Semantic Reranker
                if query_embedding is None:
                    query_embedding = await RagEngine.embed_text(user_query)

                # Prepare corpus: "table: column - description"
                corpus_texts = []
                for col in candidate_columns:
                    desc = col.get("description", "")
                    text = f"{table['name']}: {col['name']}"
                    if desc:
                        text += f" - {desc}"
                    corpus_texts.append(text)

                col_embeddings = await RagEngine.embed_batch(corpus_texts)

                # Calculate Cosine Similarity
                q_vec = np.array(query_embedding)
                c_vecs = np.array(col_embeddings)

                scores = np.dot(c_vecs, q_vec)

                # Sort indices high to low
                ranked_indices = np.argsort(scores)[::-1]

                for idx in ranked_indices[:remaining_slots]:
                    kept_columns.append(candidate_columns[idx])

            # Replace columns in table node
            table["columns"] = kept_columns

        return table_nodes

    @staticmethod
    def _is_value_match(query: str, col: Dict[str, Any], sample_data: List[Dict[str, Any]]) -> bool:
        """Check if query words strictly match any value in the column's sample data."""
        if not sample_data:
            return False

        col_name = col.get("name")
        if not col_name:
            return False

        # Get all values for this column from sample rows
        values = []
        for row in sample_data:
            val = row.get(col_name)
            if val:
                values.append(str(val).lower())

        if not values:
            return False

        query_lower = query.lower()

        for val in values:
            if len(val) < 2:
                continue

            if val in query_lower:
                return True

        return False
