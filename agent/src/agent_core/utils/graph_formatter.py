"""Graph formatter utility for converting semantic subgraph to compact Markdown.

Includes strict budget enforcement and prioritization to prevent schema context explosion.
"""

import logging
from typing import Dict, List, Set

logger = logging.getLogger(__name__)

# Hard caps for compact format
MAX_TABLES = 8
MAX_COLS_PER_TABLE = 15  # Increased from 10 to show more columns per table
SCHEMA_CONTEXT_MAX_CHARS = 8000


def format_graph_to_markdown(
    graph_data: Dict,
    max_chars: int = SCHEMA_CONTEXT_MAX_CHARS,
    max_tables: int = MAX_TABLES,
    max_cols_per_table: int = MAX_COLS_PER_TABLE,
) -> str:
    """Convert graph JSON to compact LLM-readable Markdown.

    Format:
    # Schema Context
    ## Tables
    - **TableA** (col1 `pk`, col2 `fk`, col3)
    - **TableB** (col1 `pk`, col4)

    ## Joins
    - **TableA** JOIN **TableB** ON col2

    Args:
        graph_data: Dictionary with "nodes" and "relationships" keys.
        max_chars: Maximum allowed characters in output.
        max_tables: Maximum number of tables to include.
        max_cols_per_table: Maximum columns to show per table.

    Returns:
        Markdown-formatted string summarizing the schema graph.
    """
    nodes = graph_data.get("nodes", [])
    relationships = graph_data.get("relationships", [])

    if not nodes:
        return "No relevant tables found."

    # Index nodes by ID
    node_map = {n.get("id"): n for n in nodes}

    # Separate tables and columns
    tables = [n for n in nodes if n.get("type") == "Table"]

    # Filter tables: Sort by score desc, take top max_tables
    tables.sort(key=lambda t: t.get("score", t.get("similarity", 0.5)), reverse=True)

    dropped_table_count = max(0, len(tables) - max_tables)
    tables = tables[:max_tables]
    table_ids = {t.get("id") for t in tables}

    # Map columns to tables and track FKs
    table_columns: Dict[str, List[Dict]] = {t_id: [] for t_id in table_ids}
    foreign_keys: List[Dict] = []

    # Columns involved in joins (to prioritize them)
    join_column_ids: Set[str] = set()

    for rel in relationships:
        rel_type = rel.get("type")
        source_id = rel.get("source")
        target_id = rel.get("target")

        if rel_type == "HAS_COLUMN":
            if source_id in table_ids:
                col_node = node_map.get(target_id)
                if col_node:
                    table_columns[source_id].append(col_node)
        elif rel_type == "FOREIGN_KEY_TO":
            foreign_keys.append(rel)
            join_column_ids.add(source_id)
            join_column_ids.add(target_id)

    # Helper to calculate column priority
    def get_column_priority(col: Dict) -> int:
        # Lower is higher priority
        if col.get("is_primary_key"):
            return 0
        if col.get("id") in join_column_ids:
            return 1

        # Prioritize common filter/queryable columns
        important_cols = {"rating", "name", "title", "status", "type", "category", "amount"}
        col_name = col.get("name", "").lower()
        if col_name in important_cols:
            return 2

        # Semantic columns (text types useful for filtering)
        dtype = col.get("data_type", col.get("type", "")).lower()
        if "char" in dtype or "text" in dtype or "string" in dtype:
            return 3

        return 4

    # Format Output
    output_parts = ["# Schema Context", "", "## Tables"]

    for table in tables:
        t_id = table.get("id")
        t_name = table.get("name")

        cols = table_columns.get(t_id, [])
        # Prioritize columns
        cols.sort(key=get_column_priority)

        # Take top N
        display_cols = cols[:max_cols_per_table]

        col_strings = []
        for col in display_cols:
            c_name = col.get("name")
            flags = []
            if col.get("is_primary_key"):
                flags.append("`pk`")
            # We can't easily know if *this specific column* is an FK without
            # checking relationships again efficiently. But we tracked join_column_ids.
            if col.get("id") in join_column_ids and not col.get("is_primary_key"):
                flags.append("`fk`")

            flag_str = f" {' '.join(flags)}" if flags else ""
            col_strings.append(f"{c_name}{flag_str}")

        cols_str = ", ".join(col_strings)
        if len(cols) > max_cols_per_table:
            cols_str += ", ..."

        output_parts.append(f"- **{t_name}** ({cols_str})")

    if dropped_table_count > 0:
        output_parts.append(f"_...{dropped_table_count} more tables omitted_")

    # Format Joins (Table-Level)
    if foreign_keys:
        output_parts.extend(["", "## Joins"])
        seen_joins = set()

        for fk in foreign_keys:
            src_col = node_map.get(fk.get("source"))
            tgt_col = node_map.get(fk.get("target"))

            if not src_col or not tgt_col:
                continue

            src_table_name = src_col.get("table")
            tgt_table_name = tgt_col.get("table")

            # Ensure we only show joins involving our kept tables
            # (Though our query already filters this largely, verify to be safe)
            # Actually, `foreign_keys` includes all FKs from graph_data.
            # We should only show if at least one side is in our `tables` list?
            # Or if both? Usually both for context.
            # Let's show if at least Source or Target table is in our list.

            # Simple deduplication key
            join_key = f"{src_table_name} JOIN {tgt_table_name} ON {src_col.get('name')}"

            if join_key not in seen_joins:
                seen_joins.add(join_key)
                # Output: TableA JOIN TableB ON col
                output_parts.append(
                    f"- **{src_table_name}** JOIN **{tgt_table_name}** " f"ON {src_col.get('name')}"
                )

    final_output = "\n".join(output_parts)

    # Final hard truncation if somehow still massive (unlikely with caps)
    if len(final_output) > max_chars:
        return final_output[: max_chars - 50] + "\n\n_...truncated_"

    return final_output
