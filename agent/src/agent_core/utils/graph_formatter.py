"""Graph formatter utility for converting semantic subgraph to Markdown.

Includes budget enforcement to prevent schema context explosion in LLM prompts.
"""

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

# Hard limit on schema context size (~2000 tokens at 4 chars/token)
SCHEMA_CONTEXT_MAX_CHARS = 8000

# Maximum columns to show per table before truncating
MAX_COLUMNS_PER_TABLE = 10


def format_graph_to_markdown(
    graph_data: Dict,
    max_chars: int = SCHEMA_CONTEXT_MAX_CHARS,
    max_cols_per_table: int = MAX_COLUMNS_PER_TABLE,
) -> str:
    """Convert graph JSON from get_semantic_subgraph to LLM-readable Markdown.

    Enforces a character budget to prevent prompt explosion.
    Tables and columns are dropped progressively if budget is exceeded.

    Args:
        graph_data: Dictionary with "nodes" and "relationships" keys.
        max_chars: Maximum allowed characters in output.
        max_cols_per_table: Maximum columns to show per table.

    Returns:
        Markdown-formatted string summarizing the schema graph.
    """
    nodes = graph_data.get("nodes", [])
    relationships = graph_data.get("relationships", [])

    if not nodes:
        return "No relevant tables found."

    # Index nodes by ID for quick lookup
    node_map = {n.get("id"): n for n in nodes}

    # Separate tables and columns
    tables = [n for n in nodes if n.get("type") == "Table"]

    # Build column-to-table mapping from HAS_COLUMN relationships
    table_columns: Dict[str, List[Dict]] = {t.get("id"): [] for t in tables}
    foreign_keys: List[Dict] = []

    for rel in relationships:
        rel_type = rel.get("type")
        source_id = rel.get("source")
        target_id = rel.get("target")

        if rel_type == "HAS_COLUMN":
            if source_id in table_columns:
                col_node = node_map.get(target_id, {})
                table_columns[source_id].append(col_node)
        elif rel_type == "FOREIGN_KEY_TO":
            foreign_keys.append(rel)

    # Sort tables by score (if available) descending - drop lowest scores first
    tables_with_scores = []
    for t in tables:
        score = t.get("score", t.get("similarity", 0.5))
        tables_with_scores.append((t, score))
    tables_with_scores.sort(key=lambda x: x[1], reverse=True)

    # Try formatting with progressively fewer tables until under budget
    result = _format_with_budget(
        tables_with_scores,
        table_columns,
        foreign_keys,
        node_map,
        max_chars,
        max_cols_per_table,
    )

    return result


def _format_with_budget(
    tables_with_scores: List[tuple],
    table_columns: Dict[str, List[Dict]],
    foreign_keys: List[Dict],
    node_map: Dict,
    max_chars: int,
    max_cols_per_table: int,
) -> str:
    """Format tables with budget enforcement.

    Progressively drops tables and reduces columns until under budget.
    """
    num_tables = len(tables_with_scores)

    # Try with all tables first, then reduce
    for attempt_tables in range(num_tables, 0, -1):
        # Try with full columns, then reduce
        for attempt_cols in [max_cols_per_table, 5, 3]:
            output = _render_tables(
                tables_with_scores[:attempt_tables],
                table_columns,
                foreign_keys,
                node_map,
                attempt_cols,
            )

            if len(output) <= max_chars:
                if attempt_tables < num_tables or attempt_cols < max_cols_per_table:

                    dropped_tables = num_tables - attempt_tables
                    logger.warning(
                        f"Schema context truncated: {dropped_tables} tables dropped, "
                        f"{attempt_cols} cols/table. Final size: {len(output)} chars"
                    )
                    # Add truncation marker
                    output += f"\n\n_...truncated ({dropped_tables} tables omitted)_"
                return output

    # Still over budget after all reductions - hard truncate
    output = _render_tables(
        tables_with_scores[:1],
        table_columns,
        foreign_keys,
        node_map,
        3,
    )
    logger.warning(f"Schema context hard-truncated to 1 table. Original: {num_tables} tables")
    return output[: max_chars - 50] + "\n\n_...truncated (schema too large)_"


def _render_tables(
    tables_with_scores: List[tuple],
    table_columns: Dict[str, List[Dict]],
    foreign_keys: List[Dict],
    node_map: Dict,
    max_cols: int,
) -> str:
    """Render tables to markdown with column limit."""
    output_parts = []

    for table, score in tables_with_scores:
        table_id = table.get("id")
        table_name = table.get("name", "Unknown")
        table_desc = table.get("description", "")

        output_parts.append(f"## Table: {table_name}")
        if table_desc:
            output_parts.append(f"_{table_desc}_")
        output_parts.append("")

        # List columns (limited)
        cols = table_columns.get(table_id, [])
        col_count = len(cols)

        for i, col in enumerate(cols[:max_cols]):
            col_name = col.get("name", "unknown")
            col_type = col.get("data_type", col.get("type", "unknown"))
            output_parts.append(f"- **{col_name}** ({col_type})")

        if col_count > max_cols:
            output_parts.append(f"- _...and {col_count - max_cols} more columns_")

        output_parts.append("")

    # Add FK connections (compact format)
    if foreign_keys:
        output_parts.append("### Joins")
        seen_joins = set()
        for fk in foreign_keys:
            source_col = node_map.get(fk.get("source"), {})
            target_col = node_map.get(fk.get("target"), {})
            source_table = source_col.get("table", "?")
            target_table = target_col.get("table", "?")
            join_key = f"{source_table}->{target_table}"
            if join_key not in seen_joins:
                seen_joins.add(join_key)
                output_parts.append(f"- {source_table} → {target_table}")
        output_parts.append("")

    return "\n".join(output_parts)
