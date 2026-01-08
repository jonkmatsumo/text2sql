"""Graph formatter utility for converting semantic subgraph to Markdown."""

from typing import Dict, List


def format_graph_to_markdown(graph_data: Dict) -> str:
    """Convert graph JSON from get_semantic_subgraph to LLM-readable Markdown.

    Args:
        graph_data: Dictionary with "nodes" and "relationships" keys.

    Returns:
        Markdown-formatted string summarizing the schema graph.
    """
    nodes = graph_data.get("nodes", [])
    relationships = graph_data.get("relationships", [])

    # Index nodes by ID for quick lookup
    node_map = {n.get("id"): n for n in nodes}

    # Separate tables and columns
    # Separate tables from nodes (columns are accessed via relationships)
    tables = [n for n in nodes if n.get("type") == "Table"]

    # Build column-to-table mapping from HAS_COLUMN relationships
    table_columns: Dict[str, List[Dict]] = {t.get("id"): [] for t in tables}
    foreign_keys: List[Dict] = []

    for rel in relationships:
        rel_type = rel.get("type")
        source_id = rel.get("source")
        target_id = rel.get("target")

        if rel_type == "HAS_COLUMN":
            # Source is Table, Target is Column
            if source_id in table_columns:
                col_node = node_map.get(target_id, {})
                table_columns[source_id].append(col_node)
        elif rel_type == "FOREIGN_KEY_TO":
            # Source is Column, Target is Table
            foreign_keys.append(rel)

    # Build markdown output
    output_parts = []

    for table in tables:
        table_id = table.get("id")
        table_name = table.get("name", "Unknown")
        table_desc = table.get("description", "")

        output_parts.append(f"## Table: {table_name}")
        if table_desc:
            output_parts.append(f"_{table_desc}_")
        output_parts.append("")

        # List columns
        cols = table_columns.get(table_id, [])
        if cols:
            for col in cols:
                col_name = col.get("name", "unknown")
                col_type = col.get("data_type", col.get("type", "unknown"))
                col_desc = col.get("description", "")
                line = f"- **{col_name}** ({col_type})"
                if col_desc:
                    line += f": {col_desc}"
                output_parts.append(line)
            output_parts.append("")

    # Add connections section if any foreign keys exist
    if foreign_keys:
        output_parts.append("### Connections")
        output_parts.append("")
        for fk in foreign_keys:
            source_col = node_map.get(fk.get("source"), {})
            target_table = node_map.get(fk.get("target"), {})
            col_name = source_col.get("name", "unknown")
            target_name = target_table.get("name", "unknown")
            output_parts.append(f"* Joins to [{target_name}] via [{col_name}]")
        output_parts.append("")

    return "\n".join(output_parts)
