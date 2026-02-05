"""Tests for schema fingerprint utilities."""

from agent.utils.schema_fingerprint import fingerprint_schema_nodes


def test_schema_fingerprint_deterministic_ordering():
    """Fingerprint should be stable regardless of input ordering."""
    nodes_a = [
        {"type": "Table", "name": "users"},
        {"type": "Column", "name": "id", "data_type": "int", "table": "users"},
        {"type": "Column", "name": "email", "data_type": "text", "table": "users"},
    ]
    nodes_b = [
        {"type": "Column", "name": "email", "data_type": "text", "table": "users"},
        {"type": "Table", "name": "users"},
        {"type": "Column", "name": "id", "data_type": "int", "table": "users"},
    ]

    assert fingerprint_schema_nodes(nodes_a) == fingerprint_schema_nodes(nodes_b)


def test_schema_fingerprint_changes_on_column_add():
    """Fingerprint should change when columns change."""
    nodes = [
        {"type": "Table", "name": "orders"},
        {"type": "Column", "name": "id", "data_type": "int", "table": "orders"},
    ]
    nodes_with_extra = nodes + [
        {"type": "Column", "name": "total", "data_type": "numeric", "table": "orders"}
    ]

    assert fingerprint_schema_nodes(nodes) != fingerprint_schema_nodes(nodes_with_extra)
