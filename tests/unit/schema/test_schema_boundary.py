"""Boundary test ensuring schema package remains lightweight.

This test prevents schema from importing downstream or heavy dependencies.
"""

import ast
from pathlib import Path


def test_schema_has_no_forbidden_imports():
    """Verify schema package does not import forbidden dependencies."""
    forbidden_prefixes = (
        "mcp",
        "agent",
        "ui",
        "asyncpg",
        "neo4j",
        "openai",
        "langchain",
        "memgraph",
        "psycopg",
    )

    schema_src = Path(__file__).parent.parent / "src" / "schema"
    violations = []

    for py_file in schema_src.rglob("*.py"):
        try:
            tree = ast.parse(py_file.read_text())
        except SyntaxError:
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(forbidden_prefixes):
                        violations.append(f"{py_file.name}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module and node.module.startswith(forbidden_prefixes):
                    violations.append(f"{py_file.name}: from {node.module}")

    assert not violations, "Forbidden imports in schema/:\n" + "\n".join(violations)
