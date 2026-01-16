import ast
from pathlib import Path

import pytest

FORBIDDEN_PACKAGES = {
    "mcp_server",
}


def test_no_upstream_imports():
    """Ensure ingestion/ package does not import from mcp_server."""
    # Assumes ingestion/tests/test_boundary.py -> ingestion/tests -> ingestion
    ingestion_src = Path(__file__).parent.parent / "src" / "ingestion"

    violations = []

    if not ingestion_src.exists():
        # In case structure is different or empty
        return

    for py_file in ingestion_src.rglob("*.py"):
        try:
            with open(py_file, "r", encoding="utf-8") as f:
                tree = ast.parse(f.read(), filename=str(py_file))
        except Exception as e:
            violations.append(f"Could not parse {py_file}: {e}")
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    pkg = alias.name.split(".")[0]
                    if pkg in FORBIDDEN_PACKAGES:
                        violations.append(f"{py_file.name}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    pkg = node.module.split(".")[0]
                    if pkg in FORBIDDEN_PACKAGES:
                        violations.append(f"{py_file.name}: from {node.module} import ...")

    if violations:
        message = "Forbidden upstream imports found in ingestion/:\n" + "\n".join(violations)
        pytest.fail(message)
