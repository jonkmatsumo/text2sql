import ast
from pathlib import Path

import pytest

FORBIDDEN_PACKAGES = {
    "mcp",
    "agent",
    "synthetic_data_gen",
    "ui",
    "dal",
    "ingestion",
}


def test_no_downstream_imports():
    """Ensure common/ package does not import from downstream services."""
    common_src = Path(__file__).parent.parent / "src" / "common"

    violations = []

    for py_file in common_src.rglob("*.py"):
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
        message = "Forbidden downstream imports found in common/:\n" + "\n".join(violations)
        pytest.fail(message)
