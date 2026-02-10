import ast
import os

import pytest


def test_sql_policy_is_canonical():
    """Assert SQL policy constants are defined in the canonical module and NOT elsewhere."""
    canonical_path = os.path.abspath("src/common/policy/sql_policy.py")

    # Key constants that define our SQL policy
    policy_markers = ["ALLOWED_STATEMENT_TYPES", "BLOCKED_FUNCTIONS"]

    # We'll search the codebase (excluding tests and the canonical file itself)
    # for assignments to these names.
    search_dirs = ["src/agent", "src/mcp_server", "src/dal"]

    for search_dir in search_dirs:
        for root, _, files in os.walk(search_dir):
            for file in files:
                if not file.endswith(".py"):
                    continue

                path = os.path.join(root, file)
                if path == canonical_path:
                    continue

                with open(path, "r") as f:
                    content = f.read()

                # Simple check first
                for marker in policy_markers:
                    # Look for Assignment of these markers.
                    # We allow imports/usage but not re-definition.
                    if f"{marker} =" in content:
                        # Parse AST to be sure it's an assignment at top level
                        tree = ast.parse(content)
                        for node in tree.body:
                            if isinstance(node, ast.Assign):
                                for target in node.targets:
                                    if isinstance(target, ast.Name) and target.id == marker:
                                        pytest.fail(
                                            f"Policy drift detected! {marker} redefined in {path}. "
                                            f"All SQL policies must be defined in {canonical_path}."
                                        )


def test_policy_enforcer_uses_canonical_policy():
    """Assert PolicyEnforcer imports from the canonical policy module."""
    enforcer_path = "src/agent/validation/policy_enforcer.py"
    with open(enforcer_path, "r") as f:
        content = f.read()

    assert (
        "from common.policy.sql_policy import" in content
        or "import common.policy.sql_policy" in content
    )
