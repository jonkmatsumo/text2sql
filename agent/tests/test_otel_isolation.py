import os
import subprocess
import sys

import pytest

# Define test cases: (backend, expect_mlflow_imported)
TEST_CASES = [
    ("otel", False),
]


@pytest.mark.parametrize("backend,expect_import", TEST_CASES)
def test_otel_isolation(backend, expect_import):
    """Verify loading agent_core.graph with different backends respects mlflow import isolation."""
    code = f"""
import sys
import os

# Mock MCP SDK to allow import of agent_core.tools without the package installed
from unittest.mock import MagicMock
if "mcp" not in sys.modules:
    mcp_mock = MagicMock()
    sys.modules["mcp"] = mcp_mock
    sys.modules["mcp.client"] = mcp_mock
    sys.modules["mcp.client.sse"] = mcp_mock
    sys.modules["mcp.client.streamable_http"] = mcp_mock

# Set backend
os.environ["TELEMETRY_BACKEND"] = "{backend}"

# Clear any potentially conflicting env vars
if "MLFLOW_TRACKING_URI" in os.environ:
    del os.environ["MLFLOW_TRACKING_URI"]

try:
    from agent_core import graph
except ImportError as e:
    print(f"IMPORT ERROR: {{e}}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Check if mlflow is loaded
if "mlflow" in sys.modules:
    print("STATUS: IMPORTED")
else:
    print("STATUS: NOT_IMPORTED")
"""
    # Run in a subprocess to ensure clean sys.modules
    cwd = os.getcwd()
    if os.path.exists(os.path.join(cwd, "agent", "src")):
        pythonpath = os.path.join(cwd, "agent", "src")
    elif os.path.exists(os.path.join(cwd, "src")):
        pythonpath = os.path.join(cwd, "src")
    else:
        pythonpath = cwd

    env = os.environ.copy()
    env["PYTHONPATH"] = pythonpath + os.pathsep + env.get("PYTHONPATH", "")

    result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)

    # print debug info on failure
    if result.returncode != 0:
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)

    assert result.returncode == 0, f"Subprocess failed for backend {backend}"

    if expect_import:
        assert "STATUS: IMPORTED" in result.stdout, f"Expected mlflow import for backend {backend}"
    else:
        assert (
            "STATUS: NOT_IMPORTED" in result.stdout
        ), f"Expected NO mlflow import for backend {backend}"
