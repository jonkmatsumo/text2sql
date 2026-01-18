import os
import subprocess
import sys


def test_graph_import_no_mlflow():
    """Verify that importing agent_core.graph does not load mlflow into sys.modules."""
    # Run in a subprocess to ensure a clean sys.modules environment
    code = """
import sys
import os
from unittest.mock import MagicMock

# Mock MCP SDK to allow import of agent_core.tools
if "mcp" not in sys.modules:
    mcp_mock = MagicMock()
    sys.modules["mcp"] = mcp_mock
    sys.modules["mcp.client"] = mcp_mock
    sys.modules["mcp.client.sse"] = mcp_mock
    sys.modules["mcp.client.streamable_http"] = mcp_mock

# Import the module under test
os.environ["TELEMETRY_BACKEND"] = "otel"
from agent_core import graph

# Assert mlflow is not loaded
if "mlflow" in sys.modules:
    print("mlflow_imported")
    sys.exit(1)
else:
    print("success")
    sys.exit(0)
"""
    cwd = os.getcwd()
    pythonpath = os.path.join(cwd, "agent", "src")

    env = os.environ.copy()
    env["PYTHONPATH"] = pythonpath + os.pathsep + env.get("PYTHONPATH", "")

    result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)

    assert (
        result.returncode == 0
    ), f"MLflow was imported or import failed: {result.stdout} {result.stderr}"
    assert "success" in result.stdout
