import os
import subprocess
import sys


def test_graph_import_no_mlflow():
    """Verify that importing agent.graph does not load mlflow into sys.modules."""
    # Run in a subprocess to ensure a clean sys.modules environment
    code = """
import sys
import os
from unittest.mock import MagicMock

import importlib.util
import importlib.machinery
import types

# Mock MCP SDK only if not installed
if importlib.util.find_spec("mcp") is None:
    def create_mock_module(name):
        mock = types.ModuleType(name)
        mock.__spec__ = importlib.machinery.ModuleSpec(name, loader=None)
        mock.__path__ = []
        sys.modules[name] = mock
        return mock

    mcp_mock = create_mock_module("mcp")
    create_mock_module("mcp_server.types")
    create_mock_module("mcp_server.client")
    create_mock_module("mcp_server.client.sse")
    create_mock_module("mcp_server.client.streamable_http")



# Import the module under test
os.environ["TELEMETRY_BACKEND"] = "otel"
from agent import graph

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
