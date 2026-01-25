import os
import subprocess
import sys

import pytest

# Define test cases: (backend, expect_import). OTEL-only; we never expect mlflow imported.
TEST_CASES = [
    ("otel", False),
]


@pytest.mark.parametrize("backend,expect_import", TEST_CASES)
def test_otel_isolation(backend, expect_import):
    """Verify loading agent.graph is OTEL-only; no mlflow import."""
    code = f"""
import sys
import os

# Mock MCP SDK only if not installed
import importlib.util
import importlib.machinery
import types
from unittest.mock import MagicMock
if importlib.util.find_spec("mcp") is None:
    def create_mock_module(name):
        mock = types.ModuleType(name)
        mock.__spec__ = importlib.machinery.ModuleSpec(name, loader=None)
        mock.__path__ = []
        sys.modules[name] = mock
        return mock

    mcp_mock = create_mock_module("mcp")
    mcp_types = create_mock_module("mcp_server.types")
    mcp_mock.types = mcp_types
    create_mock_module("mcp_server.client")
    create_mock_module("mcp_server.client.sse")
    create_mock_module("mcp_server.client.streamable_http")


# Set backend
os.environ["TELEMETRY_BACKEND"] = "{backend}"

# Clear any potentially conflicting env vars
if "MLFLOW_TRACKING_URI" in os.environ:
    del os.environ["MLFLOW_TRACKING_URI"]

try:
    from agent import graph
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
        assert "STATUS: IMPORTED" in result.stdout, f"Expected import for backend {backend}"
    else:
        assert (
            "STATUS: NOT_IMPORTED" in result.stdout
        ), f"Expected no mlflow import for backend {backend}"
