import sys
from pathlib import Path

# ==============================================================================
# CRITICAL INFRASTRUCTURE FILE - DO NOT DELETE
# ==============================================================================
# This file is essential for Pytest configuration and CI/CD pipeline stability.
# It ensures that 'mcp-server/src' and 'agent/src' are correctly added to sys.path
# before test collection begins. Removing this file will cause ModuleNotFoundErrors
# in CI environments.
# ==============================================================================

# Calculate root directory
ROOT_DIR = Path(__file__).parent.absolute()

# Add source directories to sys.path
# We prepend to ensure these local packages take precedence over installed ones
# This fixes "ModuleNotFoundError" in CI where editable installs might behave differently
# or when relying on 'import-mode=importlib' without explicit path setup.

mcp_server_src = ROOT_DIR / "mcp-server" / "src"
agent_src = ROOT_DIR / "agent" / "src"

if str(mcp_server_src) not in sys.path:
    sys.path.insert(0, str(mcp_server_src))
    print(f"conftest.py: Added {mcp_server_src} to sys.path")

if str(agent_src) not in sys.path:
    sys.path.insert(0, str(agent_src))
    print(f"conftest.py: Added {agent_src} to sys.path")

# Add mcp-server/tests to sys.path to allow importing fixtures
mcp_server_tests = ROOT_DIR / "mcp-server" / "tests"
if str(mcp_server_tests) not in sys.path:
    sys.path.insert(0, str(mcp_server_tests))
    print(f"conftest.py: Added {mcp_server_tests} to sys.path")
