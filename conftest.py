"""Root-level pytest configuration for path management.

This conftest runs before test-specific conftests to ensure correct import path ordering.
It guarantees that mcp-server/src is found before agent/src when both are in sys.path.
"""

import sys
from pathlib import Path


def _setup_path_order():
    """Set up sys.path order to ensure mcp-server is found before agent."""
    # Get project root directory
    root_dir = Path(__file__).parent
    mcp_server_dir = root_dir / "mcp-server"
    agent_dir = root_dir / "agent"

    mcp_server_dir_str = str(mcp_server_dir.resolve())
    agent_dir_str = str(agent_dir.resolve())

    # Remove both directories from path if already present
    # This ensures we can control the exact order
    paths_to_remove = []
    for i, path in enumerate(sys.path):
        try:
            path_obj = Path(path)
            if path_obj.exists():
                resolved_path = str(path_obj.resolve())
                if resolved_path == mcp_server_dir_str or resolved_path == agent_dir_str:
                    paths_to_remove.append(i)
        except (OSError, ValueError):
            # Skip invalid paths
            continue

    # Remove paths in reverse order to maintain indices
    for i in reversed(paths_to_remove):
        sys.path.pop(i)

    # Insert mcp-server first, then agent
    # This ensures mcp-server/src is found before agent/src for all imports
    if mcp_server_dir.exists():
        sys.path.insert(0, mcp_server_dir_str)

    if agent_dir.exists():
        # Insert after mcp-server (at index 1 if mcp-server was added, otherwise at 0)
        insert_index = 1 if mcp_server_dir.exists() else 0
        sys.path.insert(insert_index, agent_dir_str)


# Set up path immediately when module is imported
# This ensures correct path order before any test collection or imports happen
_setup_path_order()


def pytest_configure(config):
    """Configure pytest - ensure path order is correct."""
    # Re-setup path order in case pytest.ini or other config changed it
    _setup_path_order()
