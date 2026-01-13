from pathlib import Path

import pytest

# Path to streamlit_app package
STREAMLIT_APP_DIR = Path(__file__).parent.parent.parent / "streamlit_app"


def test_no_dal_imports_in_streamlit_app():
    """Verify architecture boundaries for Streamlit app.

    Streamlit app code MUST NOT import mcp_server.dal modules directly.
    It must use MCP tools via the agent interface.
    """
    violating_files = []

    if not STREAMLIT_APP_DIR.exists():
        pytest.fail(f"streamlit_app directory not found at {STREAMLIT_APP_DIR}")

    for file_path in STREAMLIT_APP_DIR.rglob("*.py"):
        # Skip __init__.py if empty or minimal
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            # Check for direct DAL imports
            if "mcp_server.dal" in content:
                # Exclude this test file itself if it were inside (it's not)
                violating_files.append(str(file_path.relative_to(STREAMLIT_APP_DIR)))

    # Assert
    assert not violating_files, (
        f"Architecture Violation: Direct DAL imports found in Streamlit app.\n"
        f"Files: {violating_files}\n"
        f"Guideline: Streamlit must access data via MCP tools, not direct DB/DAL imports."
    )
