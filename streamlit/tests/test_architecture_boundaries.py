from pathlib import Path

# Path to streamlit_app package
STREAMLIT_APP_DIR = Path(__file__).parent.parent.parent / "streamlit_app"


def test_no_dal_imports_in_streamlit_app():
    """Verify architecture boundaries for Streamlit app.

    Streamlit app code MUST NOT import mcp_server.dal modules directly.
    It must use MCP tools via the agent interface.
    """
    violating_files = []

    for file_path in STREAMLIT_APP_DIR.rglob("*.py"):
        # Skip __init__.py if empty or minimal
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            # Check for direct DAL or server-side Service imports
            # We allow some shared components if they were explicitly designed as such,
            # but maintenance and dal are definitely out.
            violations = [
                "mcp_server.dal",
                "mcp_server.services.ops",
                "mcp_server.services.canonicalization.pattern_reload_service",
                "mcp_server.config.database",
            ]
            for violation in violations:
                if violation in content:
                    violating_files.append(
                        f"{file_path.relative_to(STREAMLIT_APP_DIR)} (found '{violation}')"
                    )
                    break

    # Assert
    assert not violating_files, (
        f"Architecture Violation: Server-side logic imported in Streamlit app.\n"
        f"Files: {violating_files}\n"
        f"Guideline: Streamlit must access data and operations via MCP tools."
    )
