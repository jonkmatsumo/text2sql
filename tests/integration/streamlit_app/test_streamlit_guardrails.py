"""Guardrail tests to prevent reintroduction of streamlit wrapper directory."""

from pathlib import Path


def test_no_streamlit_wrapper_directory():
    """Ensure the streamlit/ wrapper directory does not exist.

    All Streamlit code, tests, and packaging should live in streamlit_app/.
    A streamlit/ directory would cause confusion and maintenance overhead.
    """
    repo_root = Path(__file__).resolve().parents[3]
    wrapper_path = repo_root / "streamlit"

    # Allow only if it doesn't exist OR if it only contains a README redirect
    if wrapper_path.exists():
        contents = list(wrapper_path.iterdir())
        allowed_files = {"README.md"}
        actual_files = {f.name for f in contents}

        assert actual_files <= allowed_files, (
            f"Unexpected files in streamlit/ wrapper directory: {actual_files - allowed_files}. "
            "All Streamlit code, tests, and packaging should live in streamlit_app/."
        )


def test_streamlit_app_is_complete_boundary():
    """Ensure streamlit_app/ contains all required packaging and test files."""
    repo_root = Path(__file__).resolve().parents[3]
    streamlit_app = repo_root / "streamlit-app"

    required_files = [
        "pyproject.toml",
        "Dockerfile",
    ]

    for required in required_files:
        path = streamlit_app / required
        assert path.exists(), (
            f"Missing required file/directory in streamlit-app/: {required}. "
            "streamlit-app/ should be the single Streamlit packaging boundary."
        )
