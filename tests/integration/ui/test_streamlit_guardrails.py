"""Guardrail tests to prevent reintroduction of streamlit wrapper directory."""

from pathlib import Path


def test_no_streamlit_wrapper_directory():
    """Ensure the streamlit/ wrapper directory does not exist.

    All Streamlit code, tests, and packaging should live in src/ui/.
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
            "All Streamlit code, tests, and packaging should live in src/ui/."
        )


def test_packaging_files_moved_to_correct_locations():
    """Ensure packaging files are moved from streamlit-app/ to central locations.

    We have consolidated packaging to pyproject/ and config/docker/.
    This test verifies that the old files are gone and new ones exist.
    """
    repo_root = Path(__file__).resolve().parents[3]
    ui_dir = repo_root / "streamlit-app"

    # 1. Assert legacy files are GONE (streamlit-app/ dir may not exist)
    legacy_files = ["pyproject.toml", "Dockerfile"]
    for legacy in legacy_files:
        path = ui_dir / legacy
        assert not path.exists(), (
            f"Legacy file found: {path}. "
            "This should have been moved during the packaging refactor."
        )

    # 2. Assert new files EXIST
    new_pyproject = repo_root / "pyproject/streamlit-app/pyproject.toml"
    assert new_pyproject.exists(), "New pyproject.toml not found in pyproject/streamlit-app/"

    new_dockerfile = repo_root / "config/docker/streamlit-app.Dockerfile"
    assert new_dockerfile.exists(), "New Dockerfile not found in config/docker/"
