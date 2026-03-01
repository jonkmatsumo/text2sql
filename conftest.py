import os
import sys
from pathlib import Path

import pytest

# ==============================================================================
# CRITICAL INFRASTRUCTURE FILE - DO NOT DELETE
# ==============================================================================
# This file is essential for Pytest configuration and CI/CD pipeline stability.
# It ensures that 'src' is on sys.path before test collection so all packages
# (agent, mcp, ui, ingestion, etc.) can be imported.
# ==============================================================================

# Fail fast if Python version is unsupported (PEP 604 unions require 3.10+)
if sys.version_info < (3, 10):
    print(
        f"ERROR: This project requires Python 3.10+ (found {sys.version.split()[0]}).",
        file=sys.stderr,
    )
    print("Please upgrade your local environment or use 'uv run pytest'.", file=sys.stderr)
    sys.exit(1)


ROOT_DIR = Path(__file__).parent.absolute()
src_dir = ROOT_DIR / "src"

if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))
    print(f"conftest.py: Added {src_dir} to sys.path")


@pytest.fixture(autouse=True)
def _set_pagination_signing_secret(monkeypatch):
    """Provide an explicit signing secret for all pagination tests.

    Production requires PAGINATION_CURSOR_SIGNING_SECRET to be set (fail-closed).
    Tests that verify signing behavior should override this via monkeypatch.
    """
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", "test-pagination-secret")


def pytest_collection_modifyitems(config, items):
    """Skipping integration tests unless RUN_INTEGRATION_TESTS=1."""
    run_integration = os.getenv("RUN_INTEGRATION_TESTS", "0") == "1"

    skip_integration = pytest.mark.skip(
        reason="Skipping integration tests (set RUN_INTEGRATION_TESTS=1 to run)"
    )
    for item in items:
        is_integration_path = f"{os.sep}tests{os.sep}integration{os.sep}" in str(item.fspath)
        if (is_integration_path or item.get_closest_marker("integration")) and not run_integration:
            item.add_marker(skip_integration)
