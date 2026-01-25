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

ROOT_DIR = Path(__file__).parent.absolute()
src_dir = ROOT_DIR / "src"

if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))
    print(f"conftest.py: Added {src_dir} to sys.path")


def pytest_collection_modifyitems(config, items):
    """Skip pagila-marked tests unless RUN_PAGILA_TESTS=1."""
    if os.getenv("RUN_PAGILA_TESTS", "0") == "1":
        # Run all tests including pagila tests
        return

    skip_pagila = pytest.mark.skip(
        reason="Skipping pagila dataset test (set RUN_PAGILA_TESTS=1 to run)"
    )
    for item in items:
        if "dataset_pagila" in item.keywords:
            item.add_marker(skip_pagila)
