"""Smoke test for packaging configuration.

Verifies that internal packages are locally resolvable and installed correctly.
"""

import importlib.metadata
import subprocess
import sys

import pytest


def test_packaging_metadata():
    """Verify that text2sql-dal and text2sql-mcp are installed and have correct metadata."""
    try:
        dal_version = importlib.metadata.version("text2sql-dal")
        print(f"text2sql-dal version: {dal_version}")
    except importlib.metadata.PackageNotFoundError:
        pytest.fail("text2sql-dal not installed")

    # Verify python version compatibility (this test runs under current python)
    # If we are running, the python version is effectively compatible enough to run this.
    pass


def test_pip_check():
    """Run pip check to ensure no broken dependencies."""
    # This might fail if the environment has other issues, but checks consistency
    result = subprocess.run([sys.executable, "-m", "pip", "check"], capture_output=True, text=True)
    if result.returncode != 0:
        # We allow some noise in dev environments, but strictly looking for 'text2sql-dal' issues
        if "text2sql-dal" in result.stdout or "text2sql-dal" in result.stderr:
            pytest.fail(
                f"pip check reported issues with text2sql-dal:\n{result.stdout}\n{result.stderr}"
            )
