"""Import smoke tests for ingestion package."""

import ingestion
from ingestion import patterns


def test_base_import():
    """Test base package imports correctly."""
    assert hasattr(ingestion, "__version__")
    assert ingestion.__version__ == "0.1.0"


def test_submodule_imports():
    """Test placeholder submodules import cleanly."""
    assert patterns is not None
