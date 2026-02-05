import pytest


def pytest_collection_modifyitems(items):
    """Mark collected tests in this directory as integration tests."""
    for item in items:
        if "tests/integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
