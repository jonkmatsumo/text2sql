import pytest


def pytest_collection_modifyitems(items):
    """Mark all collected tests in this directory as integration tests."""
    for item in items:
        item.add_marker(pytest.mark.integration)
