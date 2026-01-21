import os

import pytest

from dal.database import Database
from tests._support.fixtures.schema_fixtures import PAGILA_FIXTURE


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "requires_db: requires live database services")


def pytest_runtest_setup(item):
    """Skip tests marked requires_db unless enabled."""
    if "requires_db" in item.keywords:
        if not os.getenv("RUN_DB_TESTS"):
            pytest.skip("requires DB; run with RUN_DB_TESTS=1")


@pytest.fixture(autouse=True)
async def init_database(request):
    """Initialize the database connection pool for integration tests only."""
    if request.node.get_closest_marker("integration") is None:
        yield
        return
    await Database.init()
    yield
    await Database.close()


@pytest.fixture(scope="session")
def dataset_mode():
    """Return the current dataset mode from env (default: synthetic)."""
    from common.config.dataset import get_dataset_mode

    return get_dataset_mode()


@pytest.fixture(params=[PAGILA_FIXTURE], ids=lambda f: f.name)
def schema_fixture(request):
    """Fixture providing dataset-specific schema details."""
    return request.param
