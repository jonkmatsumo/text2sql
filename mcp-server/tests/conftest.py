import pytest
from mcp_server.config.database import Database


@pytest.fixture(autouse=True)
async def init_database(request):
    """Initialize the database connection pool for integration tests only."""
    if request.node.get_closest_marker("integration") is None:
        yield
        return
    await Database.init()
    yield
    await Database.close()
