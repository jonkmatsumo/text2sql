import pytest
from mcp_server.config.database import Database


@pytest.fixture(autouse=True)
async def init_database():
    """Initialize the database connection pool for all tests."""
    try:
        await Database.init()
    except Exception:
        # Fallback or allow failure if tests mock it
        pass
    yield
    await Database.close()
