import pytest
from mcp_server.services.cache.sql_constraint_validator import extract_limit_from_sql

from agent.tests.schema_fixtures import PAGILA_FIXTURE, SYNTHETIC_FIXTURE


@pytest.mark.parametrize("schema_fixture", [PAGILA_FIXTURE, SYNTHETIC_FIXTURE], indirect=True)
def test_sql_limit_extraction_parity(schema_fixture):
    """Verify limit extraction works for both schemas."""
    # Synthetic table
    sql = f"SELECT * FROM {schema_fixture.valid_table} LIMIT 5"
    assert extract_limit_from_sql(sql) == 5

    # No limit
    sql = f"SELECT * FROM {schema_fixture.valid_table}"
    assert extract_limit_from_sql(sql) is None
