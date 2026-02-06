import pytest

from mcp_server.services.cache.sql_constraint_validator import extract_limit_from_sql
from tests._support.fixtures.schema_fixtures import SYNTHETIC_FIXTURE


@pytest.mark.parametrize("schema_fixture", [SYNTHETIC_FIXTURE], indirect=True)
def test_sql_limit_extraction_parity(schema_fixture):
    """Verify limit extraction works for both schemas."""
    # Synthetic table
    sql = f"SELECT * FROM {schema_fixture.valid_table} LIMIT 5"
    assert extract_limit_from_sql(sql) == 5

    # No limit
    sql = f"SELECT * FROM {schema_fixture.valid_table}"
    assert extract_limit_from_sql(sql) is None


def test_constraint_extraction_synthetic():
    """Verify constraint extraction works for synthetic domain."""
    from mcp_server.services.cache.constraint_extractor import extract_constraints

    c = extract_constraints("Top 10 merchants by transaction count")
    assert c.limit == 10
    assert c.entity == "merchant"
    assert c.metric == "count"


def test_constraint_extraction_financial_entities():
    """Verify other financial entities are extracted."""
    from mcp_server.services.cache.constraint_extractor import extract_constraints

    assert extract_constraints("Show accounts").entity == "account"
    assert extract_constraints("List transactions from last month").entity == "transaction"
    assert extract_constraints("Find banks").entity == "institution"
