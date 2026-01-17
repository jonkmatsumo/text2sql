import pytest

from agent.tests.schema_fixtures import PAGILA_FIXTURE, SYNTHETIC_FIXTURE


@pytest.mark.asyncio
@pytest.mark.parametrize("schema_fixture", [PAGILA_FIXTURE, SYNTHETIC_FIXTURE], indirect=True)
async def test_schema_fixture_parity(schema_fixture):
    """Verify that fixtures provide necessary attributes for both datasets."""
    assert schema_fixture.valid_table
    assert schema_fixture.invalid_table
    assert schema_fixture.sample_query
    assert schema_fixture.valid_table in schema_fixture.sample_query
    assert schema_fixture.tables
    assert schema_fixture.valid_table in schema_fixture.tables
    assert schema_fixture.name in ["pagila", "synthetic"]
