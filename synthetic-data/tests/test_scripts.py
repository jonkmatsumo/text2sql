from text2sql_synth import schema
from text2sql_synth.config import SynthConfig
from text2sql_synth.orchestrator import generate_tables
from text2sql_synth.scripts import get_catalog


def test_catalog_tables_exist_in_schema():
    """Verify that every table referenced in the catalog exists in schema.py."""
    catalog = get_catalog()
    all_schema_tables = set(schema.EXPECTED_COLUMNS.keys())

    for script in catalog:
        for table in script["tables"]:
            assert (
                table in all_schema_tables
            ), f"Table '{table}' in script '{script['id']}' not found in schema.py"


def test_catalog_columns_exist_in_generated_data():
    """Verify generated data contains all columns expected by schema.py."""
    catalog = get_catalog()

    # Generate small dataset
    config = SynthConfig.preset("small")
    ctx, tables = generate_tables(config)

    for script in catalog:
        for table_name in script["tables"]:
            assert table_name in tables, f"Table '{table_name}' was not generated"
            df = tables[table_name]

            # Check columns against schema.py EXPECTED_COLUMNS
            expected_cols = schema.EXPECTED_COLUMNS.get(table_name, [])
            for col in expected_cols:
                assert col in df.columns, f"Column '{col}' missing from table '{table_name}'"


def test_catalog_id_uniqueness():
    """Verify that all script IDs are unique."""
    catalog = get_catalog()
    ids = [s["id"] for s in catalog]
    assert len(ids) == len(set(ids)), "Duplicate script IDs found in catalog"


def test_catalog_turn_count():
    """Verify that all scripts have at least 2 turns (multi-turn)."""
    catalog = get_catalog()
    for script in catalog:
        assert len(script["turns"]) >= 2, f"Script '{script['id']}' must have at least 2 turns"
