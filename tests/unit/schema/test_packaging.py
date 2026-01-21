"""Regression tests for schema package structure and imports."""

import schema


def test_schema_exports():
    """Verify schema package exports expected models."""
    assert hasattr(schema, "ColumnDef")
    assert hasattr(schema, "TableDef")
    assert hasattr(schema, "ForeignKeyDef")


def test_no_shadowing():
    """Verify we are not shadowing or being shadowed by the 'schema' PyPI package."""
    # The 'schema' PyPI package typically exports a Schema class.
    # Our package does not.
    assert not hasattr(schema, "Schema"), (
        "Found 'Schema' attribute in schema module. "
        "This suggests the 'schema' PyPI package is shadowing our local package."
    )
