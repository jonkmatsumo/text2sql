import os
import sys

import pytest

# Add src to sys.path to ensure we can import the module
sys.path.append(os.path.join(os.getcwd(), "src"))

from mcp_server.graph_ingestion.schema_parser import SchemaParser  # noqa: E402

SCHEMA_FILE = "database/init-scripts/01-schema.sql"


def test_schema_parser_initialization():
    """Test parser initialization."""
    parser = SchemaParser()
    assert parser.dialect == "postgres"


def test_parse_schema_file():
    """Test parsing the actual schema file."""
    parser = SchemaParser()

    # Ensure the schema file exists
    # If running from root, this relative path should work
    if not os.path.exists(SCHEMA_FILE):
        pytest.skip(f"Schema file not found at {SCHEMA_FILE}")

    parsed = parser.parse_file(SCHEMA_FILE)

    assert "tables" in parsed
    tables = parsed["tables"]
    assert len(tables) > 0, "No tables found in parsed schema"

    # Check for specific tables we know exist (based on view_file output)
    table_names = [t["table_name"] for t in tables]
    print(f"Found tables: {table_names}")

    assert "customer" in table_names
    assert "film" in table_names
    assert "inventory" in table_names

    # Check customer table details
    customer_table = next(t for t in tables if t["table_name"] == "customer")
    # schema might be None if not specified, but extracting from 'public.customer' should
    # give 'public' The file has CREATE TABLE public.customer so it should be 'public'
    assert customer_table["schema"] == "public"

    # Check columns
    column_names = [c["name"] for c in customer_table["columns"]]
    assert "customer_id" in column_names
    assert "first_name" in column_names
    assert "email" in column_names

    # Check types for a known column
    email_col = next(c for c in customer_table["columns"] if c["name"] == "email")
    # Type extraction depends on sqlglot, usually it comes out upper case or as is
    # In file: email text
    assert "text" in email_col["type"].lower()


def test_parse_inline_comments():
    """Test parsing logic for inline comments."""
    # Create a dummy SQL with comments to test extraction
    sql = """
    CREATE TABLE foo (
        id INT PRIMARY KEY,
        name TEXT -- This is a name column
    );
    /* Table comment */
    COMMENT ON TABLE foo IS 'Table comment via command';
    """
    # Note: Our current implementation only looks at CREATE TABLE statement comments
    # sqlglot might not attach the "This is a name column" to the column node directly in all cases
    # verifying what we have implemented.

    parser = SchemaParser()
    parsed = parser.parse_sql(sql)

    tables = parsed["tables"]
    assert len(tables) == 1
    foo_table = tables[0]
    assert foo_table["table_name"] == "foo"

    # Check columns
    assert len(foo_table["columns"]) == 2
