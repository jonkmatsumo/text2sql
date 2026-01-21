"""Regression test for schema imports in mcp-server."""


def test_schema_import_identity():
    """Verify mcp_server imports the correct ColumnDef from schema."""
    # This seemingly simple test verifies that:
    # 1. 'schema' is importable (from sys.path)
    # 2. 'mcp_server' can import 'schema'
    # 3. They resolve to the same underlying class object

    from mcp_server.models.database import ColumnDef as DBColumnDef
    from schema import ColumnDef

    assert ColumnDef is DBColumnDef
