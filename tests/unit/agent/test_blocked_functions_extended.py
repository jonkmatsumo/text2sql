"""Extended coverage for dangerous PostgreSQL function blocking."""

import pytest

from agent.validation.policy_enforcer import PolicyEnforcer
from mcp_server.tools.execute_sql_query import _validate_sql_ast

BLOCKED_FUNCTION_CASES = [
    ("dblink", "SELECT dblink('host=localhost', 'SELECT 1')"),
    ("dblink_exec", "SELECT dblink_exec('host=localhost', 'SELECT 1')"),
    ("pg_read_binary_file", "SELECT pg_read_binary_file('/tmp/x')"),
    ("lo_import", "SELECT lo_import('/tmp/infile')"),
    ("lo_export", "SELECT lo_export(123, '/tmp/outfile')"),
    ("pg_advisory_lock", "SELECT pg_advisory_lock(42)"),
    ("pg_advisory_xact_lock", "SELECT pg_advisory_xact_lock(42)"),
    ("pg_cancel_backend", "SELECT pg_cancel_backend(12345)"),
    ("pg_terminate_backend", "SELECT pg_terminate_backend(12345)"),
    ("query_to_xml", "SELECT query_to_xml('SELECT 1', true, true, '')"),
    ("query_to_json", "SELECT query_to_json('SELECT 1')"),
]


@pytest.mark.parametrize(("function_name", "sql"), BLOCKED_FUNCTION_CASES)
def test_policy_enforcer_blocks_extended_functions(function_name: str, sql: str):
    """Agent-side SQL policy should reject extended dangerous function set."""
    with pytest.raises(ValueError, match="restricted") as exc_info:
        PolicyEnforcer.validate_sql(sql)

    assert function_name in str(exc_info.value).lower()


@pytest.mark.parametrize(("function_name", "sql"), BLOCKED_FUNCTION_CASES)
def test_mcp_ast_validation_blocks_extended_functions(function_name: str, sql: str):
    """MCP-side AST validation should reject extended dangerous function set."""
    error = _validate_sql_ast(sql, "postgres")

    assert isinstance(error, str)
    assert "Forbidden function" in error
    assert function_name in error.lower()
