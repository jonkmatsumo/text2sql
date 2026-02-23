"""Extended coverage for dangerous PostgreSQL function blocking."""

import pytest

from agent.validation.policy_enforcer import PolicyEnforcer
from mcp_server.tools.execute_sql_query import _validate_sql_ast

BLOCKED_FUNCTION_CASES = [
    # Remote execution / bypass
    ("dblink", "SELECT dblink('host=localhost', 'SELECT 1')"),
    ("dblink_exec", "SELECT dblink_exec('host=localhost', 'SELECT 1')"),
    ("dblink", "SELECT DBLINK('host=localhost', 'SELECT 1')"),
    ("dblink", "SELECT public.dblink('host=localhost', 'SELECT 1')"),
    # Server filesystem access
    ("pg_read_binary_file", "SELECT pg_read_binary_file('/tmp/x')"),
    ("pg_read_file", "SELECT pg_read_file('/tmp/x')"),
    ("pg_ls_dir", "SELECT pg_ls_dir('.')"),
    ("lo_import", "SELECT lo_import('/tmp/infile')"),
    ("lo_export", "SELECT lo_export(123, '/tmp/outfile')"),
    ("pg_ls_tmpdir", "SELECT pg_ls_tmpdir()"),
    ("pg_ls_waldir", "SELECT pg_ls_waldir()"),
    # DoS / Locking
    ("pg_advisory_lock", "SELECT pg_advisory_lock(42)"),
    ("pg_advisory_xact_lock", "SELECT pg_advisory_xact_lock(42)"),
    ("pg_sleep", "SELECT pg_sleep(1)"),
    ("pg_sleep", "SELECT pg_catalog.pg_sleep(1)"),
    ("pg_sleep", "SELECT PG_SLEEP(1)"),
    # Session disruption
    ("pg_cancel_backend", "SELECT pg_cancel_backend(12345)"),
    ("pg_terminate_backend", "SELECT pg_terminate_backend(12345)"),
    ("pg_reload_conf", "SELECT pg_reload_conf()"),
    ("pg_rotate_logfile", "SELECT pg_rotate_logfile()"),
    # Arbitrary subquery execution wrappers
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


BYPASS_FORMAT_CASES = [
    ("dblink", "SELECT /*x*/ dblink ( 'host=localhost', 'SELECT 1' )"),
    ("dblink", "SELECT dblink\n('host=localhost', 'SELECT 1')"),
    ("dblink", "SELECT 1 WHERE exists (SELECT dblink('host=localhost', 'SELECT 1'))"),
    ("pg_sleep", "SELECT 1 + (SELECT pg_sleep(1))"),
    ("pg_sleep", 'SELECT "pg_sleep"(1)'),  # Quoted identifier
]


@pytest.mark.parametrize(("function_name", "sql"), BYPASS_FORMAT_CASES)
def test_policy_enforcer_blocks_bypass_formats(function_name: str, sql: str):
    """Agent-side policy should not be bypassed by comments, whitespace, or context."""
    with pytest.raises(ValueError, match="restricted"):
        PolicyEnforcer.validate_sql(sql)


@pytest.mark.parametrize(("function_name", "sql"), BYPASS_FORMAT_CASES)
def test_mcp_ast_validation_blocks_bypass_formats(function_name: str, sql: str):
    """MCP-side validation should not be bypassed by comments, whitespace, or context."""
    error = _validate_sql_ast(sql, "postgres")
    assert isinstance(error, str)
    assert "Forbidden function" in error
