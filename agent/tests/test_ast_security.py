"""Tests for hardened AST security validation."""

from agent_core.validation.ast_validator import validate_sql


def test_sql_chaining_blocked():
    """Test that multiple statements (chaining) are blocked."""
    sql = "SELECT * FROM useers; DROP TABLE users"
    result = validate_sql(sql)
    assert not result.is_valid
    # Expect syntax error (from parse returning None) or violation
    # Current parse_sql returns None + error message "SQL chaining detected"
    # validate_sql wraps parsing error into SYNTAX_ERROR violation
    assert len(result.violations) == 1
    assert "chaining" in result.violations[0].message.lower()


def test_forbidden_root_blocked():
    """Test that forbidden root commands are blocked."""
    sql = "DROP TABLE users"
    result = validate_sql(sql)
    assert not result.is_valid
    # Should trigger both Root Policy and Forbidden Command check
    # We expect at least one violation
    assert len(result.violations) >= 1
    msg = result.violations[0].message.lower()
    assert "invalid root" in msg or "forbidden" in msg


def test_nested_injection_blocked():
    """Test that forbidden commands in subqueries are blocked."""
    # Using a syntax that parses but is destructive (e.g. DELETE in subquery)
    # Postgres supports DELETE ... RETURNING
    sql = "SELECT * FROM users WHERE id = (DELETE FROM users RETURNING id)"
    result = validate_sql(sql)

    # Depending on dialect, this might parse or fail.
    # If it parses, it should be caught by recursive check.
    # If it fails parse, it is also blocked (safe).
    if result.parsed_sql:  # Parsed successfully
        assert not result.is_valid
        # Check for forbidden command detection
        violations = [v.message.lower() for v in result.violations]
        assert any("delete" in v and "forbidden" in v for v in violations)


def test_valid_with_clause():
    """Test that CTEs (WITH clauses) are allowed."""
    sql = """
    WITH regional_sales AS (
        SELECT region, SUM(amount) as total_sales
        FROM orders
        GROUP BY region
    )
    SELECT region, total_sales
    FROM regional_sales
    WHERE total_sales > (SELECT AVG(total_sales) FROM regional_sales)
    """
    result = validate_sql(sql)
    assert result.is_valid
    assert len(result.violations) == 0


def test_valid_union():
    """Test that UNIONs are allowed (if simple)."""
    sql = "SELECT id FROM t1 UNION SELECT id FROM t2"
    result = validate_sql(sql)
    assert result.is_valid


def test_invalid_implicit_command():
    """Test that generic commands/schema ops are caught."""
    sql = "CREATE SCHEMA hacking"
    result = validate_sql(sql)
    assert not result.is_valid
    violations = [v.message.lower() for v in result.violations]
    assert any("create" in v or "root" in v for v in violations)


def test_truncate_blocked():
    """Test that TRUNCATE is blocked."""
    sql = "TRUNCATE TABLE users"
    result = validate_sql(sql)
    assert not result.is_valid
    assert any("truncate" in v.message.lower() for v in result.violations)
