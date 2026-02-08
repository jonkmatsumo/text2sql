import pytest

from agent.validation.policy_enforcer import PolicyEnforcer


@pytest.fixture
def mock_allowed_tables():
    """Mock the allowed tables to avoid DB introspection."""
    tables = {"t1", "t2", "users", "orders"}
    PolicyEnforcer.set_allowed_tables(tables)
    yield
    PolicyEnforcer.set_allowed_tables(None)


def test_select_allowed(mock_allowed_tables):
    """Test that SELECT statements are allowed."""
    sql = "SELECT * FROM t1"
    assert PolicyEnforcer.validate_sql(sql) is True


def test_union_allowed(mock_allowed_tables):
    """Test that UNION statements are allowed."""
    sql = "SELECT * FROM t1 UNION SELECT * FROM t2"
    assert PolicyEnforcer.validate_sql(sql) is True


def test_intersect_allowed(mock_allowed_tables):
    """Test that INTERSECT statements are allowed."""
    sql = "SELECT * FROM t1 INTERSECT SELECT * FROM t2"
    assert PolicyEnforcer.validate_sql(sql) is True


def test_except_allowed(mock_allowed_tables):
    """Test that EXCEPT statements are allowed."""
    sql = "SELECT * FROM t1 EXCEPT SELECT * FROM t2"
    assert PolicyEnforcer.validate_sql(sql) is True


def test_cte_allowed(mock_allowed_tables):
    """Test that CTEs are allowed."""
    sql = "WITH cte AS (SELECT 1) SELECT * FROM cte"
    assert PolicyEnforcer.validate_sql(sql) is True


def test_insert_blocked(mock_allowed_tables):
    """Test that INSERT statements are blocked."""
    sql = "INSERT INTO t1 VALUES (1)"
    with pytest.raises(ValueError, match="Statement type not allowed"):
        PolicyEnforcer.validate_sql(sql)


def test_update_blocked(mock_allowed_tables):
    """Test that UPDATE statements are blocked."""
    sql = "UPDATE t1 SET col=1"
    with pytest.raises(ValueError, match="Statement type not allowed"):
        PolicyEnforcer.validate_sql(sql)


def test_drop_blocked(mock_allowed_tables):
    """Test that DROP statements are blocked."""
    sql = "DROP TABLE t1"
    with pytest.raises(ValueError, match="Statement type not allowed"):
        PolicyEnforcer.validate_sql(sql)


def test_unknown_table_blocked(mock_allowed_tables):
    """Test that access to unknown tables is blocked."""
    sql = "SELECT * FROM unknown_table"
    with pytest.raises(ValueError, match="Access to table 'unknown_table' is not allowed"):
        PolicyEnforcer.validate_sql(sql)


def test_system_function_blocked(mock_allowed_tables):
    """Test that system functions are blocked."""
    sql = "SELECT pg_read_file('etc/passwd')"
    with pytest.raises(ValueError, match="Function 'pg_read_file' is restricted"):
        PolicyEnforcer.validate_sql(sql)


def test_cross_schema_blocked(mock_allowed_tables):
    """Test that cross-schema access is blocked."""
    sql = "SELECT * FROM information_schema.tables"
    with pytest.raises(ValueError, match="Cross-schema access not allowed"):
        PolicyEnforcer.validate_sql(sql)
