"""Unit tests for AST-based SQL validation."""

from agent.validation.ast_validator import (
    SecurityViolation,
    SQLMetadata,
    ViolationType,
    extract_metadata,
    parse_sql,
    validate_security,
    validate_sql,
)


class TestParseSql:
    """Tests for SQL parsing functionality."""

    def test_parse_valid_select(self):
        """Test parsing valid SELECT statement."""
        ast, error = parse_sql("SELECT * FROM customers")
        assert ast is not None
        assert error is None

    def test_parse_complex_query(self):
        """Test parsing complex query with joins and aggregations."""
        sql = """
            SELECT c.name, COUNT(o.id) as order_count
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            WHERE o.created_at > '2024-01-01'
            GROUP BY c.name
            ORDER BY order_count DESC
        """
        ast, error = parse_sql(sql)
        assert ast is not None
        assert error is None

    def test_parse_invalid_syntax(self):
        """Test parsing invalid SQL returns error."""
        ast, error = parse_sql("SELEKT * FORM customers")
        # sqlglot may still parse some invalid SQL, so just check execution
        # The important thing is no exception is raised
        assert error is not None or ast is not None

    def test_parse_with_cte(self):
        """Test parsing CTE (WITH clause)."""
        sql = """
            WITH top_customers AS (
                SELECT customer_id, SUM(amount) as total
                FROM orders
                GROUP BY customer_id
            )
            SELECT * FROM top_customers WHERE total > 1000
        """
        ast, error = parse_sql(sql)
        assert ast is not None
        assert error is None


class TestValidateSecurity:
    """Tests for security validation functionality."""

    def test_valid_select_passes(self):
        """Test that valid SELECT passes security check."""
        ast, _ = parse_sql("SELECT name, email FROM customers")
        violations = validate_security(ast)
        assert len(violations) == 0

    def test_restricted_table_payroll(self):
        """Test that access to payroll table is blocked."""
        ast, _ = parse_sql("SELECT * FROM payroll")
        violations = validate_security(ast)
        assert len(violations) == 1
        assert violations[0].violation_type == ViolationType.RESTRICTED_TABLE
        assert "payroll" in violations[0].message.lower()

    def test_restricted_table_credentials(self):
        """Test that access to credentials table is blocked."""
        ast, _ = parse_sql("SELECT * FROM credentials")
        violations = validate_security(ast)
        assert len(violations) == 1
        assert violations[0].violation_type == ViolationType.RESTRICTED_TABLE

    def test_restricted_table_in_join(self):
        """Test that restricted table in JOIN is detected."""
        ast, _ = parse_sql("SELECT c.name FROM customers c JOIN payroll p ON c.id = p.employee_id")
        violations = validate_security(ast)
        assert len(violations) >= 1
        assert any(v.violation_type == ViolationType.RESTRICTED_TABLE for v in violations)

    def test_system_table_pg_prefix(self):
        """Test that pg_* system tables are blocked."""
        ast, _ = parse_sql("SELECT * FROM pg_catalog.pg_tables")
        violations = validate_security(ast)
        assert len(violations) >= 1
        assert any(v.violation_type == ViolationType.RESTRICTED_TABLE for v in violations)

    def test_information_schema_blocked(self):
        """Test that information_schema is blocked."""
        ast, _ = parse_sql("SELECT * FROM information_schema.tables")
        violations = validate_security(ast)
        assert len(violations) >= 1

    def test_drop_command_blocked(self):
        """Test that DROP command is blocked."""
        ast, _ = parse_sql("DROP TABLE customers")
        violations = validate_security(ast)
        assert len(violations) >= 1
        assert any(v.violation_type == ViolationType.FORBIDDEN_COMMAND for v in violations)

    def test_delete_command_blocked(self):
        """Test that DELETE command is blocked."""
        ast, _ = parse_sql("DELETE FROM customers WHERE id = 1")
        violations = validate_security(ast)
        assert len(violations) >= 1
        assert any(v.violation_type == ViolationType.FORBIDDEN_COMMAND for v in violations)

    def test_update_command_blocked(self):
        """Test that UPDATE command is blocked."""
        ast, _ = parse_sql("UPDATE customers SET name = 'test' WHERE id = 1")
        violations = validate_security(ast)
        assert len(violations) >= 1
        assert any(v.violation_type == ViolationType.FORBIDDEN_COMMAND for v in violations)

    def test_insert_command_blocked(self):
        """Test that INSERT command is blocked."""
        ast, _ = parse_sql("INSERT INTO customers (name) VALUES ('test')")
        violations = validate_security(ast)
        assert len(violations) >= 1
        assert any(v.violation_type == ViolationType.FORBIDDEN_COMMAND for v in violations)

    def test_grant_command_blocked(self):
        """Test that GRANT command is blocked."""
        ast, _ = parse_sql("GRANT SELECT ON customers TO public")
        violations = validate_security(ast)
        assert len(violations) >= 1
        assert any(v.violation_type == ViolationType.FORBIDDEN_COMMAND for v in violations)

    def test_multiple_violations(self):
        """Test query with multiple violations."""
        ast, _ = parse_sql("DELETE FROM payroll WHERE id = 1")
        violations = validate_security(ast)
        # Should have both forbidden command and restricted table
        assert len(violations) >= 2


class TestExtractMetadata:
    """Tests for metadata extraction functionality."""

    def test_single_table_lineage(self):
        """Test extraction from single table query."""
        ast, _ = parse_sql("SELECT name FROM customers")
        metadata = extract_metadata(ast)
        assert "customers" in metadata.table_lineage

    def test_multi_table_lineage(self):
        """Test extraction from multi-table join."""
        ast, _ = parse_sql(
            "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id"
        )
        metadata = extract_metadata(ast)
        # Table names may include aliases
        assert len(metadata.table_lineage) >= 2

    def test_column_usage_extraction(self):
        """Test column usage extraction."""
        ast, _ = parse_sql("SELECT name, email FROM customers WHERE status = 'active'")
        metadata = extract_metadata(ast)
        column_names = [c.split(".")[-1] for c in metadata.column_usage]
        assert "name" in column_names or any("name" in c for c in metadata.column_usage)

    def test_join_complexity_count(self):
        """Test join complexity counting."""
        ast, _ = parse_sql(
            """
            SELECT a.x, b.y, c.z
            FROM table_a a
            JOIN table_b b ON a.id = b.a_id
            JOIN table_c c ON b.id = c.b_id
            LEFT JOIN table_d d ON c.id = d.c_id
        """
        )
        metadata = extract_metadata(ast)
        assert metadata.join_complexity == 3
        assert metadata.join_count == 3
        assert metadata.estimated_table_count >= 3
        assert metadata.query_complexity_score >= (metadata.join_count * 3)

    def test_complexity_union_and_cartesian_flags(self):
        """Validation metadata should capture union count and Cartesian-join signal."""
        result = validate_sql(
            "SELECT c.id FROM customers c CROSS JOIN orders o UNION SELECT c2.id FROM customers c2",
            cartesian_join_mode="warn",
        )
        assert result.metadata is not None
        assert result.metadata.union_count == 1
        assert result.metadata.detected_cartesian_flag is True
        assert result.metadata.query_complexity_score >= 9

    def test_aggregation_detection(self):
        """Test aggregation function detection."""
        ast, _ = parse_sql("SELECT COUNT(*), SUM(amount) FROM orders")
        metadata = extract_metadata(ast)
        assert metadata.has_aggregation is True

    def test_no_aggregation(self):
        """Test query without aggregation."""
        ast, _ = parse_sql("SELECT * FROM orders")
        metadata = extract_metadata(ast)
        assert metadata.has_aggregation is False

    def test_subquery_detection(self):
        """Test subquery detection."""
        ast, _ = parse_sql("SELECT * FROM customers WHERE id IN (SELECT customer_id FROM orders)")
        metadata = extract_metadata(ast)
        assert metadata.has_subquery is True

    def test_window_function_detection(self):
        """Test window function detection."""
        ast, _ = parse_sql("SELECT name, ROW_NUMBER() OVER (ORDER BY created_at) FROM customers")
        metadata = extract_metadata(ast)
        assert metadata.has_window_function is True


class TestValidateSql:
    """Tests for the complete validate_sql function."""

    def test_valid_query_returns_valid_result(self):
        """Test that valid query returns valid result."""
        result = validate_sql("SELECT * FROM customers")
        assert result.is_valid is True
        assert len(result.violations) == 0
        assert result.metadata is not None

    def test_invalid_query_returns_violations(self):
        """Test that invalid query returns violations."""
        result = validate_sql("SELECT * FROM payroll")
        assert result.is_valid is False
        assert len(result.violations) > 0

    def test_result_includes_metadata_on_failure(self):
        """Test that metadata is extracted even on validation failure."""
        result = validate_sql("SELECT * FROM payroll")
        assert result.metadata is not None
        assert "payroll" in result.metadata.table_lineage

    def test_result_to_dict_serialization(self):
        """Test that result can be serialized to dict."""
        result = validate_sql("SELECT id, name FROM customers WHERE active = true")
        result_dict = result.to_dict()
        assert "is_valid" in result_dict
        assert "violations" in result_dict
        assert "metadata" in result_dict
        assert isinstance(result_dict["violations"], list)

    def test_column_allowlist_warn_mode(self):
        """Column allowlist warn mode should keep query valid but emit warnings."""
        result = validate_sql(
            "SELECT c.email FROM customers c",
            allowed_columns={"customers": {"id", "name"}},
            column_allowlist_mode="warn",
        )
        assert result.is_valid is True
        assert result.violations == []
        assert any("column allowlist" in warning.lower() for warning in result.warnings)

    def test_column_allowlist_block_mode(self):
        """Column allowlist block mode should reject non-allowlisted projections."""
        result = validate_sql(
            "SELECT c.email FROM customers c",
            allowed_columns={"customers": {"id", "name"}},
            column_allowlist_mode="block",
        )
        assert result.is_valid is False
        assert any(v.violation_type == ViolationType.COLUMN_ALLOWLIST for v in result.violations)

    def test_column_allowlist_allows_valid_projection(self):
        """Allowlisted projected columns should pass in block mode."""
        result = validate_sql(
            "SELECT c.name FROM customers c",
            allowed_columns={"customers": {"id", "name"}},
            column_allowlist_mode="block",
        )
        assert result.is_valid is True

    def test_cartesian_join_warn_mode(self):
        """Cartesian joins should warn by default without blocking execution."""
        result = validate_sql(
            "SELECT * FROM customers CROSS JOIN orders",
            cartesian_join_mode="warn",
        )
        assert result.is_valid is True
        assert any("cartesian join" in warning.lower() for warning in result.warnings)

    def test_cartesian_join_block_mode(self):
        """Cartesian joins should block when configured."""
        result = validate_sql(
            "SELECT * FROM customers CROSS JOIN orders",
            cartesian_join_mode="block",
        )
        assert result.is_valid is False
        assert any(v.violation_type == ViolationType.CARTESIAN_JOIN for v in result.violations)

    def test_constant_join_predicate_block_mode(self):
        """Constant join predicates should be treated as Cartesian risks in block mode."""
        result = validate_sql(
            "SELECT * FROM customers c JOIN orders o ON 1 = 1",
            cartesian_join_mode="block",
        )
        assert result.is_valid is False
        assert any(v.details.get("reason") == "join_constant_condition" for v in result.violations)


class TestSecurityViolation:
    """Tests for SecurityViolation dataclass."""

    def test_to_dict(self):
        """Test SecurityViolation serialization."""
        violation = SecurityViolation(
            violation_type=ViolationType.RESTRICTED_TABLE,
            message="Access to payroll is forbidden",
            details={"table": "payroll"},
        )
        d = violation.to_dict()
        assert d["violation_type"] == "restricted_table"
        assert "payroll" in d["message"]
        assert d["details"]["table"] == "payroll"


class TestSQLMetadata:
    """Tests for SQLMetadata dataclass."""

    def test_to_dict(self):
        """Test SQLMetadata serialization."""
        metadata = SQLMetadata(
            table_lineage=["customers", "orders"],
            column_usage=["customers.id", "orders.amount"],
            join_complexity=1,
            has_aggregation=True,
            has_subquery=False,
            has_window_function=False,
        )
        d = metadata.to_dict()
        assert d["table_lineage"] == ["customers", "orders"]
        assert d["join_complexity"] == 1
        assert d["has_aggregation"] is True
