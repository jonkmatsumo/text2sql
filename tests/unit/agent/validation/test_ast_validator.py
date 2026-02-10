"""Unit tests for the SQL AST validator."""

from agent.validation.ast_validator import ViolationType, validate_sql


class TestASTValidator:
    """Test suite for SQL AST validation conformance."""

    def test_validate_sql_basic_select(self):
        """Standard SELECT should be valid."""
        result = validate_sql("SELECT id, name FROM users WHERE active = true")
        assert result.is_valid
        assert not result.violations
        assert result.metadata.table_lineage == ["users"]
        assert "users" in result.metadata.table_lineage

    def test_validate_sql_complex_join(self):
        """Queries with JOINs should be valid and track complexity."""
        sql = """
            SELECT a.name, f.title
            FROM actor a
            JOIN film_actor fa ON a.actor_id = fa.actor_id
            JOIN film f ON fa.film_id = f.film_id
        """
        result = validate_sql(sql)
        assert result.is_valid
        assert result.metadata.join_complexity == 2
        assert "actor" in result.metadata.table_lineage
        assert "film" in result.metadata.table_lineage

    def test_validate_sql_cte(self):
        """CTE (WITH clause) should be valid."""
        sql = """
            WITH actor_films AS (
                SELECT actor_id, count(*) as count
                FROM film_actor
                GROUP BY actor_id
            )
            SELECT a.first_name, af.count
            FROM actor a
            JOIN actor_films af ON a.actor_id = af.actor_id
        """
        result = validate_sql(sql)
        assert result.is_valid
        assert "film_actor" in result.metadata.table_lineage
        assert "actor" in result.metadata.table_lineage

    def test_validate_sql_window_functions(self):
        """Ensure PostgreSQL window functions are valid."""
        sql = """
            SELECT title, length,
                   RANK() OVER (PARTITION BY rating ORDER BY length DESC) as rank
            FROM film
        """
        result = validate_sql(sql)
        assert result.is_valid
        assert result.metadata.has_window_function

    def test_validate_sql_lateral_join(self):
        """LATERAL joins should be valid."""
        sql = """
            SELECT f.title, s.rental_date
            FROM film f,
            LATERAL (SELECT rental_date FROM rental r WHERE r.inventory_id IN
                    (SELECT inventory_id FROM inventory i WHERE i.film_id = f.film_id)
                    ORDER BY rental_date DESC LIMIT 1) s
        """
        result = validate_sql(sql)
        assert result.is_valid
        assert result.metadata.has_subquery

    def test_validate_sql_forbidden_drop(self):
        """DROP command should be rejected."""
        result = validate_sql("DROP TABLE users")
        assert not result.is_valid
        assert any(v.violation_type == ViolationType.FORBIDDEN_COMMAND for v in result.violations)

    def test_validate_sql_forbidden_delete(self):
        """DELETE command should be rejected."""
        result = validate_sql("DELETE FROM users WHERE id = 1")
        assert not result.is_valid
        assert any(v.violation_type == ViolationType.FORBIDDEN_COMMAND for v in result.violations)

    def test_validate_sql_restricted_table(self):
        """Access to restricted tables (e.g. payroll) should be rejected."""
        result = validate_sql("SELECT * FROM payroll")
        assert not result.is_valid
        assert any(v.violation_type == ViolationType.RESTRICTED_TABLE for v in result.violations)
        assert "payroll" in result.violations[0].message

    def test_validate_sql_system_table_prefix(self):
        """Access to pg_catalog or information_schema should be rejected."""
        result = validate_sql("SELECT * FROM pg_user")
        assert not result.is_valid
        assert any(v.violation_type == ViolationType.RESTRICTED_TABLE for v in result.violations)

        result = validate_sql("SELECT * FROM information_schema.tables")
        assert not result.is_valid
        assert any(v.violation_type == ViolationType.RESTRICTED_TABLE for v in result.violations)

    def test_validate_sql_chaining_injection(self):
        """SQL statement chaining should be rejected."""
        result = validate_sql("SELECT 1; DROP TABLE users")
        assert not result.is_valid
        assert "chaining detected" in result.violations[0].message

    def test_validate_sql_syntax_error(self):
        """Malformed SQL should be caught as syntax error."""
        # Use an unmistakably malformed query
        result = validate_sql("SELECT ,,,, FROM")
        assert not result.is_valid
        assert result.violations[0].violation_type == ViolationType.SYNTAX_ERROR

    def test_validate_sql_complexity_limit(self, monkeypatch):
        """Should reject queries exceeding join complexity limit."""
        monkeypatch.setenv("AGENT_MAX_JOIN_COMPLEXITY", "1")
        sql = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id"
        result = validate_sql(sql)
        assert not result.is_valid
        assert any(v.violation_type == ViolationType.COMPLEXITY_LIMIT for v in result.violations)
        assert "contains 2 joins" in result.violations[0].message

    def test_validate_sql_union_injection_pattern(self):
        """Complex UNION with subqueries should be flagged as dangerous."""
        # Need > 2 unions to trigger the check in ast_validator.py
        sql = """
            SELECT name FROM users
            UNION ALL
            SELECT (SELECT password FROM secrets LIMIT 1)
            UNION ALL
            SELECT (SELECT token FROM api_keys LIMIT 1)
            UNION ALL
            SELECT (SELECT key FROM user_secrets LIMIT 1)
        """
        result = validate_sql(sql)
        assert not result.is_valid
        assert any(v.violation_type == ViolationType.DANGEROUS_PATTERN for v in result.violations)

    def test_validate_sql_union_branch_disallowed_table(self):
        """UNION branches should be blocked when a branch references a non-allowlisted table."""
        sql = "SELECT a FROM t1 UNION SELECT b FROM t2"
        result = validate_sql(sql, allowed_tables={"t1"})
        assert not result.is_valid
        assert any(
            v.details.get("reason") == "set_operation_disallowed_table" for v in result.violations
        )

    def test_validate_sql_allowlisted_table_passes(self):
        """Table references in allowlist should pass positive allowlist checks."""
        result = validate_sql("SELECT * FROM customers", allowed_tables={"customers"})
        assert result.is_valid

    def test_validate_sql_non_allowlisted_table_fails(self):
        """Unknown table should fail even when not in static denylist."""
        result = validate_sql("SELECT * FROM orders", allowed_tables={"customers"})
        assert not result.is_valid
        assert any(v.details.get("reason") == "table_not_allowlisted" for v in result.violations)

    def test_validate_sql_nested_cte_union_disallowed_table(self):
        """Nested CTE set-ops should also enforce allowlist branch checks."""
        sql = """
            WITH combined AS (
                SELECT a FROM t1
                UNION
                SELECT b FROM t2
            )
            SELECT * FROM combined
        """
        result = validate_sql(sql, allowed_tables={"t1"})
        assert not result.is_valid
        assert any(
            v.details.get("reason") == "set_operation_disallowed_table" for v in result.violations
        )

    def test_validate_sql_sensitive_column_warns_by_default(self, monkeypatch):
        """Sensitive columns should emit warnings without blocking when flag is off."""
        monkeypatch.delenv("AGENT_BLOCK_SENSITIVE_COLUMNS", raising=False)

        result = validate_sql("SELECT password FROM users")

        assert result.is_valid
        assert result.violations == []
        assert result.warnings
        assert "password" in result.warnings[0]

    def test_validate_sql_sensitive_column_blocks_when_flag_enabled(self, monkeypatch):
        """Sensitive column checks should apply across UNION branches in blocking mode."""
        monkeypatch.setenv("AGENT_BLOCK_SENSITIVE_COLUMNS", "true")
        sql = "SELECT id FROM users UNION SELECT api_key FROM users"

        result = validate_sql(sql)

        assert not result.is_valid
        assert any(v.violation_type == ViolationType.SENSITIVE_COLUMN for v in result.violations)
        assert any("api_key" in v.message for v in result.violations)
