"""Unit tests for PolicyEnforcer with schema-driven table allowlist."""

import pytest

from agent.audit import AuditEventType, get_audit_event_buffer, reset_audit_event_buffer
from agent.validation.policy_enforcer import PolicyEnforcer, clear_table_cache


class TestPolicyEnforcerTableAllowlist:
    """Tests for schema-driven table allowlist."""

    def setup_method(self):
        """Reset to static allowlist for predictable testing."""
        # Use static tables for testing (avoid DB dependency)
        PolicyEnforcer.set_allowed_tables({"customer", "fact_transaction", "dim_account"})
        reset_audit_event_buffer()

    def teardown_method(self):
        """Clear static override after each test."""
        PolicyEnforcer.set_allowed_tables(None)
        clear_table_cache()
        reset_audit_event_buffer()

    def test_allows_whitelisted_table(self):
        """Test that queries against allowed tables pass."""
        sql = "SELECT * FROM customer WHERE id = 1"
        assert PolicyEnforcer.validate_sql(sql) is True

    def test_allows_synthetic_style_tables(self):
        """Test that synthetic tables (e.g., fact_transaction) are allowed."""
        sql = (
            "SELECT * FROM fact_transaction "
            "JOIN dim_account ON fact_transaction.account_id = dim_account.id"
        )
        assert PolicyEnforcer.validate_sql(sql) is True

    def test_rejects_unknown_table(self):
        """Test that queries against non-allowed tables are rejected."""
        sql = "SELECT * FROM secret_data"
        with pytest.raises(ValueError, match="Access to table 'secret_data' is not allowed"):
            PolicyEnforcer.validate_sql(sql)
        recent = get_audit_event_buffer().list_recent(limit=1)
        assert recent[0]["event_type"] == AuditEventType.POLICY_REJECTION.value

    def test_rejects_cross_schema_access(self):
        """Test that explicit non-public schema access is rejected."""
        sql = "SELECT * FROM pg_catalog.pg_tables"
        with pytest.raises(ValueError, match="Cross-schema access not allowed"):
            PolicyEnforcer.validate_sql(sql)

    def test_allows_public_schema_explicit(self):
        """Test that explicit public schema access is allowed."""
        sql = "SELECT * FROM public.customer"
        assert PolicyEnforcer.validate_sql(sql) is True


class TestPolicyEnforcerDDLBlocking:
    """Tests for DDL/DML blocking."""

    def setup_method(self):
        """Set up test with customer table allowed."""
        PolicyEnforcer.set_allowed_tables({"customer"})

    def teardown_method(self):
        """Clean up static override."""
        PolicyEnforcer.set_allowed_tables(None)

    def test_rejects_insert(self):
        """Test that INSERT statements are rejected."""
        sql = "INSERT INTO customer VALUES (1, 'test')"
        with pytest.raises(ValueError, match="Statement type not allowed"):
            PolicyEnforcer.validate_sql(sql)

    def test_rejects_update(self):
        """Test that UPDATE statements are rejected."""
        sql = "UPDATE customer SET name = 'hacked'"
        with pytest.raises(ValueError, match="Statement type not allowed"):
            PolicyEnforcer.validate_sql(sql)

    def test_rejects_delete(self):
        """Test that DELETE statements are rejected."""
        sql = "DELETE FROM customer"
        with pytest.raises(ValueError, match="Statement type not allowed"):
            PolicyEnforcer.validate_sql(sql)

    def test_rejects_drop(self):
        """Test that DROP statements are rejected."""
        sql = "DROP TABLE customer"
        with pytest.raises(ValueError, match="Statement type not allowed"):
            PolicyEnforcer.validate_sql(sql)

    def test_rejects_create(self):
        """Test that CREATE statements are rejected."""
        sql = "CREATE TABLE evil (id INT)"
        with pytest.raises(ValueError, match="Statement type not allowed"):
            PolicyEnforcer.validate_sql(sql)


class TestPolicyEnforcerFunctionBlocking:
    """Tests for dangerous function blocking."""

    def setup_method(self):
        """Set up test with customer table allowed."""
        PolicyEnforcer.set_allowed_tables({"customer"})

    def teardown_method(self):
        """Clean up static override."""
        PolicyEnforcer.set_allowed_tables(None)

    def test_rejects_pg_read_file(self):
        """Test that pg_read_file is blocked."""
        sql = "SELECT pg_read_file('/etc/passwd')"
        with pytest.raises(ValueError, match="Function 'pg_read_file' is restricted"):
            PolicyEnforcer.validate_sql(sql)

    def test_rejects_pg_sleep(self):
        """Test that pg_sleep is blocked."""
        sql = "SELECT pg_sleep(10)"
        with pytest.raises(ValueError, match="Function 'pg_sleep' is restricted"):
            PolicyEnforcer.validate_sql(sql)

    def test_allows_safe_functions(self):
        """Test that safe functions like COUNT, SUM are allowed."""
        sql = "SELECT COUNT(*), SUM(amount) FROM customer GROUP BY id"
        # This should pass without error (customer is whitelisted)
        PolicyEnforcer.set_allowed_tables({"customer"})
        assert PolicyEnforcer.validate_sql(sql) is True


class TestPolicyEnforcerCTESupport:
    """Tests for CTE (Common Table Expression) support."""

    def setup_method(self):
        """Set up test with customer table allowed."""
        PolicyEnforcer.set_allowed_tables({"customer"})

    def teardown_method(self):
        """Clean up static override."""
        PolicyEnforcer.set_allowed_tables(None)

    def test_allows_cte_references(self):
        """Test that CTE-defined tables are allowed even if not in allowlist."""
        sql = """
            WITH my_cte AS (SELECT * FROM customer WHERE active = 1)
            SELECT * FROM my_cte
        """
        assert PolicyEnforcer.validate_sql(sql) is True

    def test_mutation_keyword_inside_comment_does_not_affect_validation(self):
        """Comments containing mutation keywords should not alter parse/validation behavior."""
        sql = """
            SELECT *
            FROM customer
            -- DROP TABLE customer
        """
        assert PolicyEnforcer.validate_sql(sql) is True

    def test_leading_block_comment_then_select_is_allowed(self):
        """Leading block comments should be stripped before SQL parsing."""
        sql = "/* synthetic comment */ SELECT * FROM customer"
        assert PolicyEnforcer.validate_sql(sql) is True


class TestPolicyEnforcerSensitiveColumns:
    """Tests for sensitive column guardrails."""

    def setup_method(self):
        """Configure allowed tables for sensitive-column tests."""
        PolicyEnforcer.set_allowed_tables({"customer"})

    def teardown_method(self):
        """Reset table overrides after sensitive-column tests."""
        PolicyEnforcer.set_allowed_tables(None)

    def test_sensitive_columns_warn_by_default(self, monkeypatch, caplog):
        """Sensitive references should warn without blocking by default."""
        monkeypatch.delenv("AGENT_BLOCK_SENSITIVE_COLUMNS", raising=False)

        with caplog.at_level("WARNING"):
            assert PolicyEnforcer.validate_sql("SELECT password FROM customer") is True

        assert any("Sensitive column reference detected" in rec.message for rec in caplog.records)

    def test_sensitive_columns_block_when_flag_enabled(self, monkeypatch):
        """Sensitive guardrail should block even when reference is in a UNION branch."""
        monkeypatch.setenv("AGENT_BLOCK_SENSITIVE_COLUMNS", "true")
        sql = "SELECT id FROM customer UNION SELECT api_key FROM customer"
        with pytest.raises(ValueError, match="Sensitive column reference detected"):
            PolicyEnforcer.validate_sql(sql)
