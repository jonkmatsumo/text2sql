"""Unit tests for PolicyEnforcer with schema-driven table allowlist."""

import pytest

from agent.validation.policy_enforcer import PolicyEnforcer, clear_table_cache


class TestPolicyEnforcerTableAllowlist:
    """Tests for schema-driven table allowlist."""

    def setup_method(self):
        """Reset to static allowlist for predictable testing."""
        # Use static tables for testing (avoid DB dependency)
        PolicyEnforcer.set_allowed_tables({"customer", "fact_transaction", "dim_account"})

    def teardown_method(self):
        """Clear static override after each test."""
        PolicyEnforcer.set_allowed_tables(None)
        clear_table_cache()

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


@pytest.mark.dataset_pagila
class TestPolicyEnforcerPagilaRegression:
    """Regression tests ensuring Pagila tables still work when configured."""

    def setup_method(self):
        """Set up test with Pagila tables."""
        # Simulate Pagila schema
        PolicyEnforcer.set_allowed_tables(
            {
                "customer",
                "rental",
                "payment",
                "staff",
                "inventory",
                "film",
                "actor",
                "address",
                "city",
                "country",
                "category",
                "language",
                "film_actor",
                "film_category",
                "store",
            }
        )

    def teardown_method(self):
        """Clean up static override."""
        PolicyEnforcer.set_allowed_tables(None)

    def test_pagila_film_query(self):
        """Test typical Pagila film query."""
        sql = """
            SELECT f.title, c.name as category
            FROM film f
            JOIN film_category fc ON f.film_id = fc.film_id
            JOIN category c ON fc.category_id = c.category_id
        """
        assert PolicyEnforcer.validate_sql(sql) is True

    def test_pagila_rental_query(self):
        """Test typical Pagila rental query."""
        sql = """
            SELECT c.first_name, r.rental_date, p.amount
            FROM customer c
            JOIN rental r ON c.customer_id = r.customer_id
            JOIN payment p ON r.rental_id = p.rental_id
        """
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
