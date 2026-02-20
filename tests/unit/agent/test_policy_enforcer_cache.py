"""Tests for PolicyEnforcer caching behavior."""

from unittest.mock import patch

from agent.validation.policy_enforcer import PolicyEnforcer, clear_table_cache


class TestPolicyEnforcerCache:
    """Tests for PolicyEnforcer caching behavior."""

    def teardown_method(self):
        """Ensure clean state."""
        clear_table_cache()
        PolicyEnforcer.set_allowed_tables(None)

    def test_cache_clearing(self):
        """Test that clear_table_cache forces re-introspection."""
        # Mock psycopg2 to control introspection result
        with patch("psycopg2.connect") as mock_connect:
            mock_cursor = mock_connect.return_value.cursor.return_value
            mock_cursor.__enter__.return_value = mock_cursor

            # First call: returns ['t1']
            mock_cursor.fetchall.return_value = [("t1",)]

            # This triggers introspection
            tables1 = PolicyEnforcer.get_allowed_tables()
            assert "t1" in tables1
            assert "t2" not in tables1
            assert mock_connect.call_count == 1

            # Second call: should use cache, even if we change mock
            mock_cursor.fetchall.return_value = [("t2",)]
            tables2 = PolicyEnforcer.get_allowed_tables()
            assert "t1" in tables2
            assert "t2" not in tables2
            assert mock_connect.call_count == 1  # No new call

            # Clear cache
            clear_table_cache()

            # Third call: should re-introspect and get new value
            tables3 = PolicyEnforcer.get_allowed_tables()
            assert "t2" in tables3
            assert "t1" not in tables3
            assert mock_connect.call_count == 2  # New call

    def test_cache_reused_within_ttl_window(self, monkeypatch):
        """Allowlist cache should be reused while TTL has not expired."""
        monkeypatch.setenv("POLICY_ALLOWED_TABLES_CACHE_TTL_SECONDS", "300")
        now = {"value": 100.0}
        monkeypatch.setattr("agent.validation.policy_enforcer.time.monotonic", lambda: now["value"])

        with patch("psycopg2.connect") as mock_connect:
            mock_cursor = mock_connect.return_value.cursor.return_value
            mock_cursor.__enter__.return_value = mock_cursor

            mock_cursor.fetchall.return_value = [("t1",)]
            tables1 = PolicyEnforcer.get_allowed_tables()
            assert tables1 == {"t1"}
            assert mock_connect.call_count == 1

            now["value"] = 399.9
            mock_cursor.fetchall.return_value = [("t2",)]
            tables2 = PolicyEnforcer.get_allowed_tables()
            assert tables2 == {"t1"}
            assert mock_connect.call_count == 1

    def test_cache_refreshes_after_ttl_expiry(self, monkeypatch):
        """Allowlist cache should be refreshed once TTL elapses."""
        monkeypatch.setenv("POLICY_ALLOWED_TABLES_CACHE_TTL_SECONDS", "300")
        now = {"value": 100.0}
        monkeypatch.setattr("agent.validation.policy_enforcer.time.monotonic", lambda: now["value"])

        with patch("psycopg2.connect") as mock_connect:
            mock_cursor = mock_connect.return_value.cursor.return_value
            mock_cursor.__enter__.return_value = mock_cursor

            mock_cursor.fetchall.return_value = [("t1",)]
            tables1 = PolicyEnforcer.get_allowed_tables()
            assert tables1 == {"t1"}
            assert mock_connect.call_count == 1

            now["value"] = 400.1
            mock_cursor.fetchall.return_value = [("t2",)]
            tables2 = PolicyEnforcer.get_allowed_tables()
            assert tables2 == {"t2"}
            assert mock_connect.call_count == 2
