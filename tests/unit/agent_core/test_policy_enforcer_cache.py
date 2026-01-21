"""Tests for PolicyEnforcer caching behavior."""

from agent_core.validation.policy_enforcer import PolicyEnforcer, clear_table_cache


class TestPolicyEnforcerCache:
    """Tests for PolicyEnforcer caching behavior."""

    def teardown_method(self):
        """Ensure clean state."""
        clear_table_cache()
        PolicyEnforcer.set_allowed_tables(None)

    def test_cache_clearing(self, mocker):
        """Test that clear_table_cache forces re-introspection."""
        # Mock psycopg2 to control introspection result
        mock_connect = mocker.patch("psycopg2.connect")
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
