from common.policy.sql_policy import (
    classify_blocked_table_reference,
    get_blocked_tables,
    load_policy_snapshot,
)


class TestPolicySnapshot:
    """Tests for policy snapshot pinning."""

    def test_snapshot_pinning(self, monkeypatch):
        """Verify that snapshot retains policy state despite env changes."""
        # 1. Setup initial policy
        monkeypatch.setenv("SQL_BLOCKED_TABLES", "table1")

        # 2. Take snapshot
        snapshot = load_policy_snapshot()
        assert "table1" in snapshot["blocked_tables"]

        # 3. Change environment (simulating drift)
        monkeypatch.setenv("SQL_BLOCKED_TABLES", "table2")

        # 4. Verify live check sees new policy
        assert "table2" in get_blocked_tables()
        assert classify_blocked_table_reference(table_name="table2") == "restricted_table"
        assert classify_blocked_table_reference(table_name="table1") is None

        # 5. Verify snapshot check sees old policy
        assert (
            classify_blocked_table_reference(table_name="table1", snapshot=snapshot)
            == "restricted_table"
        )
        assert classify_blocked_table_reference(table_name="table2", snapshot=snapshot) is None
