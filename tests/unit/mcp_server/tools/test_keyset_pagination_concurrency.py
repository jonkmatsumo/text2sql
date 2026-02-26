import json
import re
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from mcp_server.tools.execute_sql_query import handler


class _MutableDatasetConn:
    def __init__(
        self,
        rows: list[dict[str, int | float]],
        *,
        snapshot_id: str | None = None,
        transaction_id: str | None = None,
        db_role: str | None = None,
        region: str | None = None,
        node_id: str | None = None,
    ) -> None:
        self._rows = rows
        self.session_guardrail_metadata = {}
        if snapshot_id is not None:
            self.snapshot_id = snapshot_id
        if transaction_id is not None:
            self.transaction_id = transaction_id
        if db_role is not None:
            self.db_role = db_role
        if region is not None:
            self.region = region
        if node_id is not None:
            self.node_id = node_id

    async def fetch(self, query: str, *_args):
        normalized_sql = " ".join(query.split())
        limit_match = re.search(r"\bLIMIT\s+(\d+)\b", normalized_sql, flags=re.IGNORECASE)
        limit = int(limit_match.group(1)) if limit_match else len(self._rows)
        cursor_match = re.search(
            r"\bWHERE\s+id\s*>\s*([0-9]+(?:\.[0-9]+)?)\b",
            normalized_sql,
            flags=re.IGNORECASE,
        )
        cursor_value = float(cursor_match.group(1)) if cursor_match else None

        ordered_rows = sorted((dict(row) for row in self._rows), key=lambda row: float(row["id"]))
        if cursor_value is not None:
            ordered_rows = [row for row in ordered_rows if float(row["id"]) > cursor_value]
        return ordered_rows[:limit]


class _ConnCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *_exc):
        return False


@pytest.mark.asyncio
async def test_keyset_concurrency_insert_between_pages_has_no_duplicates_or_skips():
    """Concurrent inserts between page requests should not duplicate or skip ordered rows."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    shared_rows = [{"id": 1}, {"id": 2}, {"id": 4}, {"id": 5}]
    sql = "SELECT id FROM users ORDER BY id ASC"

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint",
            return_value="concurrency-fingerprint",
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        mock_get_conn.side_effect = [
            _ConnCtx(_MutableDatasetConn(shared_rows)),
            _ConnCtx(_MutableDatasetConn(shared_rows)),
        ]

        page_one_payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=2)
        page_one = json.loads(page_one_payload)
        assert "error" not in page_one
        cursor = page_one["metadata"]["next_keyset_cursor"]
        assert cursor

        # Simulate an insert that lands logically between page boundaries.
        shared_rows.append({"id": 3})

        page_two_payload = await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
            page_size=2,
        )
        page_two = json.loads(page_two_payload)
        assert "error" not in page_two

    page_one_ids = [row["id"] for row in page_one["rows"]]
    page_two_ids = [row["id"] for row in page_two["rows"]]
    assert page_one_ids == [1, 2]
    assert page_two_ids == [3, 4]
    assert set(page_one_ids).isdisjoint(set(page_two_ids))
    assert page_one_ids + page_two_ids == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_keyset_concurrency_snapshot_change_rejects_second_page_cursor():
    """Snapshot changes between pages should reject keyset cursor reuse under drift."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    shared_rows = [{"id": 1}, {"id": 2}, {"id": 4}, {"id": 5}]
    sql = "SELECT id FROM users ORDER BY id ASC"

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint",
            return_value="concurrency-fingerprint",
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        mock_get_conn.side_effect = [
            _ConnCtx(
                _MutableDatasetConn(
                    shared_rows,
                    snapshot_id="snap-1",
                    transaction_id="tx-1",
                )
            ),
            _ConnCtx(
                _MutableDatasetConn(
                    shared_rows,
                    snapshot_id="snap-2",
                    transaction_id="tx-1",
                )
            ),
        ]

        page_one_payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=2)
        page_one = json.loads(page_one_payload)
        cursor = page_one["metadata"]["next_keyset_cursor"]
        assert cursor

        # Concurrent insert occurs before second page request.
        shared_rows.append({"id": 3})

        page_two_payload = await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
            page_size=2,
        )
        page_two = json.loads(page_two_payload)

    assert page_two["error"]["category"] == "invalid_request"
    assert page_two["error"]["details_safe"]["reason_code"] == "KEYSET_SNAPSHOT_MISMATCH"


@pytest.mark.asyncio
async def test_keyset_distributed_region_drift_rejects_second_page_cursor():
    """Cross-region page routing should fail closed with stable topology mismatch reason."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    shared_rows = [{"id": 1}, {"id": 2}, {"id": 4}, {"id": 5}]
    sql = "SELECT id FROM users ORDER BY id ASC"

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint",
            return_value="concurrency-fingerprint",
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        mock_get_conn.side_effect = [
            _ConnCtx(
                _MutableDatasetConn(
                    shared_rows,
                    db_role="replica",
                    region="us-east-1",
                    node_id="node-a",
                )
            ),
            _ConnCtx(
                _MutableDatasetConn(
                    shared_rows,
                    db_role="replica",
                    region="us-west-2",
                    node_id="node-b",
                )
            ),
        ]

        page_one_payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=2)
        page_one = json.loads(page_one_payload)
        cursor = page_one["metadata"]["next_keyset_cursor"]
        assert cursor

        # Simulate writes while request is rerouted to a different region.
        shared_rows.append({"id": 3})

        page_two_payload = await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
            page_size=2,
        )
        page_two = json.loads(page_two_payload)

    assert page_two["error"]["category"] == "invalid_request"
    assert page_two["error"]["details_safe"]["reason_code"] == "KEYSET_TOPOLOGY_MISMATCH"
