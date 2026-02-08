import json

from common.models.tool_envelopes import ExecuteSQLQueryResponseEnvelope
from mcp_server.tools.execute_sql_query import _construct_error_response


def test_execute_sql_error_contract():
    """Verify that server error responses match the agent's Pydantic model."""
    error_json = _construct_error_response(
        message="Test Error",
        category="test_category",
        provider="postgres",
        is_retryable=True,
        retry_after_seconds=5.0,
    )

    # Parse with Pydantic model
    envelope = ExecuteSQLQueryResponseEnvelope.model_validate_json(error_json)

    assert envelope.error is not None
    assert envelope.error.message == "Test Error"
    assert envelope.error.category == "test_category"
    assert envelope.error.provider == "postgres"
    assert envelope.error.is_retryable is True
    assert envelope.error.retry_after_seconds == 5.0


def test_execute_sql_success_contract():
    """Verify that a manually constructed success response matches the contract."""
    # We can't easily call handler() without a DB, but we can verify the envelope construction logic
    # if we extract it or simulate it. For now, we'll verify the model structure itself
    # ensures compatibility with a mocked payload that represents what the server produces.

    server_payload = {
        "schema_version": "1.0",
        "rows": [{"id": 1, "name": "test"}],
        "columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "text"}],
        "metadata": {
            "rows_returned": 1,
            "is_truncated": False,
            "row_limit": 1000,
            "partial_reason": None,
            "cap_detected": False,
            "capability_supported": True,
        },
    }

    json_str = json.dumps(server_payload)
    envelope = ExecuteSQLQueryResponseEnvelope.model_validate_json(json_str)

    assert len(envelope.rows) == 1
    assert envelope.rows[0]["id"] == 1
    assert envelope.metadata.rows_returned == 1
    assert envelope.metadata.is_truncated is False
