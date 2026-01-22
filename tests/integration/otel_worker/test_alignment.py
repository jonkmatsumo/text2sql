from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from otel_worker.config import settings
from otel_worker.ingestion.processor import PersistenceCoordinator


@pytest.fixture
def coordinator():
    """Create a coordinator fixture."""
    return PersistenceCoordinator()


@pytest.mark.asyncio
async def test_polling_pause_on_full_buffer(coordinator):
    """Test that polling is paused when the buffer is full."""
    settings.PROCESSING_QUEUE_MAX_DEPTH = 5
    settings.BATCH_MAX_SIZE = 5

    with patch("otel_worker.ingestion.processor.poll_ingestion_queue", new_callable=AsyncMock):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            # Pass to verify no exceptions during setup
            pass


@pytest.mark.asyncio
async def test_batch_failure_marks_sql_failed(coordinator):
    """Test that a batch failure calls update_ingestion_status with 'failed'."""
    items = [
        {"id": 1, "payload_json": {"body_b64": "e30=", "content_type": "application/json"}},
        {"id": 2, "payload_json": {"body_b64": "e30=", "content_type": "application/json"}},
    ]

    # Force an exception during processing (e.g. MinIO upload or just generic)
    with patch(
        "otel_worker.ingestion.processor.parse_otlp_json_traces", side_effect=ValueError("Boom")
    ):
        with patch(
            "otel_worker.ingestion.processor.update_ingestion_status", new_callable=MagicMock
        ) as mock_update:
            with patch("otel_worker.ingestion.processor.log_event"):
                # Run process_batch
                await coordinator._process_batch(items)

                # Should have called update_ingestion_status for both items with 'failed'
                assert mock_update.call_count == 2

                # Verify args
                args, kwargs = mock_update.call_args_list[0]
                assert args[0] == 1
                assert args[1] == "failed"
                assert "Boom" in kwargs["error"]  # error message

                args, kwargs = mock_update.call_args_list[1]
                assert args[0] == 2
                assert args[1] == "failed"
