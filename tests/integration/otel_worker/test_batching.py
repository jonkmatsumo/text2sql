import asyncio
import base64
import json
from unittest.mock import patch

import pytest

from otel_worker.config import settings
from otel_worker.ingestion.processor import PersistenceCoordinator


@pytest.fixture
def mock_item():
    """Create a mock ingestion queue item."""
    trace_id = "0123456789abcdef"
    # Minimal OTLP JSON-like payload
    payload = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "test-service"}}
                    ]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": base64.b64encode(bytes.fromhex(trace_id)).decode(),
                                "spanId": base64.b64encode(b"span1234").decode(),
                                "name": "test-span",
                                "startTimeUnixNano": "1700000000000000000",
                                "endTimeUnixNano": "1700000000100000000",
                                "status": {"code": 1},
                            }
                        ]
                    }
                ],
            }
        ]
    }
    body_json = json.dumps(payload)
    body_b64 = base64.b64encode(body_json.encode()).decode()

    return {
        "id": 1,
        "received_at": "2023-01-01T00:00:00",
        "payload_json": {"content_type": "application/json", "body_b64": body_b64},
    }


@pytest.mark.asyncio
async def test_size_based_flush(mock_item):
    """Test that reaching BATCH_MAX_SIZE triggers a flush."""
    settings.BATCH_MAX_SIZE = 5
    settings.BATCH_FLUSH_INTERVAL_MS = 10000  # Large interval

    coordinator = PersistenceCoordinator(initial_delay=0)

    # Mock items return 2 then 3 then nothing
    items_stream = [
        [dict(mock_item, id=i) for i in range(2)],
        [dict(mock_item, id=i) for i in range(2, 5)],
        [],
    ]

    with (
        patch("otel_worker.ingestion.processor.poll_ingestion_queue") as mock_poll,
        patch("otel_worker.ingestion.processor.save_traces_batch") as mock_save,
        patch(
            "otel_worker.ingestion.processor.upload_trace_blob", return_value="http://minio/blob"
        ),
        patch("otel_worker.ingestion.processor.update_ingestion_status") as mock_update,
    ):
        mock_poll.side_effect = items_stream + [[]] * 100

        # Start worker
        await coordinator.start(num_workers=1)

        # Poll 1: 2 items in buffer
        # Poll 2: 3 more items, total 5 -> FLUSH

        max_wait = 2.0
        start_time = asyncio.get_event_loop().time()
        while (
            mock_update.call_count < 5 and (asyncio.get_event_loop().time() - start_time) < max_wait
        ):
            await asyncio.sleep(0.1)

        await coordinator.stop()

        assert mock_save.call_count >= 1
        assert mock_update.call_count == 5


@pytest.mark.asyncio
async def test_time_based_flush(mock_item):
    """Test that waiting BATCH_FLUSH_INTERVAL_MS triggers a flush."""
    settings.BATCH_MAX_SIZE = 100
    settings.BATCH_FLUSH_INTERVAL_MS = 200  # Small interval

    coordinator = PersistenceCoordinator(initial_delay=0)

    # 1 item initially, then nothing
    items_stream = [[mock_item]]

    with (
        patch("otel_worker.ingestion.processor.poll_ingestion_queue") as mock_poll,
        patch("otel_worker.ingestion.processor.save_traces_batch") as mock_save,
        patch(
            "otel_worker.ingestion.processor.upload_trace_blob", return_value="http://minio/blob"
        ),
        patch("otel_worker.ingestion.processor.update_ingestion_status") as mock_update,
    ):
        mock_poll.side_effect = items_stream + [[]] * 100

        await coordinator.start(num_workers=1)

        # Wait for interval to pass
        max_wait = 2.0
        start_time = asyncio.get_event_loop().time()
        while (
            mock_update.call_count < 1 and (asyncio.get_event_loop().time() - start_time) < max_wait
        ):
            await asyncio.sleep(0.1)

        await coordinator.stop()

        assert mock_save.call_count >= 1
        assert mock_update.call_count == 1


@pytest.mark.asyncio
async def test_shutdown_flush(mock_item):
    """Test that stopping the coordinator flushes the remaining buffer."""
    settings.BATCH_MAX_SIZE = 100
    settings.BATCH_FLUSH_INTERVAL_MS = 10000  # Large interval

    coordinator = PersistenceCoordinator(initial_delay=0)

    # 1 item initially
    items_stream = [[mock_item]]

    with (
        patch("otel_worker.ingestion.processor.poll_ingestion_queue") as mock_poll,
        patch("otel_worker.ingestion.processor.save_traces_batch") as mock_save,
        patch(
            "otel_worker.ingestion.processor.upload_trace_blob", return_value="http://minio/blob"
        ),
        patch("otel_worker.ingestion.processor.update_ingestion_status") as mock_update,
    ):
        mock_poll.side_effect = items_stream + [[]] * 100

        await coordinator.start(num_workers=1)

        # Give it a tiny bit to poll and fill buffer
        await asyncio.sleep(0.2)

        # Stop immediately - should trigger flush
        await coordinator.stop()

        assert mock_save.call_count == 1
        assert mock_update.call_count == 1
