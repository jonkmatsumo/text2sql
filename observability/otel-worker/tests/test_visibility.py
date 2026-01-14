import json
import logging

from otel_worker.logging import log_event


def test_log_event_structure(caplog):
    """Verify that log_event emits a structured JSON warning."""
    caplog.set_level(logging.WARNING)

    log_event("my_test_event", reason="testing", value=123)

    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelno == logging.WARNING

    # Parse the message as JSON
    data = json.loads(record.message)
    assert data["event"] == "my_test_event"
    assert data["reason"] == "testing"
    assert data["value"] == 123
    assert "timestamp" in data


def test_log_event_levels(caplog):
    """Verify that logging level can be customized."""
    caplog.set_level(logging.INFO)

    log_event("info_event", level=logging.INFO, status="ok")

    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.INFO
