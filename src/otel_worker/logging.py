import json
import logging
from datetime import datetime
from typing import Any, Dict

logger = logging.getLogger("otel_worker.events")


def log_event(event_name: str, level: int = logging.WARNING, **kwargs: Any):
    """
    Emit a structured log event.

    Args:
        event_name: Unique name of the event (e.g., 'queue_saturated').
        level: Logging level (default WARNING).
        **kwargs: Context data to include in the payload.
    """
    payload: Dict[str, Any] = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "event": event_name,
        **kwargs,
    }

    # Serialize to JSON for structured parsing
    try:
        msg = json.dumps(payload)
    except Exception:
        msg = str(payload)

    logger.log(level, msg)
