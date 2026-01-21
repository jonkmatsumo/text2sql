import uuid
from datetime import datetime, timezone


def sample_interaction_row():
    """Return a sample sample interaction row dictionary."""
    return {
        "id": str(uuid.uuid4()),
        "conversation_id": str(uuid.uuid4()),
        "created_at": datetime.now(timezone.utc),
        "schema_snapshot_id": "snap-v1",
        "user_nlq_text": "How many movies are rated PG?",
        "generated_sql": "SELECT count(*) FROM films WHERE rating = 'PG'",
        "response_payload": '{"text": "There are 200 movies"}',
        "execution_status": "SUCCESS",
        "error_type": None,
        "model_version": "gpt-4o",
        "prompt_version": "v1.2",
        "tables_used": ["films"],
        "trace_id": str(uuid.uuid4()),
    }


def sample_feedback_payload():
    """Return a sample feedback payload."""
    return {
        "interaction_id": str(uuid.uuid4()),
        "thumb": "DOWN",
        "comment": "The count seems wrong, should be higher.",
        "feedback_source": "user",
    }
