from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass
class ConversationState:
    """Core state object tracking conversation history and execution context."""

    conversation_id: str
    schema_snapshot_id: str
    turns: List[Any] = field(default_factory=list)
    working_intent: Optional[Any] = None
