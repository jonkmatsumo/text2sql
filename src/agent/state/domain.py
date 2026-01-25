import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class WorkingIntent:
    """Represents the current mutable plan/intent for the conversation."""

    metric: Optional[str] = None
    dimensions: List[str] = field(default_factory=list)
    filters: List[Dict[str, Any]] = field(default_factory=list)
    time_window: Optional[Dict[str, Any]] = None
    granularity: Optional[str] = None
    sort: Optional[Dict[str, str]] = None
    limit: Optional[int] = None
    output_format: str = "table"
    notes: Optional[str] = None


@dataclass
class ExecutionContext:
    """Context from the last successful execution."""

    last_sql: Optional[str] = None
    last_result_schema: Optional[Dict[str, Any]] = None
    last_rowcount: Optional[int] = None
    last_success: bool = False
    last_tables_used: List[str] = field(default_factory=list)
    last_result_preview: Optional[List[Dict[str, Any]]] = None
    executed_at: Optional[datetime] = None


@dataclass
class RetrievalContext:
    """Context from retrieval steps."""

    last_schema_candidates: List[str] = field(default_factory=list)  # list of table names or ids
    last_fewshot_example_ids: List[str] = field(default_factory=list)


@dataclass
class TurnRecord:
    """Immutable record of a single conversation turn."""

    turn_id: str
    user_nlq: str
    timestamp: datetime
    extracted_entities: List[str] = field(default_factory=list)
    assistant_sql: Optional[str] = None
    execution_status: str = "PENDING"  # SUCCESS, FAILURE, SKIPPED
    error_type: Optional[str] = None
    result_summary: Optional[str] = None
    tables_used: List[str] = field(default_factory=list)
    thumb_feedback: Optional[str] = None  # UP, DOWN


@dataclass
class ConversationState:
    """Core state object tracking conversation history and execution context."""

    conversation_id: str
    schema_snapshot_id: str

    # Metadata
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_active_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    db_target: Optional[str] = None
    model_version: Optional[str] = None
    prompt_version: Optional[str] = None

    # Versioning
    state_version: int = 1
    state_updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # History (Bounded)
    turns: List[TurnRecord] = field(default_factory=list)
    MAX_TURNS: int = 15

    # Contexts
    working_intent: Optional[WorkingIntent] = None
    execution_context: Optional[ExecutionContext] = None
    retrieval_context: Optional[RetrievalContext] = None

    def add_turn(self, turn: TurnRecord) -> None:
        """Add a turn to history, maintaining the size limit."""
        self.turns.append(turn)
        if len(self.turns) > self.MAX_TURNS:
            self.turns = self.turns[-self.MAX_TURNS :]

        self.last_active_at = turn.timestamp
        self._bump_version()

    def _bump_version(self) -> None:
        """Increment version and update timestamp."""
        self.state_version += 1
        self.state_updated_at = datetime.now(timezone.utc)

    def to_json(self) -> str:
        """Serialize state to a JSON string."""
        return json.dumps(asdict(self), default=self._json_serializer)

    @classmethod
    def from_json(cls, json_str: str) -> "ConversationState":
        """Deserialize state from a JSON string."""
        data = json.loads(json_str)

        # Helper to strict parse ISO timestamps
        def parse_ts(ts):
            if not ts:
                return None
            return datetime.fromisoformat(ts) if isinstance(ts, str) else ts

        # Reconstruct Turns
        turns = []
        for t in data.get("turns", []):
            if "timestamp" in t and isinstance(t["timestamp"], str):
                t["timestamp"] = parse_ts(t["timestamp"])
            turns.append(TurnRecord(**t))

        # Reconstruct Sub-objects
        working_intent = (
            WorkingIntent(**data["working_intent"]) if data.get("working_intent") else None
        )

        exec_ctx_data = data.get("execution_context")
        execution_context = None
        if exec_ctx_data:
            if exec_ctx_data.get("executed_at"):
                exec_ctx_data["executed_at"] = parse_ts(exec_ctx_data["executed_at"])
            execution_context = ExecutionContext(**exec_ctx_data)

        retrieval_context = (
            RetrievalContext(**data["retrieval_context"]) if data.get("retrieval_context") else None
        )

        # Reconstruct State
        state = cls(
            conversation_id=data["conversation_id"],
            schema_snapshot_id=data["schema_snapshot_id"],
            started_at=parse_ts(data["started_at"]),
            last_active_at=parse_ts(data["last_active_at"]),
            db_target=data.get("db_target"),
            model_version=data.get("model_version"),
            prompt_version=data.get("prompt_version"),
            state_version=data.get("state_version", 1),
            state_updated_at=parse_ts(data["state_updated_at"]),
            turns=turns,
            working_intent=working_intent,
            execution_context=execution_context,
            retrieval_context=retrieval_context,
        )
        return state

    @staticmethod
    def _json_serializer(obj):
        """Serialize datetime objects to ISO format."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
