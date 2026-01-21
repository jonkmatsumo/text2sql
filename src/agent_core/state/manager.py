from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from agent_core.state.classifier import TurnType
from agent_core.state.domain import ConversationState, ExecutionContext, TurnRecord, WorkingIntent


class StateManager:
    """Manages state transitions and updates based on conversation flow."""

    def update_state_pre_retrieval(
        self,
        state: ConversationState,
        nlq: str,
        turn_type: TurnType,
        now: datetime,
    ) -> ConversationState:
        """
        Apply mutations to state BEFORE retrieval/generation begins.

        Determines if we clear context, start fresh, or refine.
        """
        # 1. Handle Hard Resets
        if turn_type == TurnType.RESET:
            self._clear_active_context(state)
            # No working intent -> "Clean Slate"
            return state

        # 2. Handle New Questions (Topic Shift)
        if turn_type == TurnType.NEW_QUESTION:
            self._clear_active_context(state)
            # Create fresh blank intent
            state.working_intent = WorkingIntent()
            return state

        # 3. Handle Refinements
        if turn_type == TurnType.REFINEMENT:
            if not state.working_intent:
                state.working_intent = WorkingIntent()
            # We don't modify the intent here; the NLU/Planner node will do that.
            # We just ensure it exists and isn't cleared.
            return state

        # 4. Handle Follow-ups (Drill-downs)
        if turn_type == TurnType.FOLLOW_UP_ON_RESULTS:
            if not state.execution_context:
                # If valid context missing, treat as new question (fallback)
                self._clear_active_context(state)
                state.working_intent = WorkingIntent()
            else:
                # Keep context; intent might be patched or derived from result
                if not state.working_intent:
                    state.working_intent = WorkingIntent()
            return state

        # 5. Handle Repair
        if turn_type == TurnType.REPAIR:
            # We explicitly DO NOT clear context, because repair is about the *previous* turn
            # But we might flag the previous turn as "error" if we had a way to look back
            # For now, just ensure intent structure exists
            if not state.working_intent:
                state.working_intent = WorkingIntent()
            return state

        return state

    def update_state_post_execution(
        self,
        state: ConversationState,
        user_nlq: str,
        sql: str,
        execution_status: str,
        result_summary: str,
        result_schema: Dict[str, Any],
        rowcount: int,
        tables_used: List[str],
        timestamp: datetime,
        error_type: Optional[str] = None,
    ) -> ConversationState:
        """Update state AFTER execution with results."""
        # 1. Update Execution Context (if success)
        if execution_status == "SUCCESS":
            state.execution_context = ExecutionContext(
                last_sql=sql,
                last_result_schema=result_schema,
                last_rowcount=rowcount,
                last_success=True,
                last_tables_used=tables_used,
                executed_at=timestamp,
            )
        else:
            # If fail, we might keep the *previous* valid context or mark current as fail
            # Typically we track the *last successful* context for follow-ups.
            # But we update the last_success flag.
            if state.execution_context:
                state.execution_context.last_success = False

        # 2. Record Turn
        turn = TurnRecord(
            turn_id=str(int(timestamp.timestamp() * 1000)),  # simple ID gen
            user_nlq=user_nlq,
            timestamp=timestamp,
            assistant_sql=sql,
            execution_status=execution_status,
            error_type=error_type,
            result_summary=result_summary,
            tables_used=tables_used,
        )
        state.add_turn(turn)
        return state

    def check_ttl(
        self, state: ConversationState, now: datetime, ttl_minutes: int = 60
    ) -> ConversationState:
        """Check if state has expired and clear context if so."""
        if not state.last_active_at:
            return state

        delta = now - state.last_active_at
        if delta > timedelta(minutes=ttl_minutes):
            self._clear_active_context(state)

        return state

    def _clear_active_context(self, state: ConversationState) -> None:
        """Clear volatile context."""
        state.working_intent = None
        state.execution_context = None
        state.retrieval_context = None
