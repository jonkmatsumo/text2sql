"""Deterministic mocked runtime helpers for full agent pipeline tests."""

from __future__ import annotations

import inspect
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableLambda


@dataclass
class MockTool:
    """Minimal async tool wrapper that records invocations."""

    name: str
    handler: Callable[[dict[str, Any]], Any]
    calls: list[dict[str, Any]] = field(default_factory=list)

    async def ainvoke(self, payload: dict[str, Any], config: dict | None = None) -> Any:
        """Invoke tool handler with call capture."""
        del config
        self.calls.append(payload)
        result = self.handler(payload)
        if inspect.isawaitable(result):
            return await result
        return result


@dataclass
class MockDAL:
    """Deterministic DAL stub used by mocked MCP execute tool."""

    response: Any = None
    calls: list[dict[str, Any]] = field(default_factory=list)

    async def execute(self, payload: dict[str, Any]) -> Any:
        """Execute a fake SQL request and return configured response."""
        self.calls.append(payload)

        result = self.response(payload) if callable(self.response) else self.response
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, BaseException):
            raise result
        return result


@dataclass
class MockMCPClient:
    """Registry of mocked tools backed by deterministic handlers."""

    dal: MockDAL
    tool_responses: dict[str, Any] = field(default_factory=dict)

    def set_tool_response(self, tool_name: str, response: Any) -> None:
        """Override a mocked tool response for a specific test scenario."""
        self.tool_responses[tool_name] = response

    def tools(self) -> list[MockTool]:
        """Return mocked tool list expected by agent nodes."""
        return [
            MockTool("lookup_cache", lambda payload: self._dispatch("lookup_cache", payload)),
            MockTool(
                "get_semantic_subgraph",
                lambda payload: self._dispatch("get_semantic_subgraph", payload),
            ),
            MockTool(
                "resolve_ambiguity",
                lambda payload: self._dispatch("resolve_ambiguity", payload),
            ),
            MockTool(
                "recommend_examples",
                lambda payload: self._dispatch("recommend_examples", payload),
            ),
            MockTool(
                "execute_sql_query",
                lambda payload: self._dispatch("execute_sql_query", payload),
            ),
        ]

    def _dispatch(self, tool_name: str, payload: dict[str, Any]) -> Any:
        """Resolve tool output from per-tool override or default behavior."""
        if tool_name in self.tool_responses:
            configured = self.tool_responses[tool_name]
            return configured(payload) if callable(configured) else configured

        if tool_name == "lookup_cache":
            return {"value": None}
        if tool_name == "get_semantic_subgraph":
            return {
                "nodes": [
                    {"type": "Table", "name": "orders"},
                    {"type": "Column", "table": "orders", "name": "order_id"},
                ],
                "relationships": [],
            }
        if tool_name == "resolve_ambiguity":
            return {"status": "CLEAR", "resolved_bindings": {}}
        if tool_name == "recommend_examples":
            return {"examples": []}
        if tool_name == "execute_sql_query":
            return self.dal.execute(payload)
        raise ValueError(f"Unknown mocked tool '{tool_name}'.")


def install_mock_agent_runtime(
    monkeypatch: Any,
    *,
    mcp_client: MockMCPClient,
    synthesize_text: str = "Mocked synthesized response.",
    clarification_text: str = "Can you clarify your request?",
) -> None:
    """Install mocked MCP + LLM runtime across graph node modules."""

    async def _get_tools() -> list[MockTool]:
        return mcp_client.tools()

    monkeypatch.setattr("agent.tools.get_mcp_tools", _get_tools, raising=False)
    monkeypatch.setattr("agent.nodes.cache_lookup.get_mcp_tools", _get_tools, raising=False)
    monkeypatch.setattr("agent.nodes.retrieve.get_mcp_tools", _get_tools, raising=False)
    monkeypatch.setattr("agent.nodes.router.get_mcp_tools", _get_tools, raising=False)
    monkeypatch.setattr("agent.nodes.generate.get_mcp_tools", _get_tools, raising=False)
    monkeypatch.setattr("agent.nodes.execute.get_mcp_tools", _get_tools, raising=False)
    monkeypatch.setattr(
        "agent.nodes.execute.PolicyEnforcer.validate_sql",
        lambda _sql: None,
        raising=False,
    )

    async def _rewrite_passthrough(sql: str, tenant_id: Any) -> str:
        del tenant_id
        return sql

    monkeypatch.setattr(
        "agent.nodes.execute.TenantRewriter.rewrite_sql",
        _rewrite_passthrough,
        raising=False,
    )

    llm_schema_calls: dict[str, int] = {"count": 0}

    def _mock_llm_response(inputs: Any) -> AIMessage:
        prompt_text = ""
        if hasattr(inputs, "to_messages"):
            try:
                prompt_text = "\n".join(str(msg.content) for msg in inputs.to_messages())
            except Exception:
                prompt_text = str(inputs)
        elif hasattr(inputs, "messages"):
            try:
                prompt_text = "\n".join(str(msg.content) for msg in inputs.messages)
            except Exception:
                prompt_text = str(inputs)
        elif isinstance(inputs, list):
            try:
                prompt_text = "\n".join(str(getattr(msg, "content", msg)) for msg in inputs)
            except Exception:
                prompt_text = str(inputs)
        else:
            prompt_text = str(inputs)

        if "Provide a clear answer:" in prompt_text:
            return AIMessage(content=synthesize_text)
        if "An ambiguity or missing data issue was detected" in prompt_text:
            return AIMessage(content=clarification_text)
        if "You are a SQL planning expert." in prompt_text:
            return AIMessage(
                content='{"procedural_plan":["Step 1"],"clause_map":{},"schema_ingredients":[]}'
            )

        if isinstance(inputs, dict):
            if "results" in inputs:
                return AIMessage(content=synthesize_text)
            if "ambiguity_data" in inputs:
                return AIMessage(content=clarification_text)
            if "chat_history" in inputs and "question" in inputs:
                return AIMessage(content=str(inputs.get("question") or ""))
            if "schema_context" in inputs and "question" in inputs:
                llm_schema_calls["count"] += 1
                if llm_schema_calls["count"] % 2 == 1:
                    return AIMessage(
                        content=(
                            '{"procedural_plan":["Step 1"],'
                            '"clause_map":{},"schema_ingredients":[]}'
                        )
                    )
                return AIMessage(content="SELECT 1 AS value")
        return AIMessage(content="SELECT 1 AS value")

    llm = RunnableLambda(_mock_llm_response)
    monkeypatch.setattr("agent.llm_client.get_llm", lambda *args, **kwargs: llm)
    monkeypatch.setattr("agent.nodes.synthesize.get_llm", lambda *args, **kwargs: llm)


def build_app_input(
    *,
    question: str,
    tenant_id: int = 1,
    from_cache: bool = False,
    current_sql: str | None = None,
    retry_count: int = 0,
    clarify_count: int = 0,
) -> dict[str, Any]:
    """Build a deterministic baseline state payload for `app.ainvoke()`."""
    deadline_ts = time.monotonic() + 30.0
    return {
        "messages": [HumanMessage(content=question)],
        "run_id": f"run-{uuid.uuid4()}",
        "policy_snapshot": {"snapshot_id": "test-policy"},
        "schema_context": "",
        "current_sql": current_sql,
        "query_result": None,
        "error": None,
        "retry_after_seconds": None,
        "retry_count": retry_count,
        "clarify_count": clarify_count,
        "schema_refresh_count": 0,
        "active_query": None,
        "procedural_plan": None,
        "rejected_cache_context": None,
        "clause_map": None,
        "tenant_id": tenant_id,
        "from_cache": from_cache,
        "telemetry_context": None,
        "raw_user_input": question,
        "schema_snapshot_id": None,
        "pinned_schema_snapshot_id": None,
        "pending_schema_snapshot_id": None,
        "pending_schema_fingerprint": None,
        "pending_schema_version_ts": None,
        "schema_snapshot_transition": None,
        "schema_snapshot_refresh_applied": 0,
        "schema_fingerprint": None,
        "schema_version_ts": None,
        "deadline_ts": deadline_ts,
        "timeout_seconds": 30.0,
        "page_token": None,
        "page_size": None,
        "seed": 0,
        "interactive_session": True,
        "replay_mode": False,
        "replay_bundle": None,
        "generate_only": False,
        "token_budget": {"max_tokens": 50000, "consumed_tokens": 0},
        "llm_prompt_bytes_used": 0,
        "llm_budget_exceeded": False,
        "error_signatures": [],
        "decision_events": [],
        "decision_events_truncated": False,
        "decision_events_dropped": 0,
        "prefetch_kill_switch_enabled": False,
        "schema_refresh_kill_switch_enabled": False,
        "llm_retries_kill_switch_enabled": False,
    }


def unique_thread_config() -> dict[str, Any]:
    """Return unique thread config required by graph checkpointer."""
    return {"configurable": {"thread_id": f"thread-{uuid.uuid4()}"}}
