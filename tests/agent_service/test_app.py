import pytest
from fastapi.testclient import TestClient

from agent_service import app as agent_app
from common.tenancy.limits import reset_agent_run_tenant_limiter


@pytest.fixture(autouse=True)
def _reset_agent_run_limiter():
    """Reset limiter singleton to avoid cross-test token/counter leakage."""
    reset_agent_run_tenant_limiter()
    yield
    reset_agent_run_tenant_limiter()


def test_run_agent_success(monkeypatch):
    """Return a normalized success payload from the agent service."""

    async def fake_run_agent_with_tracing(
        question,
        tenant_id,
        thread_id,
        timeout_seconds=None,
        deadline_ts=None,
        page_token=None,
        page_size=None,
        interactive_session=False,
        **kwargs,
    ):
        return {
            "current_sql": "select 1",
            "query_result": [{"one": 1}],
            "messages": [type("Msg", (), {"content": "ok"})()],
            "error": None,
            "from_cache": False,
            "interaction_id": "interaction-1",
            "viz_spec": {"chartType": "bar"},
            "viz_reason": None,
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)
    monkeypatch.setattr(agent_app.telemetry, "get_current_trace_id", lambda: "a" * 32)

    client = TestClient(agent_app.app)
    resp = client.post(
        "/agent/run",
        json={"question": "hi", "tenant_id": 1, "thread_id": "thread-1"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["sql"] == "select 1"
    assert body["result"] == [{"one": 1}]
    assert body["response"] == "ok"
    assert body["error"] is None
    assert body["from_cache"] is False
    assert body["interaction_id"] == "interaction-1"
    assert body["trace_id"] == "a" * 32
    assert body["viz_spec"] == {"chartType": "bar"}


def test_run_agent_error(monkeypatch):
    """Return an error payload when the agent raises."""

    async def fake_run_agent_with_tracing(
        question,
        tenant_id,
        thread_id,
        timeout_seconds=None,
        deadline_ts=None,
        page_token=None,
        page_size=None,
        interactive_session=False,
        **kwargs,
    ):
        raise RuntimeError("boom")

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)

    client = TestClient(agent_app.app)
    resp = client.post("/agent/run", json={"question": "hi", "tenant_id": 1})

    assert resp.status_code == 200
    body = resp.json()
    assert body["error"] == "boom"
    assert body["trace_id"] is None


def test_run_agent_requires_tenant_id():
    """Request validation should reject missing tenant_id."""
    client = TestClient(agent_app.app)
    resp = client.post("/agent/run", json={"question": "hi"})
    assert resp.status_code == 422

    body = resp.json()
    assert body["detail"][0]["loc"][-1] == "tenant_id"


def test_entrypoint_sets_deadline_ts(monkeypatch):
    """Ensure deadline_ts and timeout_seconds are passed through."""
    captured = {}

    async def fake_run_agent_with_tracing(
        question,
        tenant_id,
        thread_id,
        timeout_seconds=None,
        deadline_ts=None,
        page_token=None,
        page_size=None,
        interactive_session=False,
        **kwargs,
    ):
        captured["timeout_seconds"] = timeout_seconds
        captured["deadline_ts"] = deadline_ts
        captured["interactive_session"] = interactive_session
        captured["replay_mode"] = kwargs.get("replay_mode")
        return {"messages": [], "error": None}

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)

    client = TestClient(agent_app.app)
    resp = client.post("/agent/run", json={"question": "hi", "tenant_id": 1})

    assert resp.status_code == 200
    assert captured["timeout_seconds"] == 30.0
    assert captured["deadline_ts"] is not None
    assert captured["interactive_session"] is True
    assert captured["replay_mode"] is False


def test_entrypoint_passes_replay_mode_flag(monkeypatch):
    """Explicit replay_mode flag should be propagated to graph execution."""
    captured = {}

    async def fake_run_agent_with_tracing(
        question,
        tenant_id,
        thread_id,
        timeout_seconds=None,
        deadline_ts=None,
        page_token=None,
        page_size=None,
        interactive_session=False,
        **kwargs,
    ):
        _ = (
            question,
            tenant_id,
            thread_id,
            timeout_seconds,
            deadline_ts,
            page_token,
            page_size,
            interactive_session,
        )
        captured["replay_mode"] = kwargs.get("replay_mode")
        return {"messages": [], "error": None}

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)

    client = TestClient(agent_app.app)
    resp = client.post("/agent/run", json={"question": "hi", "tenant_id": 1, "replay_mode": True})

    assert resp.status_code == 200
    assert captured["replay_mode"] is True


def test_run_agent_record_mode_returns_replay_bundle(monkeypatch):
    """Record mode should attach deterministic replay artifacts."""

    async def fake_run_agent_with_tracing(
        question,
        tenant_id,
        thread_id,
        timeout_seconds=None,
        deadline_ts=None,
        page_token=None,
        page_size=None,
        interactive_session=False,
        **kwargs,
    ):
        _ = question, tenant_id, thread_id, timeout_seconds, deadline_ts, page_token, page_size
        return {
            "current_sql": "select 1",
            "query_result": [{"one": 1}],
            "messages": [type("Msg", (), {"content": "ok"})()],
            "error": None,
            "schema_snapshot_id": "snap-1",
            "result_completeness": {"rows_returned": 1},
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)
    monkeypatch.setenv("AGENT_REPLAY_MODE", "record")

    client = TestClient(agent_app.app)
    resp = client.post(
        "/agent/run",
        json={"question": "postgresql://u:p@localhost/db", "tenant_id": 1},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["replay_bundle"] is not None
    assert body["replay_bundle_json"] is not None
    assert body["replay_metadata"]["mode"] == "record"
    assert body["replay_metadata"]["execution_mode"] == "live"
    assert "<password>" in body["replay_bundle"]["prompts"]["user"]


def test_run_agent_replay_mode_re_runs_graph_with_bundle(monkeypatch):
    """Replay mode should re-run the graph using the captured bundle for tool outputs."""
    captured = {}

    async def fake_run_agent_with_tracing(
        question,
        tenant_id,
        thread_id,
        replay_bundle=None,
        **kwargs,
    ):
        captured["replay_bundle"] = replay_bundle
        return {
            "current_sql": "select 1",
            "query_result": [{"one": 1}],
            "messages": [type("Msg", (), {"content": "captured response"})()],
            "error": None,
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)
    monkeypatch.setenv("AGENT_REPLAY_MODE", "replay")

    replay_bundle = {
        "version": "1.0",
        "captured_at": "2026-01-01T00:00:00+00:00",
        "model": {"provider": "openai", "model_id": "gpt-4o"},
        "seed": 7,
        "prompts": {"user": "show me revenue", "assistant": "ok"},
        "schema_context": {"schema_snapshot_id": "snap-1", "fingerprint": "snap-1"},
        "flags": {"AGENT_REPLAY_MODE": "replay"},
        "tool_io": [],
        "outcome": {
            "sql": "select 1",
            "result": [{"one": 1}],
            "response": "captured response",
            "error": None,
            "error_category": None,
            "retry_summary": None,
            "result_completeness": {"rows_returned": 1},
        },
    }

    client = TestClient(agent_app.app)
    resp = client.post(
        "/agent/run",
        json={
            "question": "ignored in captured replay",
            "tenant_id": 1,
            "replay_bundle": replay_bundle,
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["sql"] == "select 1"
    assert body["result"] == [{"one": 1}]
    assert body["response"] == "captured response"
    assert body["replay_metadata"]["mode"] == "replay"
    assert body["replay_metadata"]["execution_mode"] == "replay"
    assert captured["replay_bundle"] is not None
    assert captured["replay_bundle"]["version"] == "1.0"


def test_run_agent_replay_mode_validates_bundle(monkeypatch):
    """Replay mode should return validation error for malformed bundles."""
    monkeypatch.setenv("AGENT_REPLAY_MODE", "replay")

    client = TestClient(agent_app.app)
    resp = client.post(
        "/agent/run",
        json={"question": "q", "tenant_id": 1, "replay_bundle": {"version": "1.0"}},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["error"] is not None
    assert "Invalid replay bundle" in body["error"]


def test_run_agent_replay_integrity_check_passes_when_snapshots_match(monkeypatch):
    """Replay integrity check should pass when captured and current markers align."""
    monkeypatch.setenv("AGENT_REPLAY_MODE", "replay")

    async def fake_run_agent_with_tracing(*args, **kwargs):  # noqa: ARG001
        return {
            "current_sql": "select 1",
            "query_result": [{"one": 1}],
            "messages": [type("Msg", (), {"content": "captured response"})()],
            "error": None,
            "schema_snapshot_id": "snap-1",
            "policy_snapshot": {"snapshot_id": "policy-1"},
            "run_decision_summary": {"decision_summary_hash": "hash-1"},
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)

    replay_bundle = {
        "version": "1.0",
        "captured_at": "2026-01-01T00:00:00+00:00",
        "model": {"provider": "openai", "model_id": "gpt-4o"},
        "seed": 7,
        "prompts": {"user": "show me revenue", "assistant": "ok"},
        "schema_context": {"schema_snapshot_id": "snap-1", "fingerprint": "snap-1"},
        "flags": {"AGENT_REPLAY_MODE": "replay"},
        "tool_io": [],
        "outcome": {"sql": "select 1", "result": [{"one": 1}], "response": "captured response"},
        "integrity": {
            "schema_snapshot_id": "snap-1",
            "policy_snapshot_id": "policy-1",
            "decision_summary_hash": "hash-1",
        },
    }

    client = TestClient(agent_app.app)
    resp = client.post(
        "/agent/run",
        json={
            "question": "ignored",
            "tenant_id": 1,
            "replay_bundle": replay_bundle,
            "replay_integrity_check": True,
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["error"] is None
    assert body["replay_metadata"]["integrity_check"] == "passed"


def test_run_agent_replay_integrity_check_fails_on_schema_mismatch(monkeypatch):
    """Replay integrity check should fail with typed mismatch on schema divergence."""
    monkeypatch.setenv("AGENT_REPLAY_MODE", "replay")

    async def fake_run_agent_with_tracing(*args, **kwargs):  # noqa: ARG001
        return {
            "current_sql": "select 1",
            "query_result": [{"one": 1}],
            "messages": [type("Msg", (), {"content": "captured response"})()],
            "error": None,
            "schema_snapshot_id": "snap-live",
            "policy_snapshot": {"snapshot_id": "policy-1"},
            "run_decision_summary": {"decision_summary_hash": "hash-1"},
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)

    replay_bundle = {
        "version": "1.0",
        "captured_at": "2026-01-01T00:00:00+00:00",
        "model": {"provider": "openai", "model_id": "gpt-4o"},
        "prompts": {"user": "show me revenue", "assistant": "ok"},
        "schema_context": {"schema_snapshot_id": "snap-1", "fingerprint": "snap-1"},
        "flags": {"AGENT_REPLAY_MODE": "replay"},
        "tool_io": [],
        "outcome": {"sql": "select 1", "result": [], "response": "captured response"},
        "integrity": {
            "schema_snapshot_id": "snap-1",
            "policy_snapshot_id": "policy-1",
            "decision_summary_hash": "hash-1",
        },
    }

    client = TestClient(agent_app.app)
    resp = client.post(
        "/agent/run",
        json={
            "question": "ignored",
            "tenant_id": 1,
            "replay_bundle": replay_bundle,
            "replay_integrity_check": True,
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["error"] == "Replay integrity check failed."
    assert body["error_metadata"]["code"] == "REPLAY_MISMATCH"


def test_run_agent_replay_integrity_check_fails_on_policy_mismatch(monkeypatch):
    """Replay integrity check should fail when policy snapshot IDs diverge."""
    monkeypatch.setenv("AGENT_REPLAY_MODE", "replay")

    async def fake_run_agent_with_tracing(*args, **kwargs):  # noqa: ARG001
        return {
            "current_sql": "select 1",
            "query_result": [{"one": 1}],
            "messages": [type("Msg", (), {"content": "captured response"})()],
            "error": None,
            "schema_snapshot_id": "snap-1",
            "policy_snapshot": {"snapshot_id": "policy-live"},
            "run_decision_summary": {"decision_summary_hash": "hash-1"},
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)

    replay_bundle = {
        "version": "1.0",
        "captured_at": "2026-01-01T00:00:00+00:00",
        "model": {"provider": "openai", "model_id": "gpt-4o"},
        "prompts": {"user": "show me revenue", "assistant": "ok"},
        "schema_context": {"schema_snapshot_id": "snap-1", "fingerprint": "snap-1"},
        "flags": {"AGENT_REPLAY_MODE": "replay"},
        "tool_io": [],
        "outcome": {"sql": "select 1", "result": [], "response": "captured response"},
        "integrity": {
            "schema_snapshot_id": "snap-1",
            "policy_snapshot_id": "policy-1",
            "decision_summary_hash": "hash-1",
        },
    }

    client = TestClient(agent_app.app)
    resp = client.post(
        "/agent/run",
        json={
            "question": "ignored",
            "tenant_id": 1,
            "replay_bundle": replay_bundle,
            "replay_integrity_check": True,
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["error"] == "Replay integrity check failed."
    assert body["error_metadata"]["code"] == "REPLAY_MISMATCH"


def test_run_agent_debug_decision_summary_flag(monkeypatch):
    """Decision summaries are only returned when debug mode is explicitly enabled."""

    async def fake_run_agent_with_tracing(
        question,
        tenant_id,
        thread_id,
        timeout_seconds=None,
        deadline_ts=None,
        page_token=None,
        page_size=None,
        interactive_session=False,
        **kwargs,
    ):
        _ = (
            question,
            tenant_id,
            thread_id,
            timeout_seconds,
            deadline_ts,
            page_token,
            page_size,
            interactive_session,
            kwargs,
        )
        return {
            "messages": [type("Msg", (), {"content": "ok"})()],
            "error": None,
            "decision_summary": {"selected_tables": ["orders"]},
            "retry_correction_summary": {"final_stopping_reason": "success"},
            "validation_report": {
                "failed_rules": [],
                "warnings": [],
                "affected_tables": ["orders"],
            },
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)
    monkeypatch.setenv("AGENT_DEBUG_DECISION_SUMMARY", "true")

    client = TestClient(agent_app.app)
    resp = client.post("/agent/run", json={"question": "hi", "tenant_id": 1})

    assert resp.status_code == 200
    body = resp.json()
    assert body["decision_summary"] == {"selected_tables": ["orders"]}
    assert body["retry_correction_summary"] == {"final_stopping_reason": "success"}
    assert body["validation_report"] == {
        "failed_rules": [],
        "warnings": [],
        "affected_tables": ["orders"],
    }


def test_agent_service_startup_fails_on_invalid_runtime_configuration(monkeypatch):
    """Service startup should fail fast when runtime configuration is invalid."""
    monkeypatch.setenv("AGENT_COLUMN_ALLOWLIST_MODE", "invalid-mode")

    with pytest.raises(RuntimeError):
        with TestClient(agent_app.app):
            pass


def test_agent_diagnostics_endpoint(monkeypatch):
    """Diagnostics endpoint should return non-sensitive operator configuration."""
    monkeypatch.setenv("QUERY_TARGET_BACKEND", "snowflake")
    monkeypatch.setenv("AGENT_RETRY_POLICY", "adaptive")
    monkeypatch.setenv("AGENT_MAX_RETRIES", "5")
    monkeypatch.setenv("SCHEMA_CACHE_TTL_SECONDS", "600")
    monkeypatch.setenv("AGENT_COLUMN_ALLOWLIST_MODE", "warn")
    monkeypatch.setenv("AGENT_CARTESIAN_JOIN_MODE", "warn")

    with TestClient(agent_app.app) as client:
        resp = client.get("/agent/diagnostics")

    assert resp.status_code == 200
    body = resp.json()
    assert body["active_database_provider"] == "snowflake"
    assert body["retry_policy"] == {"mode": "adaptive", "max_retries": 5}
    assert body["schema_cache_ttl_seconds"] == 600
    assert "runtime_indicators" in body
    assert "active_schema_cache_size" in body["runtime_indicators"]
    assert "enabled_flags" in body
    assert "OPENAI_API_KEY" not in str(body)


def test_agent_diagnostics_endpoint_debug(monkeypatch):
    """Debug diagnostics should include recent stage-latency breakdown."""
    from agent.runtime_metrics import record_stage_latency_breakdown, reset_runtime_metrics

    reset_runtime_metrics()
    record_stage_latency_breakdown({"execution_ms": 12.5, "validation_ms": 3.0})

    with TestClient(agent_app.app) as client:
        resp = client.get("/agent/diagnostics?debug=true")

    assert resp.status_code == 200
    body = resp.json()
    assert body["debug"]["latency_breakdown_ms"]["execution_ms"] == 12.5


def test_agent_diagnostics_endpoint_self_test():
    """Self-test mode should return fake-path validation/execution health checks."""
    with TestClient(agent_app.app) as client:
        resp = client.get("/agent/diagnostics?self_test=true")

    assert resp.status_code == 200
    body = resp.json()
    assert body["self_test"]["status"] in {"ok", "degraded"}
    assert body["self_test"]["validation"]["status"] in {"ok", "degraded"}
    assert body["self_test"]["execution"]["status"] in {"ok", "degraded"}
