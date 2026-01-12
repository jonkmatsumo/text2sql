# Telemetry Backend Design: OpenTelemetry Integration

**Date:** 2026-01-11
**Status:** Proposed
**Author:** Antigravity (Observability Engineer)
**Context:** Design-level clarification for adding native OpenTelemetry (OTEL) support to the `TelemetryService` without modifying agent business logic.

---

## 1. Backend Abstraction Assessment

The existing `TelemetryBackend` interface in `agent/src/agent_core/telemetry.py` is **highly compatible** with OpenTelemetry concepts and requires no structural changes to the interface itself.

| Interface Method | MLflow Concept | OTEL Concept | Assessment |
| :--- | :--- | :--- | :--- |
| `start_span(name, ...)` | `mlflow.start_span` | `tracer.start_as_current_span` | **Clean mapping**. Both support context manager semantics. |
| `set_inputs(dict)` | Span Inputs (JSON/Artifact) | Span Attributes (`input.*` or JSON string) | **Compatible**. OTEL prefers flattened attributes or JSON strings. |
| `set_outputs(dict)` | Span Outputs (JSON/Artifact) | Span Attributes (`output.*` or JSON string) | **Compatible**. |
| `set_attribute(k, v)` | Span Tag/Attribute | Span Attribute | **Direct mapping**. |
| `add_event(name, attrs)` | Span Event | Span Event | **Direct mapping**. |
| `configure(**kwargs)` | MLflow `set_tracking_uri` | `TracerProvider` initialization | **Sufficient**. Can internalize OTEL setup logic. |
| `update_current_trace` | `mlflow.update_current_trace` | `trace.get_current_span()` (Active) | **Risk**. MLflow allows updating after span "end" (logic-dependent); OTEL enforces immutability after end. |

**Conclusion**: The abstraction is valid. The primary challenge is not the interface, but the **runtime lifecycle differences**, particularly regarding trace mutability.

---

## 2. MLflow → OTEL Span Mapping

We will map `MlflowTelemetrySpan` behaviors to `OTELTelemetrySpan` as follows:

### Span Identity & Hierarchy
| Feature | MLflow Implementation | OTEL Implementation | Notes |
| :--- | :--- | :--- | :--- |
| **Span Name** | User-provided string | User-provided string | Identical. |
| **Span Type** | `SpanType` Enum (CHAIN, TOOL, etc.) | **Attribute**: `span.type` | OTEL `SpanKind` should generally be `INTERNAL` for these agent steps. We will preserve specific semantics via a custom attribute. |
| **Hierarchy** | Implicit context (global stack) | **ContextVars** (Implicit) | OTEL's `start_as_current_span` automatically handles context propagation via `contextvars`, matching MLflow's behavior. |

### Data Mapping

**Inputs & Outputs**
- **Strategy**: JSON Serialization.
- **Rationale**: Agent inputs/outputs are often nested dictionaries which OTEL attributes (flat primitives) do not support natively.
- **Mapping**:
  - `set_inputs(d)` → `span.set_attribute("inputs", json.dumps(d))`
  - `set_outputs(d)` → `span.set_attribute("outputs", json.dumps(d))`

**Attributes**
- **Strategy**: Direct Passthrough.
- **Types**: Ensure values are `str`, `bool`, `int`, `float`, or `list` thereof. Complex objects must be `str()`'d.

### Metadata (`update_current_trace`)
- **Challenge**: In `graph.py`, `update_current_trace` is called *after* `app.ainvoke` (after the root span effectively finishes in the codebase's logical flow).
- **OTEL Constraint**: Cannot modify ended spans.
- **Mitigation**:
  - If the root span is still active (context propagation), we attach attributes.
  - If the root span is ended, we **log a warning** and drop the metadata (or strict implementation: create a "metadata" span).
  - *Recommendation*: For the initial implementation, attempt `trace.get_current_span()`. If it returns a non-recording span (or `INVALID`), fail silently/log, mirroring the `try/except` block in `graph.py`.

---

## 3. Error & Status Semantics

Current system uses "error-as-data":
```python
span.set_outputs({"error": "Security Policy Violation"})
```

**Proposed OTEL Behavior**:
We must bridge the gap between "logical error output" and "trace span status".

1.  **Passive Recording** (Base Requirement):
    - Continue setting `outputs` attribute with the JSON containing `"error"`.
    - This preserves parity with MLflow data visibility.

2.  **Active Status Setting** (Enhancement):
    - In `set_outputs(outputs)`:
        - Check `if "error" in outputs and outputs["error"] is not None:`
        - If true: `span.set_status(Status(StatusCode.ERROR, description=outputs["error"]))`
        - Else: `span.set_status(Status(StatusCode.OK))`
    - **Tradeoff**: This introduces slight logic into the backend but significantly improves Ops visibility in APM tools (Jaeger/Honeycomb) where error rates are key metrics.

---

## 4. OTEL Tracer Initialization Strategy

The `OTELTelemetryBackend.configure` method will own the `TracerProvider` details.

**Location**: `agent/src/agent_core/telemetry.py`

**Configuration Logic (`configure` method)**:
1.  Check if OTEL is already configured (global provider).
2.  If not:
    - Initialize `Resource` (service name=`text2sql-agent`).
    - Initialize `TracerProvider`.
    - Initialize `SpanProcessor` (Batch).
    - Initialize `SpanExporter` (OTLP usually, or Console/Memory for dev).
    - `trace.set_tracer_provider(provider)`.
3.  **Argument mapping**:
    - `tracking_uri` (MLflow concept) → Ignored or mapped to OTLP endpoint if applicable?
    - *Decision*: Introduce new kwargs to `configure` specifically for OTEL (e.g., `otel_endpoint`), or read generic env vars (`OTEL_EXPORTER_OTLP_ENDPOINT`) which standard OTEL SDKs do automatically. Use env vars as primary config source.

---

## 5. Dual-Write Safety (DualTelemetryBackend)

To enable safe migration, we will introduce `DualTelemetryBackend` that composites two backends.

**Structure**:
```python
class DualTelemetryBackend(TelemetryBackend):
    def __init__(self, primary: TelemetryBackend, secondary: TelemetryBackend):
        self.primary = primary
        self.secondary = secondary

    @contextlib.contextmanager
    def start_span(self, name, ...):
        # Enter both contexts.
        # CRITICAL: Isolate failure. IF secondary fails to start, primary should proceed.
        # However, context managers make 'try/except' around __enter__ tricky.
        # Simplified safe approach:
        with self.primary.start_span(...) as s1:
             # Attempt secondary
             try:
                 ctx = self.secondary.start_span(...)
                 s2 = ctx.__enter__()
             except:
                 s2 = None # Secondary failed

             yield DualTelemetrySpan(s1, s2)

             if s2:
                 ctx.__exit__(...)
```

**Requirements**:
1.  **Primary Authority**: The `primary` backend's exceptions bubble up. Logic depends on it.
2.  **Secondary Isolation**: Exceptions in `secondary` (start/end/set_attr) are logged and suppressed.
3.  **Trace ID Correlation**: Ideally, inject the Trace ID from Primary into Secondary? Hard if they are totally distinct systems. We accept they might generate distinct trace IDs for the same logical operation initially.

---

## 6. Testing Strategy

We explicitly avoid "mocking everything". We will use **In-Memory** implementations.

1.  **Unit Tests (`test_telemetry.py`)**:
    - Add `TestOTELBackend`:
    - Use `opentelemetry.sdk.trace.export.in_memory_span_exporter.InMemorySpanExporter`.
    - Validation:
        - Start span → verify in exporter.
        - `set_inputs` → verify attribute `inputs` exists and is valid JSON.
        - Error status logic → verify `span.status.status_code`.

2.  **Dual Backend Tests**:
    - `primary=InMemory1`, `secondary=InMemory2`.
    - Verify data appears in both.
    - Verify if `secondary` raises, `primary` survives.

---

## 7. Phased Implementation Blueprint

### Phase 1: Stabilize MLflow Backend
- **Status**: Completed (2026-01-11)
- **Action**: Cleanly encapsulated MLflow backend and fixed existing `InMemoryTelemetryBackend` generators.

### Phase 2: Native OTEL Implementation
- **Status**: Completed (2026-01-11)
- **Implemented**: `OTELTelemetryBackend` and `OTELTelemetrySpan`.
- **Details**:
  - `telemetry.inputs_json` and `telemetry.outputs_json` recorded as JSON strings.
  - Automatic `StatusCode.ERROR` mapping if `error` key present in outputs.
- **Verification**: Unit tests in `agent/tests/test_telemetry.py` using `InMemorySpanExporter`.

### Phase 3: Backend Selection & Dual-Write
- **Status**: Completed (2026-01-11)
- **Implemented**: `DualTelemetryBackend`, `DualTelemetrySpan`, and `TELEMETRY_BACKEND` selection logic.
- **Details**:
  - `TELEMETRY_BACKEND` env var supports `mlflow` (default), `otel`, and `dual`.
  - `DualTelemetryBackend` uses `contextlib.ExitStack` for safe multi-backend nesting.
  - Secondary backend is best-effort (exceptions are logged but suppressed).
- **Verification**: Tests for selection logic and failure isolation in `agent/tests/test_telemetry.py`.

### Phase 4: Smoke Verification
- **Status**: Pending
- **Goal**: Manual verification against a live OTEL collector.

---

## 8. Open Risks & Unknowns

1.  **Late Metadata Injection**: The `update_current_trace` call in `graph.py:347` happens *after* `app.ainvoke`.
    - **Risk**: The root span created by LangChain autoinstrumentation (if enabled) or the graph execution will likely be **ENDED** by then.
    - **Impact**: Tenant/User IDs might be missing from OTEL root traces.
    - **Workaround Plan**: Move metadata injection *into* the graph (e.g., a custom first node or `RunnableBinding`) or accept that for `v1`, these tags might only appear on the "Interaction Tool" usage spans if `update_interaction` is called inside the flow.

2.  **LangChain Autoinstrumentation**:
    - The `configure` method calls `mlflow.langchain.autolog()`.
    - If we switch to OTEL, we might need `opentelemetry-instrumentation-langchain`.
    - **Clarification**: We should treat this as "Manual Instrumentation" (`start_span` calls in nodes) FIRST. Autoinstrumentation is a separate layer. We will implement manual instrumentation backend first.

3.  **Dependencies**:
    - Need to ensure `opentelemetry-sdk` is available in the Docker image.
