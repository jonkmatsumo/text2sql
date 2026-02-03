import React, { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { fetchTraceDetail, fetchTraceSpans } from "../api";
import { SpanSummary, TraceDetail as TraceDetailModel } from "../types";
import { alignStages, buildStageRollups } from "../components/trace/compare/trace_compare_model";

const TRACE_ID_RE = /^[0-9a-f]{32}$/i;

function formatMs(value?: number) {
  if (value == null) return "—";
  if (value < 1000) return `${value} ms`;
  return `${(value / 1000).toFixed(2)} s`;
}

function formatDelta(left?: number, right?: number) {
  if (left == null || right == null) return "—";
  const diff = left - right;
  const sign = diff > 0 ? "+" : "";
  return `${sign}${formatMs(Math.abs(diff))}`;
}

export default function TraceCompare() {
  const [searchParams, setSearchParams] = useSearchParams();
  const initialLeft = searchParams.get("left") || "";
  const initialRight = searchParams.get("right") || "";
  const [leftId, setLeftId] = useState(initialLeft);
  const [rightId, setRightId] = useState(initialRight);
  const [leftTrace, setLeftTrace] = useState<TraceDetailModel | null>(null);
  const [rightTrace, setRightTrace] = useState<TraceDetailModel | null>(null);
  const [leftSpans, setLeftSpans] = useState<SpanSummary[]>([]);
  const [rightSpans, setRightSpans] = useState<SpanSummary[]>([]);
  const [leftLoading, setLeftLoading] = useState(false);
  const [rightLoading, setRightLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [leftError, setLeftError] = useState<string | null>(null);
  const [rightError, setRightError] = useState<string | null>(null);
  const [focusedStage, setFocusedStage] = useState<string | null>(null);
  const isLoading = leftLoading || rightLoading;
  const [showOnlyDeltas, setShowOnlyDeltas] = useState(false);
  const [deltaThreshold, setDeltaThreshold] = useState(0);
  const [deltaSort, setDeltaSort] = useState<"delta" | "left" | "right">("delta");

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    const next = new URLSearchParams();
    if (leftId.trim()) next.set("left", leftId.trim());
    if (rightId.trim()) next.set("right", rightId.trim());
    setSearchParams(next, { replace: true });
  };

  const handleSwap = () => {
    const left = searchParams.get("left");
    const right = searchParams.get("right");
    const next = new URLSearchParams();
    if (right) next.set("left", right);
    if (left) next.set("right", left);
    setSearchParams(next, { replace: true });
  };

  useEffect(() => {
    setLeftId(searchParams.get("left") || "");
    setRightId(searchParams.get("right") || "");
  }, [searchParams]);

  useEffect(() => {
    const left = searchParams.get("left") || "";
    const right = searchParams.get("right") || "";
    if (!TRACE_ID_RE.test(left) || !TRACE_ID_RE.test(right)) {
      setError("Both trace IDs must be valid 32-character hexadecimal values.");
      return;
    }
    setError(null);
    setFocusedStage(null);

    const loadSide = async (
      sideLabel: string,
      id: string,
      setTrace: React.Dispatch<React.SetStateAction<TraceDetailModel | null>>,
      setSpans: React.Dispatch<React.SetStateAction<SpanSummary[]>>,
      setSideError: React.Dispatch<React.SetStateAction<string | null>>,
      setLoading: React.Dispatch<React.SetStateAction<boolean>>
    ) => {
      setLoading(true);
      setSideError(null);
      setTrace(null);
      setSpans([]);
      try {
        const detail = await fetchTraceDetail(id);
        setTrace(detail);
      } catch (err: any) {
        setSideError(`Failed to load ${sideLabel} trace detail`);
        setLoading(false);
        return;
      }
      try {
        const spanData = await fetchTraceSpans(id, 2000, 0);
        setSpans(spanData);
      } catch (err: any) {
        setSpans([]);
        setSideError((prev) => prev ?? `Failed to load ${sideLabel} spans`);
      } finally {
        setLoading(false);
      }
    };

    loadSide("left", left, setLeftTrace, setLeftSpans, setLeftError, setLeftLoading);
    loadSide("right", right, setRightTrace, setRightSpans, setRightError, setRightLoading);
  }, [searchParams]);

  const alignedStages = useMemo(() => {
    const leftStages = buildStageRollups(leftSpans);
    const rightStages = buildStageRollups(rightSpans);
    return alignStages(leftStages, rightStages);
  }, [leftSpans, rightSpans]);

  const filteredStages = useMemo(() => {
    return alignedStages.filter((stage) => {
      if (!showOnlyDeltas) return true;
      const leftDuration = stage.left?.totalDurationMs ?? 0;
      const rightDuration = stage.right?.totalDurationMs ?? 0;
      const delta = Math.abs(leftDuration - rightDuration);
      return delta >= deltaThreshold;
    });
  }, [alignedStages, showOnlyDeltas, deltaThreshold]);

  const sortedStages = useMemo(() => {
    const list = [...filteredStages];
    list.sort((a, b) => {
      const leftDelta = Math.abs((a.left?.totalDurationMs ?? 0) - (a.right?.totalDurationMs ?? 0));
      const rightDelta = Math.abs((b.left?.totalDurationMs ?? 0) - (b.right?.totalDurationMs ?? 0));
      if (deltaSort === "delta") {
        return rightDelta - leftDelta;
      }
      if (deltaSort === "left") {
        return (b.left?.totalDurationMs ?? 0) - (a.left?.totalDurationMs ?? 0);
      }
      return (b.right?.totalDurationMs ?? 0) - (a.right?.totalDurationMs ?? 0);
    });
    return list;
  }, [filteredStages, deltaSort]);

  const renderPane = (
    sideLabel: "Left" | "Right",
    trace: TraceDetailModel | null,
    spans: SpanSummary[],
    sideError: string | null,
    loading: boolean
  ) => {
    const loaded = spans.length;
    const total = trace?.span_count;
    const truncated = spans.length >= 2000;
    const coverageText = total
      ? `Loaded ${loaded} / ${total} spans`
      : `Loaded ${loaded} spans (total unknown)`;

    return (
      <div className="trace-compare__pane">
        <div className="trace-compare__pane-header">
          <div>
            <h4>{sideLabel} Trace</h4>
            <div className="trace-compare__pane-id">
              {trace?.trace_id || "Trace not loaded"}
            </div>
          </div>
          <div className="trace-compare__pane-status">
            {loading ? "Loading..." : sideError ? "Error" : trace ? trace.service_name : "Idle"}
          </div>
        </div>
        {sideError ? (
          <div className="trace-compare__error">{sideError}</div>
        ) : trace ? (
          <div className="trace-compare__pane-summary">
            <div>Duration: {formatMs(trace.duration_ms)}</div>
            <div>Status: {trace.status}</div>
            <div>Stage coverage: {trace.span_count ? `${trace.span_count} spans` : "unknown"}</div>
          </div>
        ) : (
          <div className="trace-compare__empty">No trace metadata</div>
        )}
        <div className="trace-compare__coverage">
          <span>{coverageText}</span>
          {truncated && <span className="trace-compare__coverage-note">truncated at fetch limit</span>}
        </div>
      </div>
    );
  };

  return (
    <div className="page">
      <header className="hero">
        <div>
          <p className="kicker">Observability</p>
          <h1>Trace Comparison</h1>
          <p className="subtitle">Compare two traces side-by-side with aligned stages.</p>
        </div>
      </header>

      <div className="panel">
        <form onSubmit={handleSubmit} className="trace-compare__form">
          <label>
            Left trace ID
            <input
              type="text"
              value={leftId}
              onChange={(event) => setLeftId(event.target.value)}
              placeholder="Trace ID"
            />
          </label>
          <label>
            Right trace ID
            <input
              type="text"
              value={rightId}
              onChange={(event) => setRightId(event.target.value)}
              placeholder="Trace ID"
            />
          </label>
          <button type="submit">Compare</button>
          </form>
      </div>

      {!initialLeft || !initialRight ? (
        <div className="panel" style={{ textAlign: "center", color: "var(--muted)" }}>
          Enter two trace IDs to load the comparison.
        </div>
      ) : null}

      {(leftTrace || rightTrace || leftError || rightError) && (
        <div className="trace-compare__panes">
          {renderPane("Left", leftTrace, leftSpans, leftError, leftLoading)}
          <div className="trace-compare__swap">
            <button type="button" onClick={handleSwap}>
              Swap A/B
            </button>
          </div>
          {renderPane("Right", rightTrace, rightSpans, rightError, rightLoading)}
        </div>
      )}

      {isLoading && (
        <div className="panel" style={{ textAlign: "center", color: "var(--muted)" }}>
          Loading comparison...
        </div>
      )}

      {error && (
        <div className="panel" style={{ textAlign: "center", color: "var(--error)" }}>
          {error}
        </div>
      )}

      {!isLoading && leftTrace && rightTrace && (
        <div className="trace-compare__grid">
          <div className="trace-compare__summary">
            <div className="trace-compare__card">
              <h4>Duration</h4>
              <div className="trace-compare__card-row">
                <span>Left</span>
                <strong>{formatMs(leftTrace.duration_ms)}</strong>
              </div>
              <div className="trace-compare__card-row">
                <span>Right</span>
                <strong>{formatMs(rightTrace.duration_ms)}</strong>
              </div>
              <div className="trace-compare__card-row">
                <span>Delta</span>
                <strong>{formatDelta(leftTrace.duration_ms, rightTrace.duration_ms)}</strong>
              </div>
            </div>
            <div className="trace-compare__card">
              <h4>Tokens</h4>
              <div className="trace-compare__card-row">
                <span>Left</span>
                <strong>{leftTrace.total_tokens ?? "—"}</strong>
              </div>
              <div className="trace-compare__card-row">
                <span>Right</span>
                <strong>{rightTrace.total_tokens ?? "—"}</strong>
              </div>
            </div>
            <div className="trace-compare__card">
              <h4>Cost</h4>
              <div className="trace-compare__card-row">
                <span>Left</span>
                <strong>{leftTrace.estimated_cost_usd != null ? `$${leftTrace.estimated_cost_usd.toFixed(4)}` : "—"}</strong>
              </div>
              <div className="trace-compare__card-row">
                <span>Right</span>
                <strong>{rightTrace.estimated_cost_usd != null ? `$${rightTrace.estimated_cost_usd.toFixed(4)}` : "—"}</strong>
              </div>
            </div>
          </div>

          <div className="trace-compare__delta-controls">
            <div className="trace-compare__control-group">
              <span>Sort by</span>
              <select value={deltaSort} onChange={(event) => setDeltaSort(event.target.value as "delta" | "left" | "right")}>
                <option value="delta">Delta</option>
                <option value="left">Left duration</option>
                <option value="right">Right duration</option>
              </select>
            </div>
            <label className="trace-compare__control-group">
              <input
                type="checkbox"
                checked={showOnlyDeltas}
                onChange={(event) => setShowOnlyDeltas(event.target.checked)}
              />
              Show only deltas
            </label>
            <label className="trace-compare__control-group">
              <span>Threshold (ms)</span>
              <input
                type="number"
                min={0}
                value={deltaThreshold}
                onChange={(event) => setDeltaThreshold(Math.max(0, Number(event.target.value) || 0))}
              />
            </label>
          </div>
          <div className="trace-compare__stages">
            <div className="trace-compare__stages-header">
              <span>Stage</span>
              <span>Left</span>
              <span>Right</span>
              <span>Delta</span>
            </div>
            {sortedStages.map((stage) => {
              const isFocused = focusedStage === stage.key;
              return (
                <button
                  key={stage.key}
                  type="button"
                  className={`trace-compare__stage-row${isFocused ? " is-focused" : ""}`}
                  onClick={() => setFocusedStage(stage.key)}
                >
                  <span>{stage.label}</span>
                  <span>{formatMs(stage.left?.totalDurationMs)}</span>
                  <span>{formatMs(stage.right?.totalDurationMs)}</span>
                  <span>{formatDelta(stage.left?.totalDurationMs, stage.right?.totalDurationMs)}</span>
                </button>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
