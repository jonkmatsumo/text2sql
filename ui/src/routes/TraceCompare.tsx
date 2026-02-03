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
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [focusedStage, setFocusedStage] = useState<string | null>(null);

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    const next = new URLSearchParams();
    if (leftId.trim()) next.set("left", leftId.trim());
    if (rightId.trim()) next.set("right", rightId.trim());
    setSearchParams(next, { replace: true });
  };

  useEffect(() => {
    const left = searchParams.get("left") || "";
    const right = searchParams.get("right") || "";
    if (!TRACE_ID_RE.test(left) || !TRACE_ID_RE.test(right)) return;
    setIsLoading(true);
    setError(null);
    Promise.all([
      fetchTraceDetail(left),
      fetchTraceDetail(right),
      fetchTraceSpans(left, 2000, 0),
      fetchTraceSpans(right, 2000, 0)
    ])
      .then(([leftTraceData, rightTraceData, leftSpanData, rightSpanData]) => {
        setLeftTrace(leftTraceData);
        setRightTrace(rightTraceData);
        setLeftSpans(leftSpanData);
        setRightSpans(rightSpanData);
      })
      .catch((err) => {
        setError(err?.message || "Failed to load traces");
      })
      .finally(() => {
        setIsLoading(false);
      });
  }, [searchParams]);

  const alignedStages = useMemo(() => {
    const leftStages = buildStageRollups(leftSpans);
    const rightStages = buildStageRollups(rightSpans);
    return alignStages(leftStages, rightStages);
  }, [leftSpans, rightSpans]);

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

          <div className="trace-compare__stages">
            <div className="trace-compare__stages-header">
              <span>Stage</span>
              <span>Left</span>
              <span>Right</span>
              <span>Delta</span>
            </div>
            {alignedStages.map((stage) => {
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
