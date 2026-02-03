import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { fetchSpanDetail, fetchTraceDetail, fetchTraceSpans, getErrorMessage } from "../api";
import { SpanDetail, SpanSummary, TraceDetail as TraceDetailModel } from "../types";
import SpanDetailDrawer from "../components/trace/SpanDetailDrawer";
import WaterfallView, { WaterfallRow } from "../components/trace/WaterfallView";
import { buildWaterfallRows, computeCriticalPath } from "../components/trace/waterfall/waterfall_model";
import SpanTable from "../components/trace/SpanTable";
import PromptViewer from "../components/trace/PromptViewer";
import ApiLinksPanel from "../components/trace/ApiLinksPanel";
import { useOtelHealth } from "../hooks/useOtelHealth";
import { computeSpanCoverage } from "../components/trace/trace_coverage";

const TRACE_ID_RE = /^[0-9a-f]{32}$/i;
const SPAN_PAGE_LIMIT = 500;
const SPAN_MAX_LIMIT = 5000;

export default function TraceDetail() {
  const { traceId } = useParams();
  const [searchParams] = useSearchParams();

  const [trace, setTrace] = useState<TraceDetailModel | null>(null);
  const [traceError, setTraceError] = useState<string | null>(null);
  const [isTraceLoading, setIsTraceLoading] = useState(true);

  const [spans, setSpans] = useState<SpanSummary[]>([]);
  const [spansError, setSpansError] = useState<string | null>(null);
  const [isSpansLoading, setIsSpansLoading] = useState(true);
  const [isLoadingMoreSpans, setIsLoadingMoreSpans] = useState(false);
  const [hasPartialData, setHasPartialData] = useState(false);
  const [spanOffset, setSpanOffset] = useState(0);
  const [hasMoreSpans, setHasMoreSpans] = useState(false);

  const [selectedSpan, setSelectedSpan] = useState<SpanDetail | null>(null);
  const [selectedSpanId, setSelectedSpanId] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState("start_time");
  const [showCriticalPath, setShowCriticalPath] = useState(false);
  const [showEvents, setShowEvents] = useState(true);

  const { reportFailure, reportSuccess } = useOtelHealth();

  const loadTraceDetailData = useCallback(async () => {
    if (!traceId) return;
    if (!TRACE_ID_RE.test(traceId)) {
      setTraceError("Invalid trace id format.");
      setIsTraceLoading(false);
      return;
    }

    setIsTraceLoading(true);
    setTraceError(null);

    try {
      const traceData = await fetchTraceDetail(traceId);
      setTrace(traceData);
      reportSuccess();
    } catch (err: unknown) {
      const errorMessage = getErrorMessage(err);
      setTraceError(errorMessage);
      reportFailure(errorMessage);
    } finally {
      setIsTraceLoading(false);
    }
  }, [traceId, reportSuccess, reportFailure]);

  const loadSpansData = useCallback(async (resumeFromOffset?: number) => {
    if (!traceId || !TRACE_ID_RE.test(traceId)) return;

    const isResume = resumeFromOffset !== undefined;

    if (!isResume) {
      setIsSpansLoading(true);
      setSpansError(null);
      setHasPartialData(false);
      setSpans([]);
      setSpanOffset(0);
      setHasMoreSpans(false);
    } else {
      setIsLoadingMoreSpans(true);
      setSpansError(null);
    }

    try {
      const offset = resumeFromOffset ?? 0;
      const page = await fetchTraceSpans(traceId, SPAN_PAGE_LIMIT, offset);

      setSpans((prev) => {
        const existingIds = new Set(prev.map((s) => s.span_id));
        const newSpans = page.filter((s) => !existingIds.has(s.span_id));
        return [...prev, ...newSpans];
      });

      const nextOffset = offset + page.length;
      setSpanOffset(nextOffset);
      const totalSpans = trace?.span_count;
      const hasMoreFromServer =
        page.length === SPAN_PAGE_LIMIT &&
        nextOffset < SPAN_MAX_LIMIT &&
        (totalSpans == null || nextOffset < totalSpans);
      setHasMoreSpans(hasMoreFromServer);
    } catch (err: unknown) {
      const errorMessage = getErrorMessage(err);
      setSpansError(errorMessage);
      setIsLoadingMoreSpans(false);

      // Check if we have any spans already - that means partial data
      setSpans((currentSpans) => {
        if (currentSpans.length > 0) {
          setHasPartialData(true);
        }
        return currentSpans;
      });

      // We don't report global failure for spans if trace detail succeeded
      // to avoid triggering global outage banner for partial data issues.
    } finally {
      setIsSpansLoading(false);
      setIsLoadingMoreSpans(false);
    }
  }, [traceId, trace?.span_count]);

  useEffect(() => {
    if (traceId) {
      loadTraceDetailData();
      loadSpansData();
    }
  }, [loadTraceDetailData, loadSpansData, traceId]);

  const handleRetryTrace = () => {
    loadTraceDetailData();
    loadSpansData();
  };

  const handleRetrySpans = () => {
    loadSpansData();
  };

  const handleRetryRemainingSpans = () => {
    // Resume from where we left off
    loadSpansData(spanOffset);
  };

  const handleLoadMoreSpans = () => {
    loadSpansData(spanOffset);
  };

  const filteredSpans = useMemo(() => {
    if (!search.trim()) return spans;
    const q = search.toLowerCase();
    return spans.filter((span) => {
      if (span.name.toLowerCase().includes(q)) return true;
      const attrs = span.span_attributes || {};
      return Object.keys(attrs).some((key) => key.toLowerCase().includes(q));
    });
  }, [spans, search]);

  const rows = useMemo(() => buildWaterfallRows(filteredSpans), [filteredSpans]);

  const criticalPath = useMemo(() => computeCriticalPath(spans), [spans]);

  const sortedTableSpans = useMemo(() => {
    const data = [...filteredSpans];
    if (sortKey === "duration") {
      return data.sort((a, b) => b.duration_ms - a.duration_ms);
    }
    if (sortKey === "status") {
      return data.sort((a, b) => a.status_code.localeCompare(b.status_code));
    }
    return data.sort(
      (a, b) =>
        new Date(a.start_time).getTime() - new Date(b.start_time).getTime()
    );
  }, [filteredSpans, sortKey]);

  const traceStart = useMemo(() => {
    if (trace?.start_time) return new Date(trace.start_time).getTime();
    if (!spans.length) return Date.now();
    return Math.min(...spans.map((s) => new Date(s.start_time).getTime()));
  }, [trace, spans]);

  const traceDuration = trace?.duration_ms ?? 0;
  const {
    loadedCount: loadedSpanCount,
    totalCount: totalSpanCount,
    totalKnown: totalSpanKnown,
    coveragePct,
    reachedMaxLimit
  } = computeSpanCoverage(spans.length, trace?.span_count ?? null, SPAN_MAX_LIMIT);

  const handleSpanSelect = (spanId: string) => {
    if (!traceId) return;
    setSelectedSpanId(spanId);
    fetchSpanDetail(traceId, spanId)
      .then((detail) => setSelectedSpan(detail))
      .catch((err) => {
          // Local error handling for span detail could be improved, but out of scope
          console.error(err);
      });
  };

  const interactionId = searchParams.get("interactionId");

  if (isTraceLoading) {
    return (
      <div className="panel" style={{ textAlign: "center", padding: "60px 40px" }}>
        <h2 style={{ margin: "0 0 12px 0" }}>Loading trace...</h2>
        <p className="subtitle" style={{ marginBottom: "8px" }}>
          Trace ID: <code style={{ fontSize: "0.85rem" }}>{traceId}</code>
        </p>
        <p style={{ color: "var(--muted)", fontSize: "0.9rem" }}>
          Fetching trace data from telemetry store
        </p>
      </div>
    );
  }

  if (traceError || !trace) {
    return (
      <div className="panel" style={{ textAlign: "center", padding: "60px 40px" }}>
        <div style={{ fontSize: "2.5rem", marginBottom: "16px" }}>
          <svg
            width="48"
            height="48"
            viewBox="0 0 24 24"
            fill="none"
            stroke="var(--error)"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            style={{ display: "inline-block" }}
          >
            <circle cx="12" cy="12" r="10" />
            <line x1="12" y1="8" x2="12" y2="12" />
            <line x1="12" y1="16" x2="12.01" y2="16" />
          </svg>
        </div>
        <h2 style={{ margin: "0 0 12px 0", color: "var(--error)" }}>
          {traceError ? "Failed to load trace" : "No trace data"}
        </h2>
        <p style={{ color: "var(--muted)", marginBottom: "8px" }}>
          Trace ID: <code style={{ fontSize: "0.85rem" }}>{traceId}</code>
        </p>
        <p style={{ color: "var(--muted)", marginBottom: "24px", fontSize: "0.9rem" }}>
          {traceError || "The trace was not found or contains no data."}
        </p>
        <div style={{ display: "flex", gap: "12px", justifyContent: "center" }}>
          <button
            type="button"
            onClick={handleRetryTrace}
            style={{
              padding: "12px 24px",
              borderRadius: "10px",
              border: "none",
              backgroundColor: "var(--accent)",
              color: "#fff",
              fontWeight: 600,
              cursor: "pointer"
            }}
          >
            Retry
          </button>
          <Link
            to="/admin/traces/search"
            style={{
              padding: "12px 24px",
              borderRadius: "10px",
              border: "1px solid var(--border)",
              backgroundColor: "transparent",
              color: "var(--ink)",
              fontWeight: 500,
              textDecoration: "none",
              display: "inline-block"
            }}
          >
            Back to Search
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="trace-detail">
      <header className="hero">
        <div>
          <p className="kicker">Trace Detail</p>
          <h1>Trace {trace.trace_id.slice(0, 8)}...</h1>
          <p className="subtitle">Service: {trace.service_name}</p>
        </div>
      </header>

      <div className="trace-metadata">
        <div className="trace-metadata__card">
          <div>
            <span>Status</span>
            <strong>{trace.status}</strong>
          </div>
          <div>
            <span>Duration</span>
            <strong>{trace.duration_ms} ms</strong>
          </div>
          <div>
            <span>Spans</span>
            <strong>{trace.span_count}</strong>
          </div>
          <div>
            <span>Model</span>
            <strong>{trace.model_name || "—"}</strong>
          </div>
        </div>
        <div className="trace-metadata__card">
          <div>
            <span>Total Tokens</span>
            <strong>{trace.total_tokens ?? "—"}</strong>
          </div>
          <div>
            <span>Prompt Tokens</span>
            <strong>{trace.prompt_tokens ?? "—"}</strong>
          </div>
          <div>
            <span>Completion Tokens</span>
            <strong>{trace.completion_tokens ?? "—"}</strong>
          </div>
          <div>
            <span>Cost (est.)</span>
            <strong>
              {trace.estimated_cost_usd != null
                ? `$${trace.estimated_cost_usd.toFixed(4)}`
                : "—"}
            </strong>
          </div>
        </div>
        {interactionId && (
          <div className="trace-metadata__card">
            <div>
              <span>Interaction</span>
              <strong>{interactionId.slice(0, 8)}...</strong>
            </div>
          </div>
        )}
      </div>

      <div className="trace-detail__layout">
        <div className="trace-detail__left">
          <div className="trace-panel">
            <div className="trace-panel__header">
              <h3>Waterfall</h3>
              <span className="subtitle">
                {isSpansLoading
                  ? "Loading..."
                  : totalSpanKnown
                    ? `Loaded ${loadedSpanCount} / ${totalSpanCount} spans (${coveragePct ?? 0}%)`
                    : `Loaded ${loadedSpanCount} spans (total unknown)`}
              </span>
            </div>

            {isSpansLoading && spans.length === 0 && (
                <div style={{ padding: "40px", textAlign: "center", color: "var(--muted)" }}>
                    Loading spans...
                </div>
            )}

            {!isSpansLoading && spansError && spans.length === 0 && (
                <div style={{ padding: "40px", textAlign: "center", border: "1px dashed var(--error)", borderRadius: "8px", margin: "20px" }}>
                    <div style={{ color: "var(--error)", marginBottom: "12px", fontWeight: 500 }}>
                        Failed to load spans
                    </div>
                    <div style={{ fontSize: "0.9rem", color: "var(--muted)", marginBottom: "16px" }}>
                        {spansError}
                    </div>
                    <button
                        onClick={handleRetrySpans}
                        style={{
                            padding: "8px 16px",
                            borderRadius: "6px",
                            border: "none",
                            backgroundColor: "var(--surface-muted)",
                            color: "var(--ink)",
                            fontWeight: 600,
                            cursor: "pointer"
                        }}
                    >
                        Retry Spans
                    </button>
                </div>
            )}

            {spans.length > 0 && (
                <>
                    {hasPartialData && spansError && (
                        <div style={{
                            padding: "12px 16px",
                            margin: "12px",
                            backgroundColor: "#fef3c7",
                            border: "1px solid #fcd34d",
                            borderRadius: "8px",
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "space-between",
                            gap: "12px"
                        }}>
                            <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                                <span style={{ color: "#b45309", fontWeight: 500 }}>Some spans failed to load</span>
                                <span style={{ color: "#92400e", fontSize: "0.85rem" }}>({spansError})</span>
                            </div>
                            <button
                                onClick={handleRetryRemainingSpans}
                                style={{
                                    padding: "6px 12px",
                                    borderRadius: "6px",
                                    border: "1px solid #fcd34d",
                                    backgroundColor: "#fef3c7",
                                    color: "#b45309",
                                    fontWeight: 600,
                                    cursor: "pointer",
                                    fontSize: "0.85rem"
                                }}
                            >
                                Retry remaining
                            </button>
                        </div>
                    )}
                    <div className="trace-waterfall__controls">
                        <div className="trace-waterfall__controls-group">
                            <span className="trace-waterfall__controls-label">Grouped</span>
                            <span className="trace-waterfall__controls-pill">Event type</span>
                        </div>
                        <label>
                            <input
                                type="checkbox"
                                checked={showEvents}
                                onChange={(e) => setShowEvents(e.target.checked)}
                            />
                            Show Events
                        </label>
                        <label>
                            <input
                                type="checkbox"
                                checked={showCriticalPath}
                                onChange={(e) => setShowCriticalPath(e.target.checked)}
                            />
                            Show Critical Path
                        </label>
                    </div>
                    {reachedMaxLimit && (totalSpanCount == null || loadedSpanCount < totalSpanCount) && (
                      <div className="trace-waterfall__limit-banner">
                        <span>
                          Showing first {SPAN_MAX_LIMIT.toLocaleString()} spans (UI limit).
                          {totalSpanKnown ? ` Total spans: ${totalSpanCount}.` : " Total span count unknown."}
                        </span>
                        <Link to={`/admin/traces/search?trace_id=${trace.trace_id}`}>
                          Open in Trace Search
                        </Link>
                      </div>
                    )}
                    <WaterfallView
                        rows={rows}
                        traceStart={traceStart}
                        traceDurationMs={traceDuration}
                        onSelect={handleSpanSelect}
                        criticalPath={criticalPath}
                        showCriticalPath={showCriticalPath}
                        showEvents={showEvents}
                        selectedSpanId={selectedSpanId}
                    />
                    {(hasMoreSpans || reachedMaxLimit) && (
                      <div className="trace-waterfall__load-more">
                        <button
                          type="button"
                          onClick={handleLoadMoreSpans}
                          disabled={!hasMoreSpans || isLoadingMoreSpans || reachedMaxLimit}
                        >
                          {reachedMaxLimit ? "UI limit reached" : isLoadingMoreSpans ? "Loading..." : "Load more spans"}
                        </button>
                        <span className="trace-waterfall__load-more-note">
                          {totalSpanKnown
                            ? `${loadedSpanCount} of ${totalSpanCount} spans loaded`
                            : `${loadedSpanCount} spans loaded`}
                        </span>
                      </div>
                    )}
                    {isLoadingMoreSpans && (
                        <div style={{ padding: "16px", textAlign: "center", color: "var(--muted)", borderTop: "1px solid var(--border)" }}>
                            Loading more spans...
                        </div>
                    )}
                </>
            )}
          </div>

          <div className="trace-panel">
            <div className="trace-panel__header">
              <h3>Span Table</h3>
              <div style={{ display: "flex", gap: "8px" }}>
                <input
                  type="text"
                  placeholder="Search spans..."
                  value={search}
                  onChange={(event) => setSearch(event.target.value)}
                  disabled={isSpansLoading && spans.length === 0}
                />
                <select
                    value={sortKey}
                    onChange={(event) => setSortKey(event.target.value)}
                    disabled={isSpansLoading && spans.length === 0}
                >
                  <option value="start_time">Start time</option>
                  <option value="duration">Duration</option>
                  <option value="status">Status</option>
                </select>
              </div>
            </div>

            {isSpansLoading && spans.length === 0 && <div style={{ padding: "20px", color: "var(--muted)" }}>Loading...</div>}

            {!isSpansLoading && spansError && spans.length === 0 && (
                <div style={{ padding: "20px", color: "var(--muted)" }}>
                    Span data unavailable.
                </div>
            )}

            {spans.length > 0 && (
                <>
                    <SpanTable
                      spans={sortedTableSpans}
                      onSelect={handleSpanSelect}
                      selectedSpanId={selectedSpanId}
                    />
                    {(hasMoreSpans || reachedMaxLimit) && (
                      <div className="trace-waterfall__load-more trace-waterfall__load-more--table">
                        <button
                          type="button"
                          onClick={handleLoadMoreSpans}
                          disabled={!hasMoreSpans || isLoadingMoreSpans || reachedMaxLimit}
                        >
                          {reachedMaxLimit ? "UI limit reached" : isLoadingMoreSpans ? "Loading..." : "Load more spans"}
                        </button>
                        <span className="trace-waterfall__load-more-note">
                          {totalSpanKnown
                            ? `${loadedSpanCount} of ${totalSpanCount} spans loaded`
                            : `${loadedSpanCount} spans loaded`}
                        </span>
                      </div>
                    )}
                    {isLoadingMoreSpans && (
                        <div style={{ padding: "12px", textAlign: "center", color: "var(--muted)", fontSize: "0.9rem" }}>
                            Loading more spans...
                        </div>
                    )}
                </>
            )}
          </div>
        </div>

        <div className="trace-detail__right">
          <PromptViewer span={selectedSpan} />
          <ApiLinksPanel traceId={trace.trace_id} />
          <div className="trace-panel">
            <h3>Trace Attributes</h3>
            <pre>{JSON.stringify(trace.trace_attributes, null, 2)}</pre>
          </div>
          <div className="trace-panel">
            <h3>Resource Attributes</h3>
            <pre>{JSON.stringify(trace.resource_attributes, null, 2)}</pre>
          </div>
        </div>
      </div>

      <SpanDetailDrawer
        span={selectedSpan}
        onClose={() => {
          setSelectedSpan(null);
          setSelectedSpanId(null);
        }}
      />
    </div>
  );
}
