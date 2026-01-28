import React, { useEffect, useMemo, useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { fetchSpanDetail, fetchTraceDetail, fetchTraceSpans } from "../api";
import { SpanDetail, SpanSummary, TraceDetail as TraceDetailModel } from "../types";
import SpanDetailDrawer from "../components/trace/SpanDetailDrawer";
import WaterfallView, { WaterfallRow } from "../components/trace/WaterfallView";
import SpanTable from "../components/trace/SpanTable";
import PromptViewer from "../components/trace/PromptViewer";
import ApiLinksPanel from "../components/trace/ApiLinksPanel";

const TRACE_ID_RE = /^[0-9a-f]{32}$/i;

function buildSpanRows(spans: SpanSummary[]): WaterfallRow[] {
  const byId = new Map<string, SpanSummary>();
  const children = new Map<string | null, SpanSummary[]>();

  spans.forEach((span) => {
    byId.set(span.span_id, span);
  });

  spans.forEach((span) => {
    const parent = span.parent_span_id || null;
    if (!children.has(parent)) children.set(parent, []);
    children.get(parent)!.push(span);
  });

  const sortSpans = (a: SpanSummary, b: SpanSummary) => {
    const at = new Date(a.start_time).getTime();
    const bt = new Date(b.start_time).getTime();
    if (at !== bt) return at - bt;
    const aSeq = Number(a.span_attributes?.["event.seq"] ?? 0);
    const bSeq = Number(b.span_attributes?.["event.seq"] ?? 0);
    return aSeq - bSeq;
  };

  const roots = spans.filter(
    (span) => !span.parent_span_id || !byId.has(span.parent_span_id)
  );
  roots.sort(sortSpans);

  const rows: WaterfallRow[] = [];
  const walk = (span: SpanSummary, depth: number) => {
    rows.push({ span, depth });
    const kids = (children.get(span.span_id) || []).sort(sortSpans);
    kids.forEach((child) => walk(child, depth + 1));
  };

  roots.forEach((root) => walk(root, 0));
  return rows;
}

export default function TraceDetail() {
  const { traceId } = useParams();
  const [searchParams] = useSearchParams();
  const [trace, setTrace] = useState<TraceDetailModel | null>(null);
  const [spans, setSpans] = useState<SpanSummary[]>([]);
  const [selectedSpan, setSelectedSpan] = useState<SpanDetail | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState("start_time");

  useEffect(() => {
    if (!traceId) return;
    if (!TRACE_ID_RE.test(traceId)) {
      setError("Invalid trace id format.");
      setIsLoading(false);
      return;
    }

    setIsLoading(true);
    setError(null);

    const loadSpans = async () => {
      const limit = 500;
      const maxSpans = 5000;
      let offset = 0;
      let all: SpanSummary[] = [];
      while (true) {
        const page = await fetchTraceSpans(traceId, limit, offset);
        all = all.concat(page);
        if (page.length < limit || all.length >= maxSpans) break;
        offset += limit;
      }
      return all;
    };

    Promise.all([fetchTraceDetail(traceId), loadSpans()])
      .then(([traceData, spansData]) => {
        setTrace(traceData);
        setSpans(spansData);
      })
      .catch((err) => {
        setError(err.message || "Failed to load trace.");
      })
      .finally(() => setIsLoading(false));
  }, [traceId]);

  const filteredSpans = useMemo(() => {
    if (!search.trim()) return spans;
    const q = search.toLowerCase();
    return spans.filter((span) => {
      if (span.name.toLowerCase().includes(q)) return true;
      const attrs = span.span_attributes || {};
      return Object.keys(attrs).some((key) => key.toLowerCase().includes(q));
    });
  }, [spans, search]);

  const rows = useMemo(() => buildSpanRows(filteredSpans), [filteredSpans]);

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

  const handleSpanSelect = (spanId: string) => {
    if (!traceId) return;
    fetchSpanDetail(traceId, spanId)
      .then((detail) => setSelectedSpan(detail))
      .catch((err) => setError(err.message || "Failed to load span."));
  };

  const interactionId = searchParams.get("interactionId");

  if (isLoading) {
    return (
      <div className="panel">
        <h2>Loading trace...</h2>
        <p className="subtitle">Trace {traceId}</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="panel">
        <h2>Trace unavailable</h2>
        <p className="error">{error}</p>
      </div>
    );
  }

  if (!trace) {
    return (
      <div className="panel">
        <h2>No trace data</h2>
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
              <span className="subtitle">{rows.length} spans</span>
            </div>
            <WaterfallView
              rows={rows}
              traceStart={traceStart}
              traceDurationMs={traceDuration}
              onSelect={handleSpanSelect}
            />
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
                />
                <select value={sortKey} onChange={(event) => setSortKey(event.target.value)}>
                  <option value="start_time">Start time</option>
                  <option value="duration">Duration</option>
                  <option value="status">Status</option>
                </select>
              </div>
            </div>
            <SpanTable spans={sortedTableSpans} onSelect={handleSpanSelect} />
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

      <SpanDetailDrawer span={selectedSpan} onClose={() => setSelectedSpan(null)} />
    </div>
  );
}
