import { SpanSummary } from "../../../types";

export interface WaterfallRow {
  span: SpanSummary;
  depth: number;
}

export interface SpanEventMarker {
  ts: number; // relative to trace start in ms
  name: string;
  attributes?: Record<string, any>;
}

export function extractSpanEventMarkers(
  span: SpanSummary,
  traceStartMs: number
): SpanEventMarker[] {
  if (!span.events || !Array.isArray(span.events)) return [];

  return span.events
    .map((event: any): SpanEventMarker | null => {
      // OTLP uses time_unix_nano or timeUnixNano.
      // Our backend parses it as time_unix_nano (based on _parse_events in parser.py)
      // but let's be safe.
      const rawTs = event.time_unix_nano || event.timeUnixNano;
      if (!rawTs) return null;

      const eventMs = Number(rawTs) / 1_000_000;
      return {
        ts: eventMs - traceStartMs,
        name: event.name || "event",
        attributes: event.attributes as Record<string, any> | undefined,
      };
    })
    .filter((m): m is SpanEventMarker => m !== null)
    .sort((a, b) => a.ts - b.ts);
}

export interface WaterfallGroup {
  id: string;
  label: string;
  rows: WaterfallRow[];
  isExpanded: boolean;
  totalDurationMs: number;
}

export function buildWaterfallRows(spans: SpanSummary[]): WaterfallRow[] {
  const byId = new Map<string, SpanSummary>();
  const children = new Map<string | null, SpanSummary[]>();

  spans.forEach((span) => {
    byId.set(span.span_id, span);
  });

  spans.forEach((span) => {
    const parent = span.parent_span_id || null;
    const parentExists = parent && byId.has(parent);
    const key = parentExists ? parent : null;
    if (!children.has(key)) children.set(key, []);
    children.get(key)!.push(span);
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

export function defaultGroupStrategy(span: SpanSummary): { key: string; label: string } {
  const attrs = span.span_attributes || {};

  // Prefer telemetry.event_type or event.type
  const eventType = attrs["telemetry.event_type"] || attrs["event.type"];
  if (eventType) {
    return { key: String(eventType), label: String(eventType) };
  }

  // Fallback to service name span attribute
  const serviceName = attrs["service.name"] || attrs["service_name"];
  if (serviceName) {
    const label = String(serviceName);
    return { key: label, label };
  }

  // Final fallback
  return { key: "ungrouped", label: "Ungrouped" };
}

export type GroupStrategy = (span: SpanSummary) => { key: string; label: string };

export function groupWaterfallRows(
  rows: WaterfallRow[],
  strategy: GroupStrategy = defaultGroupStrategy
): WaterfallGroup[] {
  const groupsMap = new Map<string, WaterfallGroup>();
  const groupOrder: string[] = [];

  rows.forEach((row) => {
    const { key, label } = strategy(row.span);
    if (!groupsMap.has(key)) {
      groupsMap.set(key, {
        id: key,
        label,
        rows: [],
        isExpanded: true,
        totalDurationMs: 0,
      });
      groupOrder.push(key);
    }
    const group = groupsMap.get(key)!;
    group.rows.push(row);
    group.totalDurationMs += row.span.duration_ms;
  });

  return groupOrder.map((key) => groupsMap.get(key)!);
}

/**
 * Computes the critical path through the spans.
 * Returns a Set of span_ids that are on the critical path.
 */
export function computeCriticalPath(spans: SpanSummary[]): Set<string> {
  if (spans.length === 0) return new Set();

  const byId = new Map<string, SpanSummary>();
  const children = new Map<string, string[]>();

  spans.forEach(s => {
    byId.set(s.span_id, s);
  });

  spans.forEach(s => {
    if (s.parent_span_id && byId.has(s.parent_span_id)) {
      if (!children.has(s.parent_span_id)) children.set(s.parent_span_id, []);
      children.get(s.parent_span_id)!.push(s.span_id);
    }
  });

  const memo = new Map<string, { duration: number; path: string[] }>();

  function getLongestPath(spanId: string): { duration: number; path: string[] } {
    if (memo.has(spanId)) return memo.get(spanId)!;

    const span = byId.get(spanId)!;
    const kids = children.get(spanId) || [];

    if (kids.length === 0) {
      const result = { duration: span.duration_ms, path: [spanId] };
      memo.set(spanId, result);
      return result;
    }

    let bestKidPath: { duration: number; path: string[] } = { duration: 0, path: [] };

    for (const kidId of kids) {
      const kidPath = getLongestPath(kidId);
      if (kidPath.duration > bestKidPath.duration) {
        bestKidPath = kidPath;
      }
    }

    const result = {
      duration: span.duration_ms + bestKidPath.duration,
      path: [spanId, ...bestKidPath.path]
    };
    memo.set(spanId, result);
    return result;
  }

  // Find roots
  const roots = spans.filter(s => !s.parent_span_id || !byId.has(s.parent_span_id));

  let bestRootPath: { duration: number; path: string[] } = { duration: 0, path: [] };
  for (const root of roots) {
    const rootPath = getLongestPath(root.span_id);
    if (rootPath.duration > bestRootPath.duration) {
      bestRootPath = rootPath;
    }
  }

  return new Set(bestRootPath.path);
}
