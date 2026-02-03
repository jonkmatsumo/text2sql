import { SpanSummary } from "../../../types";

export interface TraceGraphNode {
  id: string;
  parentId: string | null;
  name: string;
  startOffsetMs: number;
  durationMs: number;
  serviceName?: string;
  depth: number;
  isError: boolean;
  eventCount: number;
}

export interface TraceGraphEdge {
  from: string;
  to: string;
}

export interface TraceGraphModel {
  nodes: TraceGraphNode[];
  edges: TraceGraphEdge[];
  rootIds: string[];
}

function getServiceName(span: SpanSummary): string | undefined {
  const attrs = span.span_attributes || {};
  const service = attrs["service.name"] || attrs["service_name"];
  return service ? String(service) : undefined;
}

export function buildTraceGraph(spans: SpanSummary[]): TraceGraphModel {
  if (spans.length === 0) return { nodes: [], edges: [], rootIds: [] };

  const byId = new Map<string, SpanSummary>();
  spans.forEach((span) => {
    byId.set(span.span_id, span);
  });

  const startTimes = spans.map((s) => new Date(s.start_time).getTime());
  const traceStart = Math.min(...startTimes);

  const children = new Map<string, string[]>();
  const rootIds: string[] = [];
  spans.forEach((span) => {
    const parentId = span.parent_span_id;
    if (parentId && byId.has(parentId)) {
      if (!children.has(parentId)) children.set(parentId, []);
      children.get(parentId)!.push(span.span_id);
    } else {
      rootIds.push(span.span_id);
    }
  });

  const depthMap = new Map<string, number>();
  const queue: Array<{ id: string; depth: number }> = rootIds.map((id) => ({
    id,
    depth: 0
  }));
  while (queue.length) {
    const next = queue.shift()!;
    depthMap.set(next.id, next.depth);
    const kids = children.get(next.id) || [];
    kids.forEach((kid) => queue.push({ id: kid, depth: next.depth + 1 }));
  }

  const nodes: TraceGraphNode[] = spans.map((span) => {
    const startMs = new Date(span.start_time).getTime();
    const parentId = span.parent_span_id && byId.has(span.parent_span_id)
      ? span.parent_span_id
      : null;
    return {
      id: span.span_id,
      parentId,
      name: span.name,
      startOffsetMs: startMs - traceStart,
      durationMs: span.duration_ms,
      serviceName: getServiceName(span),
      depth: depthMap.get(span.span_id) ?? 0,
      isError: span.status_code === "STATUS_CODE_ERROR",
      eventCount: Array.isArray(span.events) ? span.events.length : 0
    };
  });

  const edges: TraceGraphEdge[] = [];
  nodes.forEach((node) => {
    if (node.parentId) {
      edges.push({ from: node.parentId, to: node.id });
    }
  });

  return { nodes, edges, rootIds };
}
