import { describe, expect, it } from "vitest";
import { buildTraceGraph } from "./trace_graph_model";
import { SpanSummary } from "../../../types";

function makeSpan(partial: Partial<SpanSummary> & { span_id: string }): SpanSummary {
  const base: SpanSummary = {
    span_id: partial.span_id,
    trace_id: partial.trace_id ?? "trace-1",
    parent_span_id: partial.parent_span_id ?? null,
    name: partial.name ?? partial.span_id,
    kind: partial.kind ?? "INTERNAL",
    status_code: partial.status_code ?? "STATUS_CODE_OK",
    status_message: partial.status_message ?? null,
    start_time: partial.start_time ?? new Date("2024-01-01T00:00:00Z").toISOString(),
    end_time: partial.end_time ?? new Date("2024-01-01T00:00:01Z").toISOString(),
    duration_ms: partial.duration_ms ?? 1,
    span_attributes: partial.span_attributes ?? {},
    events: partial.events ?? [],
  };

  return base;
}

describe("buildTraceGraph", () => {
  it("builds nodes, edges, and roots with depths", () => {
    const spans = [
      makeSpan({ span_id: "root", start_time: "2024-01-01T00:00:00Z" }),
      makeSpan({ span_id: "child", parent_span_id: "root", start_time: "2024-01-01T00:00:00.100Z" }),
      makeSpan({ span_id: "orphan", parent_span_id: "missing" }),
    ];

    const graph = buildTraceGraph(spans);
    const rootNode = graph.nodes.find((n) => n.id === "root");
    const childNode = graph.nodes.find((n) => n.id === "child");
    const orphanNode = graph.nodes.find((n) => n.id === "orphan");

    expect(graph.rootIds).toEqual(expect.arrayContaining(["root", "orphan"]));
    expect(graph.edges).toEqual([{ from: "root", to: "child" }]);
    expect(rootNode?.depth).toBe(0);
    expect(childNode?.depth).toBe(1);
    expect(orphanNode?.parentId).toBe(null);
  });

  it("computes start offsets and error flags", () => {
    const spans = [
      makeSpan({ span_id: "a", start_time: "2024-01-01T00:00:00Z" }),
      makeSpan({
        span_id: "b",
        start_time: "2024-01-01T00:00:00.050Z",
        status_code: "STATUS_CODE_ERROR",
        events: [{ name: "evt" }]
      }),
    ];

    const graph = buildTraceGraph(spans);
    const nodeB = graph.nodes.find((n) => n.id === "b")!;
    expect(nodeB.startOffsetMs).toBe(50);
    expect(nodeB.isError).toBe(true);
    expect(nodeB.eventCount).toBe(1);
  });
});
