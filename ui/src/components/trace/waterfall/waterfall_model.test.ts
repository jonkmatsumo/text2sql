import { describe, expect, it } from "vitest";
import {
  buildWaterfallRows,
  computeCriticalPath,
  defaultGroupStrategy,
  groupWaterfallRows,
} from "./waterfall_model";
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

describe("waterfall_model grouping", () => {
  it("groups by event type when present", () => {
    const rows = buildWaterfallRows([
      makeSpan({
        span_id: "a",
        span_attributes: { "telemetry.event_type": "llm" },
      }),
      makeSpan({
        span_id: "b",
        span_attributes: { "event.type": "tool" },
      }),
    ]);

    const groups = groupWaterfallRows(rows);
    const labels = groups.map((g) => g.label);
    expect(labels).toContain("llm");
    expect(labels).toContain("tool");
  });

  it("falls back to service name or Ungrouped", () => {
    const serviceSpan = makeSpan({
      span_id: "svc",
      span_attributes: { "service.name": "api" },
    });
    const noServiceSpan = makeSpan({ span_id: "none", span_attributes: {} });

    expect(defaultGroupStrategy(serviceSpan)).toEqual({
      key: "api",
      label: "api",
    });
    expect(defaultGroupStrategy(noServiceSpan)).toEqual({
      key: "ungrouped",
      label: "Ungrouped",
    });
  });
});

describe("waterfall_model critical path", () => {
  it("returns the longest path in a small tree", () => {
    const spans = [
      makeSpan({ span_id: "root", duration_ms: 100 }),
      makeSpan({ span_id: "b", parent_span_id: "root", duration_ms: 50 }),
      makeSpan({ span_id: "c", parent_span_id: "root", duration_ms: 70 }),
      makeSpan({ span_id: "d", parent_span_id: "c", duration_ms: 30 }),
    ];

    const critical = computeCriticalPath(spans);
    expect(Array.from(critical)).toEqual(["root", "c", "d"]);
  });

  it("handles missing parents and single-span traces", () => {
    const spans = [
      makeSpan({ span_id: "lonely", duration_ms: 10 }),
      makeSpan({
        span_id: "orphan",
        parent_span_id: "missing-parent",
        duration_ms: 25,
      }),
    ];

    const critical = computeCriticalPath(spans);
    expect(critical.has("orphan") || critical.has("lonely")).toBe(true);

    const single = computeCriticalPath([makeSpan({ span_id: "solo" })]);
    expect(Array.from(single)).toEqual(["solo"]);
  });
});
