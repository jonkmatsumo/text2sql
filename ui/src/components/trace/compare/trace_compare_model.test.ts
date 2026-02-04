import { describe, expect, it } from "vitest";
import { alignStages, buildStageRollups } from "./trace_compare_model";
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

describe("buildStageRollups", () => {
  it("aggregates spans by stage", () => {
    const spans = [
      makeSpan({ span_id: "a", name: "llm.call", duration_ms: 100 }),
      makeSpan({ span_id: "b", name: "tool.invoke", duration_ms: 50 }),
    ];
    const rollups = buildStageRollups(spans);
    const keys = rollups.map((r) => r.key);
    expect(keys).toContain("llm");
    expect(keys).toContain("tool");
  });
});

describe("alignStages", () => {
  it("aligns stages with left-first ordering", () => {
    const left = [
      { key: "llm", label: "LLM", spanCount: 1, totalDurationMs: 10, totalSelfTimeMs: 5 }
    ];
    const right = [
      { key: "tool", label: "Tools", spanCount: 1, totalDurationMs: 20, totalSelfTimeMs: 10 }
    ];
    const aligned = alignStages(left, right);
    expect(aligned[0].key).toBe("llm");
    expect(aligned[1].key).toBe("tool");
  });
});
