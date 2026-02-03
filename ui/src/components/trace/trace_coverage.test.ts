import { describe, expect, it } from "vitest";
import { computeSpanCoverage } from "./trace_coverage";

describe("computeSpanCoverage", () => {
  it("computes coverage when total is known", () => {
    const result = computeSpanCoverage(50, 200, 5000);
    expect(result.totalKnown).toBe(true);
    expect(result.coveragePct).toBe(25);
    expect(result.reachedMaxLimit).toBe(false);
  });

  it("handles unknown totals", () => {
    const result = computeSpanCoverage(50, null, 5000);
    expect(result.totalKnown).toBe(false);
    expect(result.coveragePct).toBe(null);
  });

  it("flags reaching the max limit", () => {
    const result = computeSpanCoverage(5000, 12000, 5000);
    expect(result.reachedMaxLimit).toBe(true);
    expect(result.coveragePct).toBe(42);
  });
});
