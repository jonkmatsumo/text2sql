import { describe, it, expect } from "vitest";
import {
  bandScale,
  getNumericExtent,
  getTimeExtent,
  linearScale
} from "../scales";

describe("scales", () => {
  it("returns null extent for empty numeric values", () => {
    expect(getNumericExtent([])).toBeNull();
  });

  it("ignores nulls when computing numeric extent", () => {
    expect(getNumericExtent([null, 2, 5, undefined])).toEqual([2, 5]);
  });

  it("returns null extent when only invalid numbers provided", () => {
    expect(getNumericExtent([null, undefined, Number.NaN])).toBeNull();
  });

  it("computes time extent from ISO timestamps", () => {
    const start = "2024-01-01T00:00:00Z";
    const end = "2024-01-01T00:10:00Z";
    expect(getTimeExtent([start, end])).toEqual([
      Date.parse(start),
      Date.parse(end)
    ]);
  });

  it("creates a band scale for categorical domains", () => {
    const scale = bandScale(["A", "B", "C"], [0, 120], 0.2);
    expect(scale.bandwidth).toBeGreaterThan(0);
    expect(scale("A")).toBeLessThan(scale("B"));
    expect(scale.domain).toEqual(["A", "B", "C"]);
  });

  it("returns mid-point for zero-span linear scale", () => {
    const scale = linearScale([1, 1], [0, 100]);
    expect(scale(1)).toBe(50);
  });
});
