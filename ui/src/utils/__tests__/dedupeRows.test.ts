import { describe, it, expect } from "vitest";
import { dedupeRows } from "../dedupeRows";

describe("dedupeRows", () => {
  it("removes exact duplicate rows", () => {
    const existing = [{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }];
    const incoming = [{ id: 2, name: "Bob" }, { id: 3, name: "Carol" }];
    const result = dedupeRows(existing, incoming);
    expect(result).toEqual([{ id: 3, name: "Carol" }]);
  });

  it("returns all rows when no duplicates", () => {
    const existing = [{ id: 1 }];
    const incoming = [{ id: 2 }, { id: 3 }];
    expect(dedupeRows(existing, incoming)).toEqual([{ id: 2 }, { id: 3 }]);
  });

  it("returns empty when all are duplicates", () => {
    const rows = [{ a: 1 }, { a: 2 }];
    expect(dedupeRows(rows, rows)).toEqual([]);
  });

  it("handles column order differences", () => {
    const existing = [{ name: "Alice", id: 1 }];
    const incoming = [{ id: 1, name: "Alice" }];
    expect(dedupeRows(existing, incoming)).toEqual([]);
  });

  it("handles empty arrays", () => {
    expect(dedupeRows([], [{ id: 1 }])).toEqual([{ id: 1 }]);
    expect(dedupeRows([{ id: 1 }], [])).toEqual([]);
  });
});
