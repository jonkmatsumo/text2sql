import { describe, it, expect } from "vitest";

/**
 * Unit tests for the URL canonicalization logic used in RunHistory.
 * Tests the pure logic extracted from updateFilters.
 */

/** Mirrors the updateFilters logic from RunHistory */
function canonicalizeParams(
    current: Record<string, string>,
    updates: Record<string, string | number | undefined>,
    isOffsetUpdate = false
): URLSearchParams {
    const merged: Record<string, string> = { ...current };

    Object.entries(updates).forEach(([key, value]) => {
        if (value === undefined || value === "" || value === "All" || value === 0) {
            delete merged[key];
        } else {
            merged[key] = String(value);
        }
    });

    if (!isOffsetUpdate) {
        delete merged["offset"];
    }

    const canonical = new URLSearchParams();
    Object.keys(merged).sort().forEach(k => canonical.set(k, merged[k]));
    return canonical;
}

describe("RunHistory URL canonicalization", () => {
    it("omits status=All from URL", () => {
        const result = canonicalizeParams({}, { status: "All" });
        expect(result.has("status")).toBe(false);
    });

    it("omits feedback=All from URL", () => {
        const result = canonicalizeParams({}, { feedback: "All" });
        expect(result.has("feedback")).toBe(false);
    });

    it("omits offset=0 from URL", () => {
        const result = canonicalizeParams({}, { offset: 0 }, true);
        expect(result.has("offset")).toBe(false);
    });

    it("omits empty string values", () => {
        const result = canonicalizeParams({}, { q: "" });
        expect(result.has("q")).toBe(false);
    });

    it("includes non-default status", () => {
        const result = canonicalizeParams({}, { status: "FAILED" });
        expect(result.get("status")).toBe("FAILED");
    });

    it("includes non-default feedback", () => {
        const result = canonicalizeParams({}, { feedback: "DOWN" });
        expect(result.get("feedback")).toBe("DOWN");
    });

    it("includes non-zero offset", () => {
        const result = canonicalizeParams({}, { offset: 50 }, true);
        expect(result.get("offset")).toBe("50");
    });

    it("writes params in deterministic alphabetical order", () => {
        const result = canonicalizeParams({}, { status: "FAILED", q: "test", feedback: "DOWN" });
        const keys = [...result.keys()];
        expect(keys).toEqual([...keys].sort());
    });

    it("resets offset when filter changes (not offset update)", () => {
        const result = canonicalizeParams({ offset: "50", status: "SUCCESS" }, { status: "FAILED" }, false);
        expect(result.has("offset")).toBe(false);
        expect(result.get("status")).toBe("FAILED");
    });

    it("preserves offset when explicitly updating offset", () => {
        const result = canonicalizeParams({ status: "FAILED" }, { offset: 100 }, true);
        expect(result.get("offset")).toBe("100");
        expect(result.get("status")).toBe("FAILED");
    });

    it("produces stable output for same inputs", () => {
        const r1 = canonicalizeParams({}, { status: "FAILED", q: "test" });
        const r2 = canonicalizeParams({}, { q: "test", status: "FAILED" });
        expect(r1.toString()).toBe(r2.toString());
    });
});
