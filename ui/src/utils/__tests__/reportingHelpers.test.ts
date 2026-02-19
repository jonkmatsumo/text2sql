import { describe, it, expect } from "vitest";
import { buildContractMismatchReport, extractIdentifiers, summarizeUnexpectedResponse } from "../runtimeGuards";

describe("extractIdentifiers", () => {
    it("extracts common IDs from an object", () => {
        const payload = {
            id: "123",
            trace_id: "trace-abc",
            other: "field"
        };
        expect(extractIdentifiers(payload)).toEqual({
            id: "123",
            trace_id: "trace-abc"
        });
    });

    it("returns empty object for null or non-objects", () => {
        expect(extractIdentifiers(null)).toEqual({});
        expect(extractIdentifiers("string")).toEqual({});
        expect(extractIdentifiers([])).toEqual({});
    });

    it("extracts nothing if no target keys are present", () => {
        expect(extractIdentifiers({ foo: "bar" })).toEqual({});
    });
});

describe("summarizeUnexpectedResponse", () => {
    it("serializes small objects completely", () => {
        const obj = { active: true, count: 5 };
        const summary = summarizeUnexpectedResponse(obj);
        expect(JSON.parse(summary)).toEqual(obj);
    });

    it("truncates large objects", () => {
        const largeObj = { data: "x".repeat(100) };
        const summary = summarizeUnexpectedResponse(largeObj, 10);
        expect(summary).toContain("...");
        expect(summary).toContain("[truncated, total_size=");
        expect(summary.length).toBeLessThan(largeObj.data.length);
    });

    it("bounds large arrays with a stable marker", () => {
        const summary = summarizeUnexpectedResponse({ items: [1, 2, 3, 4, 5] }, { maxArrayItems: 2, maxChars: 1000 });
        expect(summary).toContain('"items"');
        expect(summary).toContain("[+3 more items]");
    });

    it("handles circular references", () => {
        const a: any = { name: "a" };
        const b: any = { name: "b" };
        a.child = b;
        b.parent = a;

        const summary = summarizeUnexpectedResponse(a);
        expect(summary).toContain("[Circular]");
    });

    it("handles null and undefined", () => {
        expect(summarizeUnexpectedResponse(null)).toBe("null");
        expect(summarizeUnexpectedResponse(undefined)).toBe("undefined");
    });
});

describe("buildContractMismatchReport", () => {
    it("includes surface, identifiers, and bounded summary", () => {
        const report = buildContractMismatchReport("OpsService.listRuns", {
            trace_id: "trace-1",
            runs: [{ id: "run-1" }, { id: "run-2" }, { id: "run-3" }],
        }, { maxArrayItems: 1, maxChars: 1000 });

        expect(report.surface).toBe("OpsService.listRuns");
        expect(report.ids).toEqual({ trace_id: "trace-1" });
        expect(report.summary).toContain("[+2 more items]");
    });
});
