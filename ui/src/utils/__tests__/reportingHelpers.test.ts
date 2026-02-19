import { describe, it, expect } from "vitest";
import { extractIdentifiers, summarizeUnexpectedResponse } from "../runtimeGuards";

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
        expect(summary).toContain("[truncated]");
        expect(summary.length).toBeLessThan(largeObj.data.length);
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
