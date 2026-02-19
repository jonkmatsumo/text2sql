import { describe, it, expect } from "vitest";
import { buildRunContextBundle } from "../buildRunContextBundle";

describe("buildRunContextBundle", () => {
    it("includes bundle version header", () => {
        const result = buildRunContextBundle({ runId: "abc-123" });
        expect(result).toContain("Bundle-Version: 1");
    });

    it("includes run ID when provided", () => {
        const result = buildRunContextBundle({ runId: "abc-123" });
        expect(result).toContain("Run ID          : abc-123");
    });

    it("includes trace ID when provided", () => {
        const result = buildRunContextBundle({ traceId: "trace-xyz" });
        expect(result).toContain("Trace ID        : trace-xyz");
    });

    it("includes request ID when provided", () => {
        const result = buildRunContextBundle({ requestId: "req-456" });
        expect(result).toContain("Request ID      : req-456");
    });

    it("includes execution status", () => {
        const result = buildRunContextBundle({ executionStatus: "SUCCESS" });
        expect(result).toContain("Execution Status: SUCCESS");
    });

    it("includes completeness flag as yes/no", () => {
        expect(buildRunContextBundle({ isComplete: true })).toContain("Complete        : yes");
        expect(buildRunContextBundle({ isComplete: false })).toContain("Complete        : no");
    });

    it("includes user query section", () => {
        const result = buildRunContextBundle({ userQuery: "Show me sales by region" });
        expect(result).toContain("--- User Query ---");
        expect(result).toContain("Show me sales by region");
    });

    it("includes generated SQL section", () => {
        const result = buildRunContextBundle({ generatedSql: "SELECT region, SUM(sales) FROM orders GROUP BY region" });
        expect(result).toContain("--- Generated SQL ---");
        expect(result).toContain("SELECT region");
    });

    it("includes validation section with status and errors", () => {
        const result = buildRunContextBundle({
            validationStatus: "FAILED",
            validationErrors: ["Column 'region' not found", "Table 'orders' does not exist"],
        });
        expect(result).toContain("--- Validation ---");
        expect(result).toContain("Status: FAILED");
        expect(result).toContain("Column 'region' not found");
        expect(result).toContain("Table 'orders' does not exist");
    });

    it("omits validation errors section when errors array is empty", () => {
        const result = buildRunContextBundle({ validationStatus: "PASSED", validationErrors: [] });
        expect(result).toContain("Status: PASSED");
        expect(result).not.toContain("Errors:");
    });

    it("handles fully partial payload (no fields)", () => {
        const result = buildRunContextBundle({});
        expect(result).toContain("=== Run Context Bundle ===");
        expect(result).toContain("Generated-At    :");
        expect(result).not.toContain("Run ID:");
        expect(result).not.toContain("Trace ID:");
    });

    it("includes generation timestamp", () => {
        const before = new Date().toISOString().slice(0, 16); // YYYY-MM-DDTHH:MM
        const result = buildRunContextBundle({ runId: "test" });
        expect(result).toContain("Generated-At    :");
        expect(result).toContain(before);
    });

    it("includes environment header", () => {
        const result = buildRunContextBundle({ environment: "production" });
        expect(result).toContain("Environment     : production");
    });

    it("produces full payload with all fields", () => {
        const result = buildRunContextBundle({
            runId: "run-1",
            traceId: "trace-1",
            requestId: "req-1",
            userQuery: "test query",
            generatedSql: "SELECT 1",
            validationStatus: "PASSED",
            executionStatus: "SUCCESS",
            isComplete: true,
        });
        expect(result).toContain("run-1");
        expect(result).toContain("trace-1");
        expect(result).toContain("req-1");
        expect(result).toContain("test query");
        expect(result).toContain("SELECT 1");
        expect(result).toContain("PASSED");
        expect(result).toContain("SUCCESS");
        expect(result).toContain("yes");
    });
});
