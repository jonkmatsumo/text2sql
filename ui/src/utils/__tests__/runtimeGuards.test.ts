import { describe, it, expect } from "vitest";
import { isInteractionArray, isJobStatusResponse, isOpsJobResponseArray, isRunDiagnosticsResponse, buildContractMismatchReport } from "../runtimeGuards";

describe("runtimeGuards", () => {
    describe("isInteractionArray", () => {
        it("returns true for valid Interaction array", () => {
            const valid = [{ id: "1", execution_status: "SUCCESS", user_nlq_text: "test", generated_sql: "SELECT 1" }];
            expect(isInteractionArray(valid)).toBe(true);
        });

        it("returns true for empty array", () => {
            expect(isInteractionArray([])).toBe(true);
        });

        it("returns false for non-array", () => {
            expect(isInteractionArray({})).toBe(false);
            expect(isInteractionArray(null)).toBe(false);
        });

        it("returns false if elements miss required keys", () => {
            expect(isInteractionArray([{ id: "1" }])).toBe(false);
            expect(isInteractionArray([{ execution_status: "SUCCESS" }])).toBe(false);
        });
    });

    describe("isJobStatusResponse", () => {
        it("returns true for valid job status response", () => {
            expect(isJobStatusResponse({
                id: "job1",
                status: "RUNNING",
                job_type: "REINDEX",
                started_at: new Date().toISOString()
            })).toBe(true);
        });

        it("returns false for invalid status", () => {
            expect(isJobStatusResponse({ id: "job1", status: "INVALID" })).toBe(false);
        });

        it("returns false for missing keys", () => {
            expect(isJobStatusResponse({ id: "job1" })).toBe(false);
            expect(isJobStatusResponse({ status: "COMPLETED" })).toBe(false);
        });
    });

    describe("isRunDiagnosticsResponse", () => {
        it("returns true for valid diagnostics response", () => {
            expect(isRunDiagnosticsResponse({
                diagnostics_schema_version: 1,
                enabled_flags: {}
            })).toBe(true);
        });

        it("returns false if diagnostics_schema_version is missing or wrong type", () => {
            expect(isRunDiagnosticsResponse({ enabled_flags: {} })).toBe(false);
            expect(isRunDiagnosticsResponse({ diagnostics_schema_version: "1", enabled_flags: {} })).toBe(false);
        });

        it("returns false if enabled_flags is missing", () => {
            expect(isRunDiagnosticsResponse({ diagnostics_schema_version: 1 })).toBe(false);
        });
    });

    describe("isOpsJobResponseArray", () => {
        it("returns true for valid jobs array", () => {
            expect(isOpsJobResponseArray([
                {
                    id: "job-1",
                    job_type: "SCHEMA_HYDRATION",
                    status: "RUNNING",
                    started_at: new Date().toISOString(),
                },
            ])).toBe(true);
        });

        it("returns true for an empty jobs array", () => {
            expect(isOpsJobResponseArray([])).toBe(true);
        });

        it("returns false for malformed jobs array", () => {
            expect(isOpsJobResponseArray([{ id: "job-1" }])).toBe(false);
            expect(isOpsJobResponseArray({ items: [] })).toBe(false);
        });
    });

    describe("buildContractMismatchReport", () => {
        it("populates report with context and preview", () => {
            const malformed = { some: "garbage", request_id: "req-123" };
            const context = { jobId: "job-abc" };
            const report = buildContractMismatchReport("TestSurface", malformed, context);

            expect(report.surface).toBe("TestSurface");
            expect(report.ids.request_id).toBe("req-123");
            expect(report.request_context).toEqual(context);
            expect(report.response_preview).toContain("garbage");
        });

        it("handles null context", () => {
            const malformed = { id: "123" };
            const report = buildContractMismatchReport("TestSurface", malformed);
            expect(report.request_context).toBeUndefined();
            expect(report.response_preview).toContain("123");
        });
    });
});
