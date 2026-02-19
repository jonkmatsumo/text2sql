import { describe, it, expect } from "vitest";
import { isInteractionArray, isJobStatusResponse, isRunDiagnosticsResponse } from "../runtimeGuards";

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
});
