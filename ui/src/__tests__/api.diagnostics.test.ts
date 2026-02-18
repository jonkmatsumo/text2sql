import { describe, it, expect, vi, beforeEach, afterEach, Mock } from "vitest";
import { getDiagnostics } from "../api";
import type { RunDiagnosticsResponse } from "../types/diagnostics";

describe("getDiagnostics", () => {
    beforeEach(() => {
        global.fetch = vi.fn();
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it("calls the diagnostics endpoint with debug=false by default", async () => {
        const mockResponse = { diagnostics_schema_version: 1, retry_policy: {} };
        (global.fetch as Mock).mockResolvedValue({
            ok: true,
            json: async () => mockResponse,
        });

        const result = await getDiagnostics();

        expect(global.fetch).toHaveBeenCalledWith(
            expect.stringContaining("/agent/diagnostics"),
            expect.objectContaining({
                headers: expect.objectContaining({
                    "Content-Type": "application/json",
                }),
            })
        );
        const url = (global.fetch as Mock).mock.calls[0][0];
        expect(url).not.toContain("debug=true");
        expect(result).toEqual(mockResponse);
    });

    it("calls the diagnostics endpoint with debug=true when requested", async () => {
        const mockResponse = { diagnostics_schema_version: 1, debug: { latency: {} } };
        (global.fetch as Mock).mockResolvedValue({
            ok: true,
            json: async () => mockResponse,
        });

        await getDiagnostics(true);

        const url = (global.fetch as Mock).mock.calls[0][0];
        expect(url).toContain("debug=true");
    });

    it("returns run diagnostics fields when audit_run_id is provided", async () => {
        const mockResponse: RunDiagnosticsResponse = {
            diagnostics_schema_version: 1,
            retry_policy: { mode: "none", max_retries: 0 },
            schema_cache_ttl_seconds: 300,
            runtime_indicators: {
                active_schema_cache_size: 0,
                last_schema_refresh_timestamp: null,
                avg_query_complexity: 0,
                recent_truncation_event_count: 0,
            },
            enabled_flags: {
                schema_binding_validation: true,
                schema_binding_soft_mode: false,
                column_allowlist_mode: "strict",
                column_allowlist_from_schema_context: true,
                cartesian_join_mode: "warn",
                capability_fallback_mode: "enabled",
                provider_cap_mitigation: "enabled",
                decision_summary_debug: true,
                disable_prefetch: false,
                disable_schema_refresh: false,
                disable_llm_retries: false,
            },
            run_context: {
                execution_status: "SUCCESS",
                user_nlq_text: "top customers",
            },
            validation: {
                ast_valid: true,
                syntax_errors: [],
            },
            completeness: {
                is_truncated: false,
            },
            generated_sql: "SELECT 1",
            audit_events: [{ decision: "Generated SQL" }],
        };

        (global.fetch as Mock).mockResolvedValue({
            ok: true,
            json: async () => mockResponse,
        });

        const result = await getDiagnostics(true, "run-123");

        const url = (global.fetch as Mock).mock.calls[0][0];
        expect(url).toContain("audit_run_id=run-123");
        expect(result.run_context?.execution_status).toBe("SUCCESS");

        const typedResult: RunDiagnosticsResponse = result;
        expect(typedResult.generated_sql).toBe("SELECT 1");
    });

    it("throws ApiError when response is not ok", async () => {
        (global.fetch as Mock).mockResolvedValue({
            ok: false,
            status: 403,
            json: async () => ({
                error: { message: "Forbidden", code: "AUTH_ERROR" }
            }),
        });

        await expect(getDiagnostics()).rejects.toThrow("Forbidden");
    });
});
