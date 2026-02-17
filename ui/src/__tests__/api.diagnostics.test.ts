import { describe, it, expect, vi, beforeEach, afterEach, Mock } from "vitest";
import { getDiagnostics } from "../api";

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
