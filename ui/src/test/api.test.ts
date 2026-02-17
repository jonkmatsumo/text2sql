import { describe, it, expect, vi, beforeEach, afterEach, Mock } from "vitest";
import { generateSQL, executeSQL, OpsService } from "../api";
import { GenerateSQLRequest, ExecuteSQLRequest } from "../types";

// Mock global fetch
const globalFetch = global.fetch as Mock;

describe("API Client", () => {
    beforeEach(() => {
        global.fetch = vi.fn();
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    describe("generateSQL", () => {
        it("should call the correct endpoint with the correct payload", async () => {
            const mockResponse = { sql: "SELECT * FROM users", result: null };
            (global.fetch as Mock).mockResolvedValue({
                ok: true,
                json: async () => mockResponse,
            });

            const request: GenerateSQLRequest = {
                question: "Show users",
                tenant_id: 1,
            };

            const result = await generateSQL(request);

            expect(global.fetch).toHaveBeenCalledWith(
                expect.stringContaining("/agent/generate_sql"),
                expect.objectContaining({
                    method: "POST",
                    body: JSON.stringify(request),
                })
            );
            expect(result).toEqual(mockResponse);
        });

        it("should throw an error if the request fails", async () => {
            (global.fetch as Mock).mockResolvedValue({
                ok: false,
                status: 500,
                json: async () => ({ error: { message: "Internal Error" } }),
            });

            const request: GenerateSQLRequest = {
                question: "Show users",
                tenant_id: 1,
            };

            await expect(generateSQL(request)).rejects.toThrow("Internal Error");
        });
    });

    describe("executeSQL", () => {
        it("should call the correct endpoint with the correct payload", async () => {
            const mockResponse = { sql: "SELECT * FROM users", result: [{ id: 1 }] };
            (global.fetch as Mock).mockResolvedValue({
                ok: true,
                json: async () => mockResponse,
            });

            const request: ExecuteSQLRequest = {
                question: "Show users",
                sql: "SELECT * FROM users",
                tenant_id: 1,
            };

            const result = await executeSQL(request);

            expect(global.fetch).toHaveBeenCalledWith(
                expect.stringContaining("/agent/execute_sql"),
                expect.objectContaining({
                    method: "POST",
                    body: JSON.stringify(request),
                })
            );
            expect(result).toEqual(mockResponse);
        });
    });

    describe("OpsService", () => {

        it("cancelJob should call the correct endpoint", async () => {
            const mockResponse = { success: true };
            (global.fetch as Mock).mockResolvedValue({
                ok: true,
                json: async () => mockResponse,
            });

            const jobId = "test-job-id";
            const result = await OpsService.cancelJob(jobId);

            expect(global.fetch).toHaveBeenCalledWith(
                expect.stringContaining(`/ops/jobs/${jobId}/cancel`),
                expect.objectContaining({
                    method: "POST",
                })
            );
            expect(result).toEqual(mockResponse);
        });

        it("cancelJob should throw error on failure", async () => {
            (global.fetch as Mock).mockResolvedValue({
                ok: false,
                status: 400,
                json: async () => ({ error: { message: "Cannot cancel" } }),
            });

            await expect(OpsService.cancelJob("id")).rejects.toThrow("Cannot cancel");
        });
    });
});
