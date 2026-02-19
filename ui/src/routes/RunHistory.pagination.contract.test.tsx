import { render, screen, waitFor } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { MemoryRouter, useLocation } from "react-router-dom";
import RunHistory from "./RunHistory";
import { ApiError, OpsService } from "../api";
import * as useToastHook from "../hooks/useToast";
import { RUN_HISTORY_PAGE_SIZE } from "../constants/pagination";

function buildRuns(count: number) {
    return Array.from({ length: count }, (_, index) => ({
        id: `run-${index + 1}`,
        user_nlq_text: `Query ${index + 1}`,
        generated_sql: "SELECT 1",
        generated_sql_preview: "SELECT 1",
        response_payload: "{}",
        execution_status: "SUCCESS",
        thumb: "UP",
        trace_id: `trace-${index + 1}`,
        created_at: "2026-02-10T00:00:00Z",
        model_version: "gpt-5",
    }));
}

function LocationProbe() {
    const location = useLocation();
    return <div data-testid="location-search">{location.search}</div>;
}

function renderRunHistory(initialPath = "/admin/runs") {
    return render(
        <MemoryRouter initialEntries={[initialPath]}>
            <RunHistory />
            <LocationProbe />
        </MemoryRouter>
    );
}

describe("RunHistory pagination contract guards", () => {
    let showToastMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        vi.clearAllMocks();
        showToastMock = vi.fn();
        vi.spyOn(useToastHook, "useToast").mockReturnValue({ show: showToastMock } as any);
    });

    it("surfaces malformed listRuns payloads as fetch errors", async () => {
        vi.spyOn(OpsService, "listRuns").mockRejectedValueOnce(
            new ApiError("Received unexpected response from listRuns", 200, "MALFORMED_RESPONSE")
        );

        renderRunHistory();

        await waitFor(() => {
            expect(screen.getByTestId("runhistory-load-error")).toHaveTextContent(
                "Could not load run history. Refresh to retry."
            );
        });
        expect(screen.queryByText("No runs recorded yet.")).not.toBeInTheDocument();
        expect(showToastMock).toHaveBeenCalledWith(
            "Received unexpected response from listRuns",
            "error",
            expect.objectContaining({
                dedupeKey: expect.stringContaining("MALFORMED_RESPONSE"),
            })
        );
    });

    it("does not silently convert malformed listRuns responses into empty-state copy", async () => {
        vi.spyOn(OpsService, "listRuns").mockRejectedValueOnce(
            new ApiError("Received unexpected response from listRuns", 200, "MALFORMED_RESPONSE")
        );

        renderRunHistory();

        await waitFor(() => {
            expect(screen.getByTestId("runhistory-load-error")).toHaveTextContent(
                "Could not load run history. Refresh to retry."
            );
        });
        expect(screen.queryByText("No runs recorded yet.")).not.toBeInTheDocument();
    });

    it("falls back to page-size heuristic when has_more is undefined", async () => {
        vi.spyOn(OpsService, "listRuns").mockResolvedValueOnce({
            runs: buildRuns(RUN_HISTORY_PAGE_SIZE),
            has_more: undefined,
        } as any);

        renderRunHistory();

        const nextButton = await screen.findByRole("button", { name: "Next page" });
        expect(nextButton).not.toBeDisabled();
    });

    it("enables Next when has_more is true even for short pages", async () => {
        vi.spyOn(OpsService, "listRuns").mockResolvedValueOnce({
            runs: buildRuns(5),
            has_more: true,
        } as any);

        renderRunHistory();

        const nextButton = await screen.findByRole("button", { name: "Next page" });
        expect(nextButton).not.toBeDisabled();
    });

    it("disables Next when has_more is false even for full pages", async () => {
        vi.spyOn(OpsService, "listRuns").mockResolvedValueOnce({
            runs: buildRuns(RUN_HISTORY_PAGE_SIZE),
            has_more: false,
        } as any);

        renderRunHistory();

        const nextButton = await screen.findByRole("button", { name: "Next page" });
        expect(nextButton).toBeDisabled();
    });

    it("recovers non-zero offset when page is empty and has_more is false", async () => {
        vi.spyOn(OpsService, "listRuns")
            .mockResolvedValueOnce({ runs: [], has_more: false } as any)
            .mockResolvedValueOnce({ runs: buildRuns(2), has_more: false } as any);

        renderRunHistory("/admin/runs?offset=100");

        await waitFor(() => {
            expect(OpsService.listRuns).toHaveBeenCalledTimes(2);
        });
        expect((OpsService.listRuns as any).mock.calls[0]).toEqual([RUN_HISTORY_PAGE_SIZE, 100, "All", "All"]);
        expect((OpsService.listRuns as any).mock.calls[1]).toEqual([RUN_HISTORY_PAGE_SIZE, 50, "All", "All"]);
        await waitFor(() => {
            expect(screen.getByTestId("location-search")).toHaveTextContent("offset=50");
        });

        expect(showToastMock).toHaveBeenCalledWith(
            "Requested page is out of range. Showing previous results.",
            "warning"
        );
        expect(showToastMock).toHaveBeenCalledTimes(1);
    });

    it("attempts a single recovery step when has_more is true but non-zero page is empty", async () => {
        vi.spyOn(OpsService, "listRuns")
            .mockResolvedValueOnce({ runs: [], has_more: true } as any)
            .mockResolvedValueOnce({ runs: [], has_more: true } as any);

        renderRunHistory("/admin/runs?offset=100");

        await waitFor(() => {
            expect(OpsService.listRuns).toHaveBeenCalledTimes(2);
        });
        expect((OpsService.listRuns as any).mock.calls[0]).toEqual([RUN_HISTORY_PAGE_SIZE, 100, "All", "All"]);
        expect((OpsService.listRuns as any).mock.calls[1]).toEqual([RUN_HISTORY_PAGE_SIZE, 50, "All", "All"]);

        await waitFor(() => {
            expect(screen.getByTestId("location-search")).toHaveTextContent("offset=50");
        });
        expect(screen.getByText("No results on this page")).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Next page" })).toBeDisabled();
        expect(showToastMock).toHaveBeenCalledTimes(1);
    });

    it("does not attempt recovery when offset is 0 even if has_more is true and page is empty", async () => {
        vi.spyOn(OpsService, "listRuns")
            .mockResolvedValueOnce({ runs: [], has_more: true } as any);

        renderRunHistory("/admin/runs?offset=0");

        await waitFor(() => {
            expect(OpsService.listRuns).toHaveBeenCalledTimes(1);
        });
        expect(screen.getByText("No results on this page")).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Next page" })).toBeDisabled();
        expect(showToastMock).not.toHaveBeenCalled();
    });

    it("renders total_count in pagination summary when available", async () => {
        vi.spyOn(OpsService, "listRuns").mockResolvedValueOnce({
            runs: buildRuns(3),
            total_count: 120,
        } as any);

        renderRunHistory("/admin/runs?offset=50");

        await waitFor(() => {
            expect(screen.getByText("Showing 51–53 of 120")).toBeInTheDocument();
        });
    });
});
