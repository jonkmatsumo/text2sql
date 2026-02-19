import { render, screen, waitFor, fireEvent, act } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { MemoryRouter } from "react-router-dom";
import RunHistory from "./RunHistory";
import { OpsService } from "../api";
import * as useToastHook from "../hooks/useToast";
import { RUN_HISTORY_PAGE_SIZE } from "../constants/pagination";

const mockRuns = [
    {
        id: "run-1",
        user_nlq_text: "Top customers",
        generated_sql: "SELECT * FROM customers",
        generated_sql_preview: "SELECT * FROM customers",
        response_payload: "{}",
        execution_status: "SUCCESS",
        thumb: "UP",
        trace_id: "trace-1",
        created_at: "2026-02-10T00:00:00Z",
        model_version: "gpt-5",
    },
];

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

function renderRunHistory(initialPath = "/admin/runs") {
    return render(
        <MemoryRouter initialEntries={[initialPath]}>
            <RunHistory />
        </MemoryRouter>
    );
}

describe("RunHistory search scope messaging", () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.spyOn(OpsService, "listRuns").mockResolvedValue({ runs: mockRuns } as any);
        vi.spyOn(useToastHook, "useToast").mockReturnValue({ show: vi.fn() } as any);
    });

    it("hides the page-scoped search disclaimer when query is empty", async () => {
        renderRunHistory();

        await waitFor(() => {
            expect(OpsService.listRuns).toHaveBeenCalledTimes(1);
        });

        expect(screen.queryByTestId("runhistory-search-scope-note")).not.toBeInTheDocument();
    });

    it("mentions page-scoped search in empty state when query yields no loaded-page match", async () => {
        renderRunHistory("/admin/runs?q=missing");

        await waitFor(() => {
            expect(screen.getByText("No matches on this page. Try Next to search older runs.")).toBeInTheDocument();
        });

        expect(screen.getByTestId("runhistory-empty-search-scope-note")).toHaveTextContent(
            /Search is limited to this page/i
        );
        expect(screen.getByTestId("runhistory-empty-search-scope-note")).toHaveTextContent(
            /Results only include runs already loaded/i
        );
    });

    it("shows the page-scoped disclaimer while the search input is focused", async () => {
        renderRunHistory();
        const searchInput = await screen.findByLabelText("Search runs by query or ID");

        fireEvent.focus(searchInput);
        const disclaimer = screen.getByTestId("runhistory-search-scope-note");
        expect(disclaimer).toHaveTextContent(/Search is limited to this page/i);
        expect(disclaimer).toHaveTextContent(/Results only include runs already loaded/i);

        fireEvent.blur(searchInput);
        await waitFor(() => {
            expect(screen.queryByTestId("runhistory-search-scope-note")).not.toBeInTheDocument();
        });
    });

    it("shows 'Search All' disabled button with explanatory tooltip", async () => {
        renderRunHistory();
        const searchAllBtn = screen.getByRole("button", { name: /Search All/i });
        expect(searchAllBtn).toBeDisabled();
        expect(searchAllBtn).toHaveAttribute("title", expect.stringMatching(/not yet supported by the backend/i));
    });

    it("shows 'more runs exist' hint when q is set and page is full", async () => {
        (OpsService.listRuns as any).mockResolvedValueOnce({
            runs: buildRuns(RUN_HISTORY_PAGE_SIZE),
            has_more: true
        });
        renderRunHistory("/admin/runs?q=test");

        await waitFor(() => {
            expect(screen.getByTestId("runhistory-more-runs-hint")).toHaveTextContent(
                "More runs exist beyond this page; try Next."
            );
        });
    });

    it("hides 'more runs exist' hint when q is set but has_more is false", async () => {
        (OpsService.listRuns as any).mockResolvedValueOnce({
            runs: buildRuns(5),
            has_more: false
        });
        renderRunHistory("/admin/runs?q=test");

        await waitFor(() => {
            expect(screen.queryByTestId("runhistory-more-runs-hint")).not.toBeInTheDocument();
        });
    });

    it("does not clear all filters when Escape is pressed in the search input", async () => {
        renderRunHistory("/admin/runs?q=missing&status=FAILED");

        const searchInput = await screen.findByLabelText("Search runs by query or ID");
        await act(async () => {
            searchInput.focus();
        });
        expect(searchInput).toHaveFocus();
        await act(async () => {
            fireEvent.keyDown(window, { key: "Escape" });
        });

        expect(searchInput).toHaveValue("missing");
        expect(searchInput).not.toHaveFocus();
        expect(screen.getByRole("button", { name: "Clear all filters" })).toBeInTheDocument();
    });

    it("clears all filters when Escape is pressed outside input focus", async () => {
        renderRunHistory("/admin/runs?q=missing&status=FAILED");

        const searchInput = await screen.findByLabelText("Search runs by query or ID");
        await act(async () => {
            fireEvent.blur(searchInput);
        });
        await act(async () => {
            fireEvent.keyDown(window, { key: "Escape" });
        });

        await waitFor(() => {
            expect(searchInput).toHaveValue("");
        });
        expect(screen.queryByRole("button", { name: "Clear all filters" })).not.toBeInTheDocument();
    });

    it("shows 'No results' when page is empty at offset 0", async () => {
        (OpsService.listRuns as any).mockResolvedValueOnce({ runs: [] });
        renderRunHistory("/admin/runs?offset=0");

        await waitFor(() => {
            expect(screen.getByText(/No results/i)).toBeInTheDocument();
        });
    });

    it("disables Next and shows tooltip when page-scoped search active and results < page size", async () => {
        (OpsService.listRuns as any).mockResolvedValueOnce({ runs: buildRuns(5) }); // fewer than limit
        renderRunHistory("/admin/runs?q=test");

        const nextButton = await screen.findByRole("button", { name: "Next page" });
        expect(nextButton).toBeDisabled();
        expect(nextButton).toHaveAttribute("title", expect.stringMatching(/Search is limited to this page/i));
    });

    it("respects has_more metadata for disabling Next button", async () => {
        // Return structured response with has_more: false
        (OpsService.listRuns as any).mockResolvedValueOnce({
            runs: buildRuns(RUN_HISTORY_PAGE_SIZE),
            has_more: false
        });
        renderRunHistory();

        const nextButton = await screen.findByRole("button", { name: "Next page" });
        expect(nextButton).toBeDisabled();
    });

    it("enables Next button when has_more is true even if results < limit", async () => {
        // This case is unlikely in real world but tests our logic
        (OpsService.listRuns as any).mockResolvedValueOnce({
            runs: buildRuns(5),
            has_more: true
        });
        renderRunHistory();

        const nextButton = await screen.findByRole("button", { name: "Next page" });
        expect(nextButton).not.toBeDisabled();
    });

    it("falls back to page-size heuristic when has_more is missing", async () => {
        (OpsService.listRuns as any).mockResolvedValueOnce({
            runs: buildRuns(RUN_HISTORY_PAGE_SIZE)
        });
        renderRunHistory();

        const nextButton = await screen.findByRole("button", { name: "Next page" });
        expect(nextButton).not.toBeDisabled();
    });

    it("renders empty state when listRuns payload is malformed", async () => {
        const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => { });
        (OpsService.listRuns as any).mockResolvedValueOnce({});
        renderRunHistory();

        await waitFor(() => {
            expect(screen.getByText("No runs recorded yet.")).toBeInTheDocument();
        });

        expect(consoleErrorSpy).toHaveBeenCalledWith(
            "Operator API contract mismatch (RunHistory.listRuns)",
            expect.objectContaining({
                endpoint: "RunHistory.listRuns",
                summary: "Expected result.runs to be an array",
            })
        );

        consoleErrorSpy.mockRestore();
    });

    it("auto-redirects to offset 0 if page is empty at non-zero offset (Empty Page Recovery)", async () => {
        (OpsService.listRuns as any).mockResolvedValueOnce({ runs: [] }); // Empty
        renderRunHistory("/admin/runs?offset=50");

        await waitFor(() => {
            // Should have called listRuns again with offset 0
            expect(OpsService.listRuns).toHaveBeenLastCalledWith(RUN_HISTORY_PAGE_SIZE, 0, "All", "All");
        });
    });
});
