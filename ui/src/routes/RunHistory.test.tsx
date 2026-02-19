import { render, screen, waitFor, fireEvent, act } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { MemoryRouter, useLocation } from "react-router-dom";
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

describe("RunHistory search scope messaging", () => {
    let showToastMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        vi.clearAllMocks();
        showToastMock = vi.fn();
        vi.spyOn(OpsService, "listRuns").mockResolvedValue({ runs: mockRuns } as any);
        vi.spyOn(useToastHook, "useToast").mockReturnValue({ show: showToastMock } as any);
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
            expect(screen.getByText("No matches found on this page. Try Next to search older runs.")).toBeInTheDocument();
        });

        expect(screen.getByTestId("runhistory-empty-search-scope-note")).toHaveTextContent(
            /Search is limited to this page/i
        );
        expect(screen.getByTestId("runhistory-empty-search-scope-note")).toHaveTextContent(
            /Results only include runs already loaded/i
        );
    });

    it("shows page-scoped disclaimer and inline label when query is set", async () => {
        renderRunHistory("/admin/runs?q=top");

        await waitFor(() => {
            expect(screen.getByLabelText("Search runs by query or ID")).toHaveValue("top");
        });
        const disclaimer = screen.getByTestId("runhistory-search-scope-note");
        expect(disclaimer).toHaveTextContent(/Search is limited to this page/i);
        expect(disclaimer).toHaveTextContent(/Results only include runs already loaded/i);
        expect(screen.getByTestId("runhistory-search-scope-inline-label")).toHaveTextContent(
            "Search is limited to this page."
        );
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

    it("clears search and resets offset when Clear is clicked", async () => {
        renderRunHistory("/admin/runs?offset=100&q=missing");

        const clearSearchButton = await screen.findByRole("button", { name: "Clear search query" });
        fireEvent.click(clearSearchButton);

        await waitFor(() => {
            expect(screen.getByLabelText("Search runs by query or ID")).toHaveValue("");
        });
        expect(screen.getByTestId("location-search")).not.toHaveTextContent("q=");
        expect(screen.getByTestId("location-search")).not.toHaveTextContent("offset=");
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

    it("closes shortcuts modal before clearing filters when Escape is pressed", async () => {
        renderRunHistory("/admin/runs?q=missing&status=FAILED");

        await screen.findByRole("button", { name: "Clear all filters" });
        fireEvent.click(screen.getByRole("button", { name: "Show keyboard shortcuts" }));
        expect(screen.getByRole("dialog", { name: "Keyboard shortcuts" })).toBeInTheDocument();

        await act(async () => {
            fireEvent.keyDown(window, { key: "Escape" });
        });

        await waitFor(() => {
            expect(screen.queryByRole("dialog", { name: "Keyboard shortcuts" })).not.toBeInTheDocument();
        });
        expect(screen.getByLabelText("Search runs by query or ID")).toHaveValue("missing");
        expect(screen.getByRole("button", { name: "Clear all filters" })).toBeInTheDocument();
    });

    it("shows deterministic empty-page range copy at offset 0", async () => {
        (OpsService.listRuns as any).mockResolvedValueOnce({ runs: [] });
        renderRunHistory("/admin/runs?offset=0");

        await waitFor(() => {
            expect(screen.getByText("No results on this page")).toBeInTheDocument();
        });
    });

    it("renders Details links with whitespace-free run details routes", async () => {
        renderRunHistory();

        const detailsLink = await screen.findByRole("link", { name: "View details for run run-1" });
        expect(detailsLink).toHaveAttribute("href", "/admin/runs/run-1");
    });

    it("keeps Next disabled when page is empty at non-zero offset even if has_more=true", async () => {
        (OpsService.listRuns as any).mockResolvedValueOnce({ runs: [], has_more: true });
        renderRunHistory("/admin/runs?offset=100");

        await waitFor(() => {
            expect(screen.getByText("No results on this page")).toBeInTheDocument();
        });

        expect(screen.getByRole("button", { name: "Next page" })).toBeDisabled();
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
                surface: "RunHistory.listRuns",
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

    it("recovers offset deterministically for high-offset empty pages and shows one warning toast", async () => {
        (OpsService.listRuns as any)
            .mockResolvedValueOnce({ runs: [], has_more: false })
            .mockResolvedValueOnce({ runs: buildRuns(3), has_more: false });

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
});
