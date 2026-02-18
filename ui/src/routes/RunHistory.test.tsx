import { render, screen, waitFor, fireEvent, act } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { MemoryRouter } from "react-router-dom";
import RunHistory from "./RunHistory";
import { OpsService } from "../api";
import * as useToastHook from "../hooks/useToast";
import { RUN_HISTORY_PAGE_SIZE } from "../constants/operatorUi";

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
        vi.spyOn(OpsService, "listRuns").mockResolvedValue(mockRuns as any);
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
            "Search filters the current page. Use Next/Prev to search older runs."
        );
    });

    it("shows the page-scoped disclaimer while the search input is focused", async () => {
        renderRunHistory();
        const searchInput = await screen.findByLabelText("Search runs by query or ID");

        fireEvent.focus(searchInput);
        expect(screen.getByTestId("runhistory-search-scope-note")).toHaveTextContent(
            "Search filters the current page. Use Next/Prev to search older runs."
        );

        fireEvent.blur(searchInput);
        await waitFor(() => {
            expect(screen.queryByTestId("runhistory-search-scope-note")).not.toBeInTheDocument();
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

    it("shows 'No results' instead of a range when page is empty with non-zero offset", async () => {
        (OpsService.listRuns as any).mockResolvedValueOnce([]);
        renderRunHistory("/admin/runs?offset=100");

        await waitFor(() => {
            expect(screen.getByText("No results")).toBeInTheDocument();
        });

        expect(screen.queryByText(/Showing results/i)).not.toBeInTheDocument();
    });

    it("disables Next and shows tooltip when page-scoped search active and results < page size", async () => {
        (OpsService.listRuns as any).mockResolvedValueOnce(buildRuns(5)); // fewer than limit
        renderRunHistory("/admin/runs?q=test");

        const nextButton = await screen.findByRole("button", { name: "Next page" });
        expect(nextButton).toBeDisabled();
        expect(nextButton).toHaveAttribute("title", "Search filters the current page. Use Next/Prev to search older runs.");
    });
});
