import { render, screen, waitFor, fireEvent } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { MemoryRouter } from "react-router-dom";
import RunHistory from "./RunHistory";
import { OpsService } from "../api";
import * as useToastHook from "../hooks/useToast";

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

    it("renders the page-scoped search disclaimer near the search input", async () => {
        renderRunHistory();

        await waitFor(() => {
            expect(OpsService.listRuns).toHaveBeenCalledTimes(1);
        });

        expect(screen.getByTestId("runhistory-search-scope-note")).toHaveTextContent(
            "Search currently filters the loaded page only. Use pagination to search older runs."
        );
    });

    it("mentions page-scoped search in empty state when query yields no loaded-page match", async () => {
        renderRunHistory("/admin/runs?q=missing");

        await waitFor(() => {
            expect(screen.getByText("No runs matched your search on this loaded page.")).toBeInTheDocument();
        });

        expect(screen.getByTestId("runhistory-empty-search-scope-note")).toHaveTextContent(
            "Search currently filters the loaded page only. Use pagination to search older runs."
        );
    });

    it("does not clear all filters when Escape is pressed in the search input", async () => {
        renderRunHistory("/admin/runs?q=missing&status=FAILED");

        const searchInput = await screen.findByLabelText("Search runs by query or ID");
        searchInput.focus();
        fireEvent.keyDown(window, { key: "Escape" });

        expect(searchInput).toHaveValue("missing");
        expect(searchInput).not.toHaveFocus();
        expect(screen.getByRole("button", { name: "Clear all filters" })).toBeInTheDocument();
    });

    it("clears all filters when Escape is pressed outside input focus", async () => {
        renderRunHistory("/admin/runs?q=missing&status=FAILED");

        const searchInput = await screen.findByLabelText("Search runs by query or ID");
        searchInput.blur();
        fireEvent.keyDown(window, { key: "Escape" });

        await waitFor(() => {
            expect(searchInput).toHaveValue("");
        });
        expect(screen.queryByRole("button", { name: "Clear all filters" })).not.toBeInTheDocument();
    });
});
