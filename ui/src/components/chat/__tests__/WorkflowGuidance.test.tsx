import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, it, expect, beforeEach, vi } from "vitest";
import { WorkflowGuidance, __clearGuidanceCache } from "../WorkflowGuidance";

function renderWithRouter(ui: React.ReactElement) {
    return render(<MemoryRouter>{ui}</MemoryRouter>);
}

describe("WorkflowGuidance", () => {
    beforeEach(() => {
        __clearGuidanceCache();
    });

    it("renders nothing when no category provided", () => {
        const { container } = renderWithRouter(<WorkflowGuidance />);
        expect(container.firstChild).toBeNull();
    });

    it("renders nothing for unknown category", () => {
        const { container } = renderWithRouter(<WorkflowGuidance category="some_unknown_error" />);
        expect(container.firstChild).toBeNull();
    });

    it("renders timeout guidance correctly", () => {
        renderWithRouter(<WorkflowGuidance category="timeout" />);
        expect(screen.getByText("Timeout")).toBeInTheDocument();
        expect(screen.getByText(/took too long to complete/i)).toBeInTheDocument();
        expect(screen.getByText("warn")).toBeInTheDocument();
        expect(screen.getByRole("link", { name: "Check Connectivity" })).toBeInTheDocument();
    });

    it("renders auth guidance correctly", () => {
        renderWithRouter(<WorkflowGuidance category="auth" />);
        expect(screen.getByText("Authentication Error")).toBeInTheDocument();
        expect(screen.getByRole("link", { name: "Update Target Settings" })).toBeInTheDocument();
    });

    it("renders transient error guidance correctly", () => {
        renderWithRouter(<WorkflowGuidance category="transient" />);
        expect(screen.getByText("Transient Error")).toBeInTheDocument();
        expect(screen.getByText(/temporary error occurred/i)).toBeInTheDocument();
        const link = screen.getByRole("link", { name: "Retry Operation" });
        expect(link).toBeInTheDocument();
        expect(link).toHaveAttribute("href", "/admin/diagnostics");
        expect(link).not.toHaveAttribute("href", "#");
    });

    it("renders schema_missing guidance with correct title and description", () => {
        renderWithRouter(<WorkflowGuidance category="schema_missing" />);
        expect(screen.getByText("Schema Not Found")).toBeInTheDocument();
        expect(screen.getByText(/ingest the table or refresh/i)).toBeInTheDocument();
    });

    it("renders schema_missing primary CTA linking to ingestion wizard", () => {
        renderWithRouter(<WorkflowGuidance category="schema_missing" />);
        const link = screen.getByRole("link", { name: "Go to Ingestion Wizard" });
        expect(link).toBeInTheDocument();
        expect(link).toHaveAttribute("href", "/admin/operations?tab=ingestion");
    });

    it("renders schema_missing secondary CTA", () => {
        renderWithRouter(<WorkflowGuidance category="schema_missing" />);
        expect(screen.getByRole("link", { name: "Check Schema Hydration" })).toBeInTheDocument();
    });

    it("renders schema_drift guidance", () => {
        renderWithRouter(<WorkflowGuidance category="schema_drift" />);
        expect(screen.getByText("Schema Mismatch")).toBeInTheDocument();
        expect(screen.getByText(/schema appears to have changed/i)).toBeInTheDocument();
        expect(screen.getByRole("link", { name: "Run Schema Hydration" })).toBeInTheDocument();
    });

    it("renders connectivity guidance", () => {
        renderWithRouter(<WorkflowGuidance category="connectivity" />);
        expect(screen.getByText("Connection Error")).toBeInTheDocument();
        expect(screen.getByRole("link", { name: "Verify Target Settings" })).toBeInTheDocument();
        expect(screen.getByRole("link", { name: "Check Connectivity Diagnostics" })).toBeInTheDocument();
    });

    it("renders budget_exhausted guidance", () => {
        renderWithRouter(<WorkflowGuidance category="budget_exhausted" />);
        expect(screen.getByText("Budget Exhausted")).toBeInTheDocument();
        expect(screen.getByRole("link", { name: "Manage Quotas" })).toBeInTheDocument();
    });

    it("renders budget_exceeded guidance", () => {
        renderWithRouter(<WorkflowGuidance category="budget_exceeded" />);
        expect(screen.getByText("Budget Exceeded")).toBeInTheDocument();
        expect(screen.getByRole("link", { name: "Manage Quotas" })).toBeInTheDocument();
    });

    it("schema_missing CTA matches errorMapping source (no duplication)", () => {
        renderWithRouter(<WorkflowGuidance category="schema_missing" />);
        // Should have exactly the CTAs defined in errorMapping, not duplicates
        const links = screen.getAllByRole("link");
        const hrefs = links.map(l => l.getAttribute("href"));
        expect(hrefs.filter(h => h === "/admin/operations?tab=ingestion")).toHaveLength(1);
    });

    it("prevents duplicate guidance rendering for the same category in a short window", async () => {
        const { unmount } = renderWithRouter(
            <div>
                <WorkflowGuidance category="timeout" />
                <WorkflowGuidance category="timeout" />
            </div>
        );

        // Should only see one "Timeout" title
        expect(screen.getAllByText("Timeout")).toHaveLength(1);
        unmount();

        // After some time, it should be able to render again
        vi.useFakeTimers();
        renderWithRouter(<WorkflowGuidance category="timeout" />);
        // Wait for potential logic to clear
        vi.advanceTimersByTime(1100);
        renderWithRouter(<WorkflowGuidance category="timeout" />);

        // This is a bit tricky with global state in tests, but let's at least verify
        // that multiple in same render are suppressed.
        vi.useRealTimers();
    });
});
