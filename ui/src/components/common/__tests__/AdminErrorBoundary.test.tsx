import { render, screen, fireEvent } from "@testing-library/react";
import { describe, it, expect, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { AdminErrorBoundary } from "../AdminErrorBoundary";

const ThrowError = () => {
    throw new Error("Test error");
};

describe("AdminErrorBoundary", () => {
    it("renders children when there is no error", () => {
        render(
            <MemoryRouter>
                <AdminErrorBoundary>
                    <div>Safe Content</div>
                </AdminErrorBoundary>
            </MemoryRouter>
        );
        expect(screen.getByText("Safe Content")).toBeInTheDocument();
    });

    it("renders fallback UI when a child throws", () => {
        const spy = vi.spyOn(console, "error").mockImplementation(() => { });

        render(
            <MemoryRouter>
                <AdminErrorBoundary>
                    <ThrowError />
                </AdminErrorBoundary>
            </MemoryRouter>
        );

        expect(screen.getAllByRole("heading", { name: /something went wrong/i }).length).toBeGreaterThan(0);
        expect(screen.getAllByRole("link", { name: /back to runs/i }).length).toBeGreaterThan(0);
        expect(screen.getAllByText("Technical Details").length).toBeGreaterThan(0);
        expect(screen.getAllByText(/Test error/i).length).toBeGreaterThan(0);

        spy.mockRestore();
    });

    it("reloads the page when Refresh Console is clicked", () => {
        vi.spyOn(console, "error").mockImplementation(() => { });
        const reloadSpy = vi.fn();
        Object.defineProperty(window, "location", {
            value: { reload: reloadSpy },
            configurable: true
        });

        render(
            <MemoryRouter>
                <AdminErrorBoundary>
                    <ThrowError />
                </AdminErrorBoundary>
            </MemoryRouter>
        );

        fireEvent.click(screen.queryAllByText("Refresh Console")[0]);
        expect(reloadSpy).toHaveBeenCalled();
    });

    it("resets when its key changes", () => {
        vi.spyOn(console, "error").mockImplementation(() => { });

        const { rerender } = render(
            <MemoryRouter initialEntries={["/admin/runs"]}>
                <AdminErrorBoundary key="/admin/runs">
                    <ThrowError />
                </AdminErrorBoundary>
            </MemoryRouter>
        );

        expect(screen.getAllByRole("heading", { name: /something went wrong/i }).length).toBeGreaterThan(0);

        rerender(
            <MemoryRouter initialEntries={["/admin/jobs"]}>
                <AdminErrorBoundary key="/admin/jobs">
                    <div>New Safe Content</div>
                </AdminErrorBoundary>
            </MemoryRouter>
        );

        expect(screen.queryByRole("heading", { name: /something went wrong/i })).not.toBeInTheDocument();
        expect(screen.getByText("New Safe Content")).toBeInTheDocument();
    });
});
