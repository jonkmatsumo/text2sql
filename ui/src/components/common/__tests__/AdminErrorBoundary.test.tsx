import { render, screen, fireEvent } from "@testing-library/react";
import { describe, it, expect, vi } from "vitest";
import { AdminErrorBoundary } from "../AdminErrorBoundary";

const ThrowError = () => {
    throw new Error("Test error");
};

describe("AdminErrorBoundary", () => {
    it("renders children when there is no error", () => {
        render(
            <AdminErrorBoundary>
                <div>Safe Content</div>
            </AdminErrorBoundary>
        );
        expect(screen.getByText("Safe Content")).toBeInTheDocument();
    });

    it("renders fallback UI when a child throws", () => {
        // Suppress console.error for this test as we expect an error
        const spy = vi.spyOn(console, "error").mockImplementation(() => { });

        render(
            <AdminErrorBoundary>
                <ThrowError />
            </AdminErrorBoundary>
        );

        expect(screen.getByText("Something went wrong")).toBeInTheDocument();
        expect(screen.getByText(/An unexpected error occurred in the Operator Console/i)).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Refresh Console" })).toBeInTheDocument();

        spy.mockRestore();
    });

    it("reloads the page when Refresh Console is clicked", () => {
        vi.spyOn(console, "error").mockImplementation(() => { });
        const reloadSpy = vi.fn();
        Object.defineProperty(window, "location", {
            value: { reload: reloadSpy },
            writable: true
        });

        render(
            <AdminErrorBoundary>
                <ThrowError />
            </AdminErrorBoundary>
        );

        fireEvent.click(screen.getByText("Refresh Console"));
        expect(reloadSpy).toHaveBeenCalled();
    });
});
