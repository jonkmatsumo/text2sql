import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { MemoryRouter, Routes, Route } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";
import RunHistory from "../RunHistory";
import RunDetails from "../RunDetails";
import { useToast } from "../../hooks/useToast";

vi.mock("../../hooks/useToast", () => ({
    useToast: vi.fn(),
}));

vi.mock("../../api", () => ({
    OpsService: {
        listRuns: vi.fn().mockResolvedValue({ runs: [], has_more: false }),
    },
    getDiagnostics: vi.fn().mockResolvedValue({}),
    getErrorMessage: (e: any) => e.message,
    ApiError: class extends Error {
        code: string;
        constructor(m: string, c: string) {
            super(m);
            this.code = c;
        }
    },
}));

describe("Clipboard failure handling", () => {
    const showToast = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
        (useToast as any).mockReturnValue({ show: showToast });

        // Mock clipboard API with rejection
        Object.assign(navigator, {
            clipboard: {
                writeText: vi.fn().mockRejectedValue(new Error("Clip error")),
            },
        });
    });

    it("RunHistory copyLink shows error toast on clipboard rejection", async () => {
        render(
            <MemoryRouter>
                <RunHistory />
            </MemoryRouter>
        );

        const copyBtn = await screen.findByRole("button", { name: /copy link/i });
        fireEvent.click(copyBtn);

        await waitFor(() => {
            expect(showToast).toHaveBeenCalledWith("Could not copy to clipboard", "error");
        });

        // Ensure "Copied" text isn't shown
        expect(screen.queryByText(/Copied!/i)).toBeNull();
    });

    it("RunDetails copyRunContext shows error toast on clipboard rejection", async () => {
        render(
            <MemoryRouter initialEntries={["/admin/runs/abc"]}>
                <Routes>
                    <Route path="/admin/runs/:runId" element={<RunDetails />} />
                </Routes>
            </MemoryRouter>
        );

        const copyBtn = await screen.findByRole("button", { name: /copy run context/i });
        fireEvent.click(copyBtn);

        await waitFor(() => {
            expect(showToast).toHaveBeenCalledWith("Could not copy to clipboard", "error");
        });

        expect(screen.queryByText(/✓ Copied!/i)).toBeNull();
    });
});
