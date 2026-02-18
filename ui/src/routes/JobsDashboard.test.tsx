import { render, screen, waitFor, fireEvent, act } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { MemoryRouter } from "react-router-dom";
import JobsDashboard from "./JobsDashboard";
import { OpsService } from "../api";
import * as useConfirmationHook from "../hooks/useConfirmation";
import * as useToastHook from "../hooks/useToast";

const mockJobs = [
    {
        id: "job-1",
        job_type: "SCHEMA_HYDRATION",
        status: "RUNNING",
        started_at: new Date().toISOString(),
        finished_at: null,
        error_message: null,
        result: {}
    }
];

describe("JobsDashboard Cancellation", () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.spyOn(OpsService, "listJobs").mockResolvedValue(mockJobs as any);
        vi.spyOn(useToastHook, "useToast").mockReturnValue({ show: vi.fn() } as any);
        vi.spyOn(useConfirmationHook, "useConfirmation").mockReturnValue({
            confirm: vi.fn().mockResolvedValue(true),
            dialogProps: { isOpen: false, onConfirm: vi.fn(), onClose: vi.fn() }
        } as any);
    });

    it("triggers cancellation and shows optimistic update", async () => {
        let resolveCancel: (value: any) => void;
        const cancelPromise = new Promise((resolve) => {
            resolveCancel = resolve;
        });
        vi.spyOn(OpsService, "cancelJob").mockReturnValue(cancelPromise as any);

        render(
            <MemoryRouter>
                <JobsDashboard />
            </MemoryRouter>
        );

        await screen.findByText("SCHEMA_HYDRATION", {}, { timeout: 2000 });

        const cancelButton = screen.getByRole("button", { name: /cancel/i });
        fireEvent.click(cancelButton);

        // Verify optimistic update (status should change to CANCELLING)
        expect(await screen.findByText("CANCELLING")).toBeInTheDocument();

        // Resolve
        await act(async () => {
            resolveCancel!({ success: true });
        });

        expect(OpsService.cancelJob).toHaveBeenCalledWith("job-1");
    });

    it("prevents duplicate cancellation requests", async () => {
        const confirmMock = vi.fn().mockResolvedValue(true);
        vi.spyOn(useConfirmationHook, "useConfirmation").mockReturnValue({
            confirm: confirmMock,
            dialogProps: { isOpen: false, onConfirm: vi.fn(), onClose: vi.fn() }
        } as any);

        render(
            <MemoryRouter>
                <JobsDashboard />
            </MemoryRouter>
        );

        await screen.findByText("SCHEMA_HYDRATION");

        const cancelButton = screen.getByRole("button", { name: /cancel/i });

        // Trigger multiple clicks
        fireEvent.click(cancelButton);
        fireEvent.click(cancelButton);

        await waitFor(() => {
            expect(confirmMock).toHaveBeenCalledTimes(1);
        });
    });
});
