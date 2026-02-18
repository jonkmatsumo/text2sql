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

    it("keeps cancel state unchanged until confirmation is accepted", async () => {
        let resolveConfirm: (value: boolean) => void;
        const confirmPromise = new Promise<boolean>((resolve) => {
            resolveConfirm = resolve;
        });
        const confirmMock = vi.fn().mockReturnValue(confirmPromise);
        vi.spyOn(useConfirmationHook, "useConfirmation").mockReturnValue({
            confirm: confirmMock,
            dialogProps: { isOpen: false, onConfirm: vi.fn(), onClose: vi.fn() }
        } as any);

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

        await screen.findByText("SCHEMA_HYDRATION");
        fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

        expect(confirmMock).toHaveBeenCalledTimes(1);
        expect(screen.getByRole("button", { name: "Cancel" })).toBeInTheDocument();
        expect(screen.queryByText("CANCELLING")).not.toBeInTheDocument();

        await act(async () => {
            resolveConfirm!(true);
            await Promise.resolve();
        });

        expect(await screen.findByText("CANCELLING")).toBeInTheDocument();

        await act(async () => {
            resolveCancel!({ success: true });
        });
    });

    it("stops cancel-status polling once the job reaches a terminal state", async () => {
        const getJobStatusMock = vi
            .spyOn(OpsService, "getJobStatus")
            .mockResolvedValue({ ...mockJobs[0], status: "FAILED" } as any);
        vi.spyOn(OpsService, "cancelJob").mockResolvedValue({ success: true } as any);

        try {
            render(
                <MemoryRouter>
                    <JobsDashboard />
                </MemoryRouter>
            );

            await screen.findByText("SCHEMA_HYDRATION");
            vi.useFakeTimers();
            fireEvent.click(screen.getByRole("button", { name: /cancel/i }));

            await act(async () => {
                await Promise.resolve();
            });
            expect(OpsService.cancelJob).toHaveBeenCalledWith("job-1");

            await act(async () => {
                vi.advanceTimersByTime(2000);
                await Promise.resolve();
            });

            expect(getJobStatusMock).toHaveBeenCalledTimes(1);

            await act(async () => {
                vi.advanceTimersByTime(8000);
                await Promise.resolve();
            });

            expect(getJobStatusMock).toHaveBeenCalledTimes(1);
        } finally {
            vi.useRealTimers();
        }
    });

    it("does not trigger unmounted state update warnings during in-flight cancel", async () => {
        let resolveCancel: (value: any) => void;
        const cancelPromise = new Promise((resolve) => {
            resolveCancel = resolve;
        });
        vi.spyOn(OpsService, "cancelJob").mockReturnValue(cancelPromise as any);
        const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

        try {
            const { unmount } = render(
                <MemoryRouter>
                    <JobsDashboard />
                </MemoryRouter>
            );

            await screen.findByText("SCHEMA_HYDRATION");
            fireEvent.click(screen.getByRole("button", { name: /cancel/i }));
            unmount();

            await act(async () => {
                resolveCancel!({ success: true });
                await Promise.resolve();
            });

            const hadUnmountWarning = consoleErrorSpy.mock.calls.some((call) =>
                call.join(" ").includes("state update on an unmounted component")
            );
            expect(hadUnmountWarning).toBe(false);
        } finally {
            consoleErrorSpy.mockRestore();
        }
    });

    it("clears the cancel polling interval on component teardown", async () => {
        vi.spyOn(OpsService, "cancelJob").mockResolvedValue({ success: true } as any);
        vi.spyOn(OpsService, "getJobStatus").mockResolvedValue({ ...mockJobs[0], status: "RUNNING" } as any);
        const clearIntervalSpy = vi.spyOn(window, "clearInterval");

        try {
            const { unmount } = render(
                <MemoryRouter>
                    <JobsDashboard />
                </MemoryRouter>
            );

            await screen.findByText("SCHEMA_HYDRATION");
            vi.useFakeTimers();
            fireEvent.click(screen.getByRole("button", { name: /cancel/i }));

            await act(async () => {
                await Promise.resolve();
            });

            await act(async () => {
                vi.advanceTimersByTime(2000);
                await Promise.resolve();
            });

            unmount();
            expect(clearIntervalSpy).toHaveBeenCalled();
        } finally {
            vi.useRealTimers();
            clearIntervalSpy.mockRestore();
        }
    });
});
