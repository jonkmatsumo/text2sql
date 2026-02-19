import { render, screen, waitFor, fireEvent, act, within } from "@testing-library/react";
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
    let showToastMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        vi.setConfig({ testTimeout: 15000 });
        vi.clearAllMocks();
        vi.spyOn(OpsService, "listJobs").mockResolvedValue(mockJobs as any);
        showToastMock = vi.fn();
        vi.spyOn(useToastHook, "useToast").mockReturnValue({ show: showToastMock } as any);
        vi.spyOn(useConfirmationHook, "useConfirmation").mockReturnValue({
            confirm: vi.fn().mockResolvedValue(true),
            dialogProps: { isOpen: false, onConfirm: vi.fn(), onClose: vi.fn() }
        } as any);
    });

    it("blurs focused filters on first Escape, then clears filters on second Escape", async () => {
        render(
            <MemoryRouter>
                <JobsDashboard />
            </MemoryRouter>
        );

        await screen.findByText("SCHEMA_HYDRATION");

        const typeFilter = screen.getByDisplayValue("All Types");
        const statusFilter = screen.getByDisplayValue("All Statuses");

        fireEvent.change(typeFilter, { target: { value: "SCHEMA_HYDRATION" } });
        fireEvent.change(statusFilter, { target: { value: "RUNNING" } });

        expect(typeFilter).toHaveValue("SCHEMA_HYDRATION");
        expect(statusFilter).toHaveValue("RUNNING");

        typeFilter.focus();
        expect(typeFilter).toHaveFocus();

        fireEvent.keyDown(window, { key: "Escape" });
        expect(typeFilter).not.toHaveFocus();
        expect(typeFilter).toHaveValue("SCHEMA_HYDRATION");
        expect(statusFilter).toHaveValue("RUNNING");

        fireEvent.keyDown(window, { key: "Escape" });

        await waitFor(() => {
            expect(typeFilter).toHaveValue("");
            expect(statusFilter).toHaveValue("");
        });
    });

    it("closes keyboard shortcuts modal before clearing filters on Escape", async () => {
        render(
            <MemoryRouter>
                <JobsDashboard />
            </MemoryRouter>
        );

        await screen.findByText("SCHEMA_HYDRATION");

        const typeFilter = screen.getByDisplayValue("All Types");
        const statusFilter = screen.getByDisplayValue("All Statuses");
        fireEvent.change(typeFilter, { target: { value: "SCHEMA_HYDRATION" } });
        fireEvent.change(statusFilter, { target: { value: "RUNNING" } });

        fireEvent.click(screen.getByRole("button", { name: "Show keyboard shortcuts" }));
        expect(screen.getByRole("dialog", { name: "Keyboard shortcuts" })).toBeInTheDocument();

        fireEvent.keyDown(window, { key: "Escape" });
        await waitFor(() => {
            expect(screen.queryByRole("dialog", { name: "Keyboard shortcuts" })).not.toBeInTheDocument();
        });

        expect(typeFilter).toHaveValue("SCHEMA_HYDRATION");
        expect(statusFilter).toHaveValue("RUNNING");
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
        const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => { });

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

    it("shows a single timeout warning and refresh affordance when cancel polling times out", async () => {
        vi.spyOn(OpsService, "cancelJob").mockResolvedValue({ success: true } as any);
        vi.spyOn(OpsService, "getJobStatus").mockResolvedValue({ ...mockJobs[0], status: "RUNNING" } as any);

        try {
            render(
                <MemoryRouter>
                    <JobsDashboard />
                </MemoryRouter>
            );

            await screen.findByText("SCHEMA_HYDRATION");
            vi.useFakeTimers();
            const cancelButton = screen.getByRole("button", { name: /cancel/i });
            fireEvent.click(cancelButton);
            fireEvent.click(cancelButton);

            await act(async () => {
                await Promise.resolve();
            });

            for (let i = 0; i < 10; i += 1) {
                await act(async () => {
                    vi.advanceTimersByTime(2000);
                    await Promise.resolve();
                });
            }

            const timeoutWarningToasts = showToastMock.mock.calls.filter(
                ([message, type]) =>
                    message.includes("timed out") && message.includes("Refresh list to re-check") && type === "warning"
            );
            expect(timeoutWarningToasts).toHaveLength(1);

            const timeoutBanner = screen.getByTestId("cancel-timeout-refresh-banner");
            expect(timeoutBanner).toHaveTextContent("Status check timed out —");
            expect(within(timeoutBanner).getByRole("button", { name: "Refresh list" })).toBeInTheDocument();

            const listJobsCallsBeforeRefresh = (OpsService.listJobs as any).mock.calls.length;
            fireEvent.click(within(timeoutBanner).getByRole("button", { name: "Refresh list" }));
            await act(async () => {
                await Promise.resolve();
            });
            expect((OpsService.listJobs as any).mock.calls.length).toBeGreaterThan(listJobsCallsBeforeRefresh);
        } finally {
            vi.useRealTimers();
        }
    });

    it("prevents overlapping pollJobUntilTerminal executions per job", async () => {
        vi.spyOn(OpsService, "cancelJob").mockResolvedValue({} as any);
        vi.spyOn(OpsService, "getJobStatus").mockResolvedValue({ ...mockJobs[0], status: "RUNNING" } as any);

        render(
            <MemoryRouter>
                <JobsDashboard />
            </MemoryRouter>
        );

        await screen.findByText("SCHEMA_HYDRATION");
        const cancelButton = screen.getByRole("button", { name: /cancel/i });

        // Double click
        await act(async () => {
            fireEvent.click(cancelButton);
            fireEvent.click(cancelButton);
        });

        // Ensure it didn't call cancelJob twice
        expect(OpsService.cancelJob).toHaveBeenCalledTimes(1);
    }, 10000);

    it("reconciles cancellation state after manual refresh reveals terminal status", async () => {
        let resolveCancel: (value: any) => void;
        const cancelPromise = new Promise((resolve) => { resolveCancel = resolve; });
        vi.spyOn(OpsService, "cancelJob").mockReturnValue(cancelPromise as any);
        const listJobsSpy = vi.spyOn(OpsService, "listJobs");

        // Initial state is RUNNING, then COMPLETED after refresh
        listJobsSpy.mockResolvedValueOnce(mockJobs as any);
        const terminalJobs = [{ ...mockJobs[0], status: "COMPLETED" }];
        listJobsSpy.mockResolvedValue(terminalJobs as any);

        render(
            <MemoryRouter>
                <JobsDashboard />
            </MemoryRouter>
        );

        await screen.findByText("SCHEMA_HYDRATION");
        const cancelButton = screen.getByRole("button", { name: /cancel/i });

        await act(async () => {
            fireEvent.click(cancelButton);
        });

        // Wait for optimistic state
        expect(await screen.findByText("CANCELLING")).toBeInTheDocument();

        // Simulate manual refresh where backend now says COMPLETED
        const refreshButton = screen.getByRole("button", { name: /^refresh$/i });
        await act(async () => {
            fireEvent.click(refreshButton);
        });

        // Should call showToast for terminal status
        await waitFor(() => {
            expect(showToastMock).toHaveBeenCalledWith(
                expect.stringContaining("reached terminal state: COMPLETED"),
                "success"
            );
        });
        expect(screen.queryByText("CANCELLING")).not.toBeInTheDocument();

        // Cleanup
        await act(async () => {
            resolveCancel!({ success: true });
        });
    }, 10000);
});
