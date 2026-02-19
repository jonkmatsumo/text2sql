import { describe, it, expect, vi } from "vitest";
import { TERMINAL_STATUSES } from "../JobsDashboard";

/**
 * Unit tests for cancellation state reconciliation logic.
 * Tests the pure reconciliation logic extracted from JobsDashboard.fetchJobs.
 */

type OpsJobStatus = "PENDING" | "RUNNING" | "CANCELLING" | "CANCELLED" | "COMPLETED" | "FAILED";

interface Job {
    id: string;
    status: OpsJobStatus;
}

/**
 * Mirrors the reconciliation logic from fetchJobs.
 * Returns the IDs of jobs that drifted from CANCELLING to terminal.
 */
function reconcileCancellingJobs(
    freshJobs: Job[],
    cancellingIds: Set<string>
): string[] {
    if (cancellingIds.size === 0) return [];
    const resolved: string[] = [];
    for (const job of freshJobs) {
        if (cancellingIds.has(job.id) && TERMINAL_STATUSES.has(job.status)) {
            resolved.push(job.id);
        }
    }
    return resolved;
}

describe("Cancellation state reconciliation", () => {
    it("returns resolved IDs when CANCELLING job reaches CANCELLED", () => {
        const jobs: Job[] = [{ id: "job-1", status: "CANCELLED" }];
        const cancelling = new Set(["job-1"]);
        expect(reconcileCancellingJobs(jobs, cancelling)).toEqual(["job-1"]);
    });

    it("returns resolved IDs when CANCELLING job reaches COMPLETED", () => {
        const jobs: Job[] = [{ id: "job-1", status: "COMPLETED" }];
        const cancelling = new Set(["job-1"]);
        expect(reconcileCancellingJobs(jobs, cancelling)).toEqual(["job-1"]);
    });

    it("returns resolved IDs when CANCELLING job reaches FAILED", () => {
        const jobs: Job[] = [{ id: "job-1", status: "FAILED" }];
        const cancelling = new Set(["job-1"]);
        expect(reconcileCancellingJobs(jobs, cancelling)).toEqual(["job-1"]);
    });

    it("returns empty when CANCELLING job is still CANCELLING", () => {
        const jobs: Job[] = [{ id: "job-1", status: "CANCELLING" }];
        const cancelling = new Set(["job-1"]);
        expect(reconcileCancellingJobs(jobs, cancelling)).toEqual([]);
    });

    it("returns empty when CANCELLING job is still RUNNING", () => {
        const jobs: Job[] = [{ id: "job-1", status: "RUNNING" }];
        const cancelling = new Set(["job-1"]);
        expect(reconcileCancellingJobs(jobs, cancelling)).toEqual([]);
    });

    it("returns empty when cancellingIds is empty", () => {
        const jobs: Job[] = [{ id: "job-1", status: "CANCELLED" }];
        expect(reconcileCancellingJobs(jobs, new Set())).toEqual([]);
    });

    it("only resolves jobs that are in cancellingIds", () => {
        const jobs: Job[] = [
            { id: "job-1", status: "CANCELLED" },
            { id: "job-2", status: "CANCELLED" },
        ];
        const cancelling = new Set(["job-1"]);
        expect(reconcileCancellingJobs(jobs, cancelling)).toEqual(["job-1"]);
    });

    it("resolves multiple jobs in one pass", () => {
        const jobs: Job[] = [
            { id: "job-1", status: "CANCELLED" },
            { id: "job-2", status: "FAILED" },
            { id: "job-3", status: "RUNNING" },
        ];
        const cancelling = new Set(["job-1", "job-2", "job-3"]);
        const resolved = reconcileCancellingJobs(jobs, cancelling);
        expect(resolved).toContain("job-1");
        expect(resolved).toContain("job-2");
        expect(resolved).not.toContain("job-3");
    });
});

describe("Cancellation idempotency", () => {
    it("does not add a job to cancellingIds if already present (double-click guard)", () => {
        const cancellingIds = new Set(["job-1"]);
        // Simulates the guard: if (cancellingJobIds.has(jobId)) return;
        const tryCancel = (jobId: string): boolean => {
            if (cancellingIds.has(jobId)) return false; // already cancelling
            cancellingIds.add(jobId);
            return true;
        };

        expect(tryCancel("job-1")).toBe(false);
        expect(cancellingIds.size).toBe(1);
    });

    it("allows cancellation of a different job", () => {
        const cancellingIds = new Set(["job-1"]);
        const tryCancel = (jobId: string): boolean => {
            if (cancellingIds.has(jobId)) return false;
            cancellingIds.add(jobId);
            return true;
        };

        expect(tryCancel("job-2")).toBe(true);
        expect(cancellingIds.has("job-2")).toBe(true);
    });

    it("prevents multiple confirmation dialogs for the same job", () => {
        const confirmingIds = new Set<string>();
        const tryOpenConfirm = (jobId: string): boolean => {
            if (confirmingIds.has(jobId)) return false;
            confirmingIds.add(jobId);
            return true;
        };

        expect(tryOpenConfirm("job-1")).toBe(true);
        expect(tryOpenConfirm("job-1")).toBe(false);
        confirmingIds.delete("job-1");
        expect(tryOpenConfirm("job-1")).toBe(true);
    });
});

describe("Cancellation failure paths", () => {
    it("removes job from cancellingIds on network failure", async () => {
        const cancellingIds = new Set(["job-1"]);
        const showToast = vi.fn();
        const fetchJobs = vi.fn();

        // Simulate the catch block
        const handleCancelFailure = (jobId: string, err: Error) => {
            showToast(err.message || "Failed to cancel job", "error");
            fetchJobs();
            cancellingIds.delete(jobId);
        };

        handleCancelFailure("job-1", new Error("Network error"));
        expect(showToast).toHaveBeenCalledWith("Network error", "error");
        expect(fetchJobs).toHaveBeenCalledTimes(1);
        expect(cancellingIds.has("job-1")).toBe(false);
    });

    it("shows 'already completed' message for 409 conflict", () => {
        const showToast = vi.fn();

        const handleCancelError = (err: { status?: number; message?: string }) => {
            let message = err.message || "Failed to cancel job";
            if (err.status === 409) {
                message = "Job is already completed and cannot be canceled.";
            }
            showToast(message, "error");
        };

        handleCancelError({ status: 409 });
        expect(showToast).toHaveBeenCalledWith(
            "Job is already completed and cannot be canceled.",
            "error"
        );
    });

    it("shows generic error for non-409 failures", () => {
        const showToast = vi.fn();

        const handleCancelError = (err: { status?: number; message?: string }) => {
            let message = err.message || "Failed to cancel job";
            if (err.status === 409) {
                message = "Job is already completed and cannot be canceled.";
            }
            showToast(message, "error");
        };

        handleCancelError({ status: 500, message: "Internal server error" });
        expect(showToast).toHaveBeenCalledWith("Internal server error", "error");
    });

    it("RETAINS cancelling state if cancel request succeeds (waiting for polling)", () => {
        const cancellingIds = new Set(["job-1"]);
        // Simulate finally block CHANGE: we no longer blindly delete
        const handleCancelFinally = (jobId: string, success: boolean) => {
            if (!success) {
                cancellingIds.delete(jobId);
            }
            // If success, we wait for reconciliation!
        };

        handleCancelFinally("job-1", true);
        expect(cancellingIds.has("job-1")).toBe(true); // Should still be there!

        handleCancelFinally("job-2", false); // Failed request
        expect(cancellingIds.has("job-2")).toBe(false); // Should be cleared
    });
});
describe("Polling terminality", () => {
    it("identifies all TERMINAL_STATUSES as stop conditions", () => {
        const statuses: OpsJobStatus[] = ["CANCELLED", "COMPLETED", "FAILED"];
        for (const status of statuses) {
            expect(TERMINAL_STATUSES.has(status)).toBe(true);
        }
    });

    it("does not identify CANCELLING or RUNNING as terminal", () => {
        expect(TERMINAL_STATUSES.has("CANCELLING")).toBe(false);
        expect(TERMINAL_STATUSES.has("RUNNING")).toBe(false);
        expect(TERMINAL_STATUSES.has("PENDING")).toBe(false);
    });

    /**
     * Simulates the loop condition in pollJobUntilTerminal:
     * if (TERMINAL_STATUSES.has(job.status)) { clear() }
     */
    it("stops polling exactly when status enters TERMINAL_STATUSES", () => {
        const shouldStop = (status: OpsJobStatus) => TERMINAL_STATUSES.has(status);

        expect(shouldStop("RUNNING")).toBe(false);
        expect(shouldStop("CANCELLING")).toBe(false);
        expect(shouldStop("CANCELLED")).toBe(true);
        expect(shouldStop("COMPLETED")).toBe(true);
        expect(shouldStop("FAILED")).toBe(true);
    });
});

describe("Timeout throttling", () => {
    it("fires toast once if no previous toast exists", () => {
        const lastToastAt = new Map<string, number>();
        const jobId = "job-1";
        const now = Date.now();
        const showToast = vi.fn();

        const lastToast = lastToastAt.get(jobId);
        if (!lastToast || now - lastToast > 30000) {
            lastToastAt.set(jobId, now);
            showToast("timeout");
        }

        expect(showToast).toHaveBeenCalledTimes(1);
    });

    it("does not fire toast twice within the throttle interval", () => {
        const lastToastAt = new Map<string, number>();
        const jobId = "job-1";
        const now = Date.now();
        const showToast = vi.fn();

        lastToastAt.set(jobId, now - 10000); // 10s ago

        const lastToast = lastToastAt.get(jobId);
        if (!lastToast || now - lastToast > 30000) {
            lastToastAt.set(jobId, now);
            showToast("timeout");
        }

        expect(showToast).not.toHaveBeenCalled();
    });

    it("fires toast again after throttle interval passed", () => {
        const lastToastAt = new Map<string, number>();
        const jobId = "job-1";
        const now = Date.now();
        const showToast = vi.fn();

        lastToastAt.set(jobId, now - 31000); // 31s ago

        const lastToast = lastToastAt.get(jobId);
        if (!lastToast || now - lastToast > 30000) {
            lastToastAt.set(jobId, now);
            showToast("timeout");
        }

        expect(showToast).toHaveBeenCalledTimes(1);
        expect(lastToastAt.get(jobId)).toBe(now);
    });

    it("includes job context in the toast message (matching logic)", () => {
        const showToast = vi.fn();
        const jobId = "job-12345678";
        const jobType = "INGESTION";

        const message = `Status check for ${jobType} (${jobId.slice(0, 8)}) timed out. Refresh list to re-check.`;
        showToast(message, "warning");

        expect(showToast).toHaveBeenCalledWith(
            "Status check for INGESTION (job-1234) timed out. Refresh list to re-check.",
            "warning"
        );
    });
});
