import React, { useEffect, useState, useCallback, useMemo } from "react";
import { OpsService } from "../api";
import { OpsJobResponse, OpsJobStatus } from "../types/admin";
import { JobsTable } from "../components/ops/JobsTable";
import { KeyboardShortcutsModal } from "../components/ops/KeyboardShortcutsModal";
import { useToast } from "../hooks/useToast";
import { useConfirmation } from "../hooks/useConfirmation";
import { useOperatorShortcuts } from "../hooks/useOperatorShortcuts";
import { ConfirmationDialog } from "../components/common/ConfirmationDialog";

export const TERMINAL_STATUSES = new Set<OpsJobStatus>(["CANCELLED", "COMPLETED", "FAILED"]);

export default function JobsDashboard() {
    const [jobs, setJobs] = useState<OpsJobResponse[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [cancellingJobIds, setCancellingJobIds] = useState<Set<string>>(new Set());
    const [filterType, setFilterType] = useState<string>("");
    const [filterStatus, setFilterStatus] = useState<string>("");
    const [shortcutsOpen, setShortcutsOpen] = useState(false);
    const { show: showToast } = useToast();
    const { confirm, dialogProps } = useConfirmation();

    const isMountedRef = React.useRef(true);
    const cancelPollIntervalRef = React.useRef<number | null>(null);

    // Ref so fetchJobs can read the latest cancellingJobIds without being recreated
    const cancellingJobIdsRef = React.useRef(cancellingJobIds);
    React.useEffect(() => { cancellingJobIdsRef.current = cancellingJobIds; }, [cancellingJobIds]);

    const clearCancelPollInterval = useCallback(() => {
        if (cancelPollIntervalRef.current !== null) {
            window.clearInterval(cancelPollIntervalRef.current);
            cancelPollIntervalRef.current = null;
        }
    }, []);

    useEffect(() => {
        return () => {
            isMountedRef.current = false;
            clearCancelPollInterval();
        };
    }, [clearCancelPollInterval]);

    const fetchJobs = useCallback(async () => {
        if (!isMountedRef.current) return;
        setIsLoading(true);
        try {
            const data = await OpsService.listJobs(50, filterType || undefined, filterStatus || undefined);
            if (!isMountedRef.current) return;
            setJobs(data);

            // Reconcile: if a job we thought was CANCELLING is now terminal, notify and clean up
            const currentCancelling = cancellingJobIdsRef.current;
            if (currentCancelling.size > 0) {
                const resolved: string[] = [];
                for (const job of data) {
                    if (currentCancelling.has(job.id) && TERMINAL_STATUSES.has(job.status)) {
                        resolved.push(job.id);
                        showToast(`Job ${job.id.slice(0, 8)} reached terminal state: ${job.status}`, "success");
                    }
                }
                if (resolved.length > 0) {
                    setCancellingJobIds(prev => {
                        if (!isMountedRef.current) return prev;
                        const next = new Set(prev);
                        resolved.forEach(id => next.delete(id));
                        return next;
                    });
                }
            }
        } catch (err) {
            showToast("Failed to load jobs", "error");
        } finally {
            if (isMountedRef.current) {
                setIsLoading(false);
            }
        }
    }, [filterType, filterStatus, showToast]);

    useEffect(() => {
        fetchJobs();
        const interval = setInterval(fetchJobs, 5000);
        return () => clearInterval(interval);
    }, [fetchJobs]);

    const SHORTCUTS = useMemo(() => [
        { key: "r", label: "Refresh list", handler: fetchJobs },
        { key: "Escape", label: "Clear filters", handler: () => { setFilterType(""); setFilterStatus(""); }, allowInInput: true },
        { key: "?", label: "Show shortcuts", handler: () => setShortcutsOpen(true) },
    ], [fetchJobs]);

    useOperatorShortcuts({ shortcuts: SHORTCUTS, disabled: shortcutsOpen });

    const pollJobUntilTerminal = useCallback((jobId: string, maxAttempts = 10) => {
        clearCancelPollInterval();
        let attempts = 0;
        cancelPollIntervalRef.current = window.setInterval(async () => {
            if (!isMountedRef.current) {
                clearCancelPollInterval();
                return;
            }
            attempts++;
            try {
                const job = await OpsService.getJobStatus(jobId);
                if (TERMINAL_STATUSES.has(job.status as OpsJobStatus)) {
                    clearCancelPollInterval();
                    showToast(`Job ${jobId.slice(0, 8)} reached terminal state: ${job.status}`, "success");
                    void fetchJobs();
                } else if (attempts >= maxAttempts) {
                    clearCancelPollInterval();
                    void fetchJobs();
                }
            } catch (err) {
                clearCancelPollInterval();
            }
        }, 2000);
    }, [clearCancelPollInterval, fetchJobs, showToast]);

    const handleCancel = async (jobId: string) => {
        if (cancellingJobIds.has(jobId)) return;

        const job = jobs.find(j => j.id === jobId);
        if (!job || job.status === "CANCELLING") return;

        clearCancelPollInterval();
        setCancellingJobIds((prev: Set<string>) => {
            const next = new Set(prev);
            next.add(jobId);
            return next;
        });

        const confirmed = await confirm({
            title: "Cancel Job",
            description: "Are you sure you want to cancel this background job? This action cannot be undone.",
            confirmText: "Cancel Job",
            danger: true
        });

        if (!confirmed) {
            if (isMountedRef.current) {
                setCancellingJobIds((prev: Set<string>) => {
                    const next = new Set(prev);
                    next.delete(jobId);
                    return next;
                });
            }
            return;
        }

        // Optimistic update
        setJobs((prev: OpsJobResponse[]) => prev.map(j => (j.id === jobId ? { ...j, status: "CANCELLING" as OpsJobStatus } : j)));

        try {
            await OpsService.cancelJob(jobId);
            if (!isMountedRef.current) return;
            showToast("Job cancellation requested", "success");
            pollJobUntilTerminal(jobId);
        } catch (err: any) {
            let message = err.message || "Failed to cancel job";
            if (err.code === "JOB_ALREADY_TERMINAL" || err.status === 409) {
                message = "Job is already completed and cannot be canceled.";
            }
            showToast(message, "error");
            void fetchJobs();
        } finally {
            if (isMountedRef.current) {
                setCancellingJobIds((prev: Set<string>) => {
                    const next = new Set(prev);
                    next.delete(jobId);
                    return next;
                });
            }
        }
    };

    return (
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            <div className="sm:flex sm:items-center">
                <div className="sm:flex-auto">
                    <h1 className="text-xl font-semibold text-gray-900 dark:text-gray-100">Background Jobs</h1>
                    <p className="mt-2 text-sm text-gray-700 dark:text-gray-300">
                        A list of all operational jobs including ingestion, hydration, and synthetic data generation.
                    </p>
                </div>
                <div className="mt-4 sm:mt-0 sm:ml-16 sm:flex-none flex items-center gap-3">
                    <button
                        onClick={() => setShortcutsOpen(true)}
                        aria-label="Show keyboard shortcuts"
                        title="Keyboard shortcuts (?)"
                        className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 border border-gray-200 dark:border-gray-700 rounded px-2 py-0.5 text-sm font-mono"
                    >
                        ?
                    </button>
                    <button
                        onClick={fetchJobs}
                        className="inline-flex items-center justify-center rounded-md border border-transparent bg-indigo-600 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 sm:w-auto"
                    >
                        Refresh
                    </button>
                </div>
            </div>

            <div className="mt-4 flex gap-4">
                <select
                    value={filterType}
                    onChange={(e) => setFilterType(e.target.value)}
                    className="mt-1 block w-full rounded-md border-gray-300 py-2 pl-3 pr-10 text-base focus:border-indigo-500 focus:outline-none focus:ring-indigo-500 sm:text-sm dark:bg-gray-800 dark:border-gray-700 dark:text-gray-200"
                >
                    <option value="">All Types</option>
                    <option value="SCHEMA_HYDRATION">Schema Hydration</option>
                    <option value="SCHEMA_INGESTION">Schema Ingestion</option>
                    <option value="CACHE_REINDEX">Cache Reindex</option>
                    <option value="PATTERN_ENRICHMENT">Pattern Enrichment</option>
                    <option value="SYNTH_GENERATE">Synthetic Generation</option>
                </select>

                <select
                    value={filterStatus}
                    onChange={(e) => setFilterStatus(e.target.value)}
                    className="mt-1 block w-full rounded-md border-gray-300 py-2 pl-3 pr-10 text-base focus:border-indigo-500 focus:outline-none focus:ring-indigo-500 sm:text-sm dark:bg-gray-800 dark:border-gray-700 dark:text-gray-200"
                >
                    <option value="">All Statuses</option>
                    <option value="PENDING">Pending</option>
                    <option value="RUNNING">Running</option>
                    <option value="COMPLETED">Completed</option>
                    <option value="FAILED">Failed</option>
                </select>
            </div>

            <div className="mt-8 flex flex-col">
                <JobsTable jobs={jobs} isLoading={isLoading} onCancel={handleCancel} cancellingJobIds={cancellingJobIds} />
            </div>

            <ConfirmationDialog {...dialogProps} />
            <KeyboardShortcutsModal
                isOpen={shortcutsOpen}
                onClose={() => setShortcutsOpen(false)}
                shortcuts={SHORTCUTS}
            />
        </div>
    );
}
