import React, { useEffect, useState, useCallback, useMemo } from "react";
import { OpsService } from "../api";
import { OpsJobResponse, OpsJobStatus } from "../types/admin";
import { JobsTable } from "../components/ops/JobsTable";
import { KeyboardShortcutsModal } from "../components/ops/KeyboardShortcutsModal";
import { useToast } from "../hooks/useToast";
import { useConfirmation } from "../hooks/useConfirmation";
import { useOperatorShortcuts } from "../hooks/useOperatorShortcuts";
import { ConfirmationDialog } from "../components/common/ConfirmationDialog";
import { JOBS_DASHBOARD_PAGE_SIZE } from "../constants/pagination";
import { handleOperatorEscapeShortcut } from "../utils/operatorEscape";

export const TERMINAL_STATUSES = new Set<OpsJobStatus>(["CANCELLED", "COMPLETED", "FAILED"]);

export default function JobsDashboard() {
    const [jobs, setJobs] = useState<OpsJobResponse[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [cancellingJobIds, setCancellingJobIds] = useState<Set<string>>(new Set());
    const [cancelPollTimeoutJobIds, setCancelPollTimeoutJobIds] = useState<Set<string>>(new Set());
    const [filterType, setFilterType] = useState<string>("");
    const [filterStatus, setFilterStatus] = useState<string>("");
    const [shortcutsOpen, setShortcutsOpen] = useState(false);
    const { show: showToast } = useToast();
    const { confirm, dialogProps } = useConfirmation();
    const openShortcutsModal = useCallback(() => setShortcutsOpen(true), []);
    const closeShortcutsModal = useCallback(() => setShortcutsOpen(false), []);

    const isMountedRef = React.useRef(true);
    const activePollersRef = React.useRef<Map<string, number>>(new Map());
    const confirmingCancelJobIdsRef = React.useRef(new Set<string>());
    const lastTimeoutToastAtRef = React.useRef(new Map<string, number>());

    // Ref so fetchJobs can read the latest cancellingJobIds without being recreated
    const cancellingJobIdsRef = React.useRef(cancellingJobIds);
    React.useEffect(() => { cancellingJobIdsRef.current = cancellingJobIds; }, [cancellingJobIds]);

    const clearCancelPollInterval = useCallback((jobId?: string) => {
        if (jobId) {
            const interval = activePollersRef.current.get(jobId);
            if (interval !== undefined) {
                window.clearInterval(interval);
                activePollersRef.current.delete(jobId);
            }
        } else {
            activePollersRef.current.forEach((interval) => window.clearInterval(interval));
            activePollersRef.current.clear();
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
            const data = await OpsService.listJobs(
                JOBS_DASHBOARD_PAGE_SIZE,
                filterType || undefined,
                filterStatus || undefined
            );
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
                        cancellingJobIdsRef.current = next;
                        return next;
                    });
                }
            }

            setCancelPollTimeoutJobIds((prev) => {
                if (prev.size === 0) return prev;
                let changed = false;
                const next = new Set(prev);
                for (const job of data) {
                    if (next.has(job.id) && TERMINAL_STATUSES.has(job.status)) {
                        next.delete(job.id);
                        lastTimeoutToastAtRef.current.delete(job.id);
                        changed = true;
                    }
                }
                return changed ? next : prev;
            });
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

    const clearFilters = useCallback(() => {
        setFilterType("");
        setFilterStatus("");
    }, []);

    const handleEscapeShortcut = useCallback(() => {
        handleOperatorEscapeShortcut({
            isModalOpen: shortcutsOpen,
            closeModal: closeShortcutsModal,
            clearFilters,
        });
    }, [clearFilters, closeShortcutsModal, shortcutsOpen]);

    const SHORTCUTS = useMemo(() => [
        { key: "r", label: "Refresh list", handler: fetchJobs },
        { key: "Escape", label: "Clear filters", handler: handleEscapeShortcut, allowInInput: true },
        { key: "?", label: "Show shortcuts", handler: openShortcutsModal },
    ], [fetchJobs, handleEscapeShortcut, openShortcutsModal]);

    useOperatorShortcuts({ shortcuts: SHORTCUTS, disabled: shortcutsOpen });

    const pollJobUntilTerminal = useCallback((jobId: string, jobType: string, maxAttempts = 10) => {
        if (activePollersRef.current.has(jobId)) {
            return; // Already polling this job
        }

        let attempts = 0;
        const interval = window.setInterval(async () => {
            if (!isMountedRef.current) {
                clearCancelPollInterval(jobId);
                return;
            }
            attempts++;
            try {
                const job = await OpsService.getJobStatus(jobId);
                if (TERMINAL_STATUSES.has(job.status as OpsJobStatus)) {
                    clearCancelPollInterval(jobId);
                    lastTimeoutToastAtRef.current.delete(jobId);
                    setCancelPollTimeoutJobIds((prev) => {
                        if (!prev.has(jobId)) return prev;
                        const next = new Set(prev);
                        next.delete(jobId);
                        return next;
                    });
                    showToast(`Job ${jobId.slice(0, 8)} reached terminal state: ${job.status}`, "success");
                    void fetchJobs();
                } else if (attempts >= maxAttempts) {
                    clearCancelPollInterval(jobId);
                    const now = Date.now();
                    const lastToast = lastTimeoutToastAtRef.current.get(jobId);
                    if (!lastToast || now - lastToast > 30000) {
                        lastTimeoutToastAtRef.current.set(jobId, now);
                        showToast(`Status check for ${jobType} (${jobId.slice(0, 8)}) timed out. Refresh list to re-check.`, "warning");
                    }
                    setCancelPollTimeoutJobIds((prev) => {
                        if (prev.has(jobId)) return prev;
                        const next = new Set(prev);
                        next.add(jobId);
                        return next;
                    });
                    void fetchJobs();
                }
            } catch (err) {
                clearCancelPollInterval(jobId);
                console.error(`Error polling job ${jobId}:`, err);
            }
        }, 2000);

        activePollersRef.current.set(jobId, interval);
    }, [clearCancelPollInterval, fetchJobs, showToast]);

    const handleCancel = async (jobId: string) => {
        if (cancellingJobIds.has(jobId) || confirmingCancelJobIdsRef.current.has(jobId)) return;

        const job = jobs.find(j => j.id === jobId);
        if (!job || job.status === "CANCELLING") return;

        clearCancelPollInterval(jobId);
        lastTimeoutToastAtRef.current.delete(jobId);
        setCancelPollTimeoutJobIds((prev) => {
            if (!prev.has(jobId)) return prev;
            const next = new Set(prev);
            next.delete(jobId);
            return next;
        });
        confirmingCancelJobIdsRef.current.add(jobId);
        let confirmed = false;
        try {
            confirmed = await confirm({
                title: "Cancel Job",
                description: "Are you sure you want to cancel this background job? This action cannot be undone.",
                confirmText: "Cancel Job",
                danger: true
            });
        } finally {
            confirmingCancelJobIdsRef.current.delete(jobId);
        }

        if (!confirmed || !isMountedRef.current) {
            return;
        }

        setCancellingJobIds((prev: Set<string>) => {
            const next = new Set(prev);
            next.add(jobId);
            cancellingJobIdsRef.current = next;
            return next;
        });

        // Optimistic update
        setJobs((prev: OpsJobResponse[]) => prev.map(j => (j.id === jobId ? { ...j, status: "CANCELLING" as OpsJobStatus } : j)));

        try {
            await OpsService.cancelJob(jobId);
            if (!isMountedRef.current) return;
            showToast("Job cancellation requested", "success");
            pollJobUntilTerminal(jobId, job.job_type);
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

    const handleTimeoutRefresh = useCallback(() => {
        lastTimeoutToastAtRef.current.clear();
        setCancelPollTimeoutJobIds(new Set());
        void fetchJobs();
    }, [fetchJobs]);

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
                        onClick={openShortcutsModal}
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
                {cancelPollTimeoutJobIds.size > 0 && (
                    <div
                        data-testid="cancel-timeout-refresh-banner"
                        className="mb-4 rounded-md border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900"
                    >
                        <span>Status check timed out — </span>
                        <button
                            onClick={handleTimeoutRefresh}
                            className="font-medium underline hover:no-underline"
                        >
                            Refresh list
                        </button>
                    </div>
                )}
                <JobsTable jobs={jobs} isLoading={isLoading} onCancel={handleCancel} cancellingJobIds={cancellingJobIds} />
            </div>

            <ConfirmationDialog {...dialogProps} />
            <KeyboardShortcutsModal
                isOpen={shortcutsOpen}
                onClose={closeShortcutsModal}
                shortcuts={SHORTCUTS}
            />
        </div>
    );
}
