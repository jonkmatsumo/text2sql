import React, { useEffect, useState, useCallback, useMemo } from "react";
import { useSearchParams, Link } from "react-router-dom";
import { OpsService } from "../api";
import { Interaction, InteractionStatus, FeedbackThumb } from "../types/admin";
import { useToast } from "../hooks/useToast";
import { useOperatorShortcuts } from "../hooks/useOperatorShortcuts";
import { LoadingState } from "../components/common/LoadingState";
import { KeyboardShortcutsModal } from "../components/ops/KeyboardShortcutsModal";
import TraceLink from "../components/common/TraceLink";
import FilterSelect from "../components/common/FilterSelect";

const STATUS_OPTIONS: { value: InteractionStatus | "All"; label: string }[] = [
    { value: "All", label: "All Statuses" },
    { value: "SUCCESS", label: "Success" },
    { value: "FAILED", label: "Failed" },
    { value: "PENDING", label: "Pending" },
    { value: "APPROVED", label: "Approved" },
    { value: "REJECTED", label: "Rejected" },
];

const THUMB_OPTIONS: { value: FeedbackThumb; label: string }[] = [
    { value: "All", label: "All Feedback" },
    { value: "UP", label: "Positive (UP)" },
    { value: "DOWN", label: "Negative (DOWN)" },
    { value: "None", label: "No Feedback" },
];

export default function RunHistory() {
    const [searchParams, setSearchParams] = useSearchParams();
    const [runs, setRuns] = useState<Interaction[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [shortcutsOpen, setShortcutsOpen] = useState(false);
    const [linkCopied, setLinkCopied] = useState(false);

    const copyLink = useCallback(() => {
        navigator.clipboard.writeText(window.location.href).then(() => {
            setLinkCopied(true);
            setTimeout(() => setLinkCopied(false), 2000);
        });
    }, []);

    // Derived state from URL
    const statusFilter = (searchParams.get("status") as InteractionStatus | "All") || "All";
    const thumbFilter = (searchParams.get("feedback") as FeedbackThumb) || "All";
    const searchQuery = searchParams.get("q") || "";
    const offset = parseInt(searchParams.get("offset") || "0", 10);

    const limit = 50;
    const { show: showToast } = useToast();
    const searchInputRef = React.useRef<HTMLInputElement>(null);

    const updateFilters = useCallback((updates: Record<string, string | number | undefined>) => {
        setSearchParams(prev => {
            // Start from current params
            const merged: Record<string, string> = {};
            prev.forEach((v, k) => { merged[k] = v; });

            // Apply updates
            Object.entries(updates).forEach(([key, value]) => {
                if (value === undefined || value === "" || value === "All" || value === 0) {
                    delete merged[key];
                } else {
                    merged[key] = String(value);
                }
            });

            // Reset offset on filter change unless explicitly updating offset
            if (!Object.prototype.hasOwnProperty.call(updates, "offset")) {
                delete merged["offset"];
            }

            // Write in deterministic alphabetical order
            const canonical = new URLSearchParams();
            Object.keys(merged).sort().forEach(k => canonical.set(k, merged[k]));
            return canonical;
        }, { replace: true });
    }, [setSearchParams]);

    const fetchRuns = useCallback(async () => {
        setIsLoading(true);
        try {
            const data = await OpsService.listRuns(limit, offset, statusFilter, thumbFilter);
            const seenIds = new Set<string>();
            const uniqueData = data.filter(run => {
                if (seenIds.has(run.id)) return false;
                seenIds.add(run.id);
                return true;
            });
            setRuns(uniqueData);
        } catch (err) {
            showToast("Failed to fetch run history", "error");
        } finally {
            setIsLoading(false);
        }
    }, [offset, statusFilter, thumbFilter, showToast]);

    useEffect(() => {
        fetchRuns();
    }, [fetchRuns]);

    const clearFilters = useCallback(() => {
        setSearchParams({}, { replace: true });
    }, [setSearchParams]);

    const SHORTCUTS = useMemo(() => [
        { key: "r", label: "Refresh list", handler: fetchRuns },
        { key: "/", label: "Focus search", handler: () => searchInputRef.current?.focus() },
        { key: "Escape", label: "Clear filters", handler: clearFilters, allowInInput: true },
        { key: "?", label: "Show shortcuts", handler: () => setShortcutsOpen(true) },
    ], [fetchRuns, clearFilters]);

    useOperatorShortcuts({ shortcuts: SHORTCUTS, disabled: shortcutsOpen });

    const filteredRuns = useMemo(() =>
        runs.filter(run =>
            run.user_nlq_text.toLowerCase().includes(searchQuery.toLowerCase()) ||
            run.id.toLowerCase().includes(searchQuery.toLowerCase())
        ),
        [runs, searchQuery]
    );

    return (
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            <header className="mb-8 flex items-start justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Run History</h1>
                    <p className="mt-2 text-sm text-gray-600 dark:text-gray-400">
                        Browse and inspect historical agent execution runs.
                    </p>
                </div>
                <div className="mt-1 flex items-center gap-2">
                    <button
                        onClick={copyLink}
                        aria-label="Copy link to current view"
                        title="Copy link to current view"
                        className="text-gray-500 hover:text-gray-700 dark:hover:text-gray-200 border border-gray-200 dark:border-gray-700 rounded px-2.5 py-0.5 text-xs font-medium transition-colors"
                    >
                        {linkCopied ? "✓ Copied!" : "Copy link"}
                    </button>
                    <button
                        onClick={() => setShortcutsOpen(true)}
                        aria-label="Show keyboard shortcuts"
                        title="Keyboard shortcuts (?)"
                        className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 font-mono border border-gray-200 dark:border-gray-700 rounded px-2 py-0.5 text-sm"
                    >
                        ?
                    </button>
                </div>
            </header>

            <div className="mb-6 grid grid-cols-1 gap-4 sm:grid-cols-4 items-end">
                <div className="sm:col-span-2">
                    <label className="block text-xs font-medium text-gray-500 uppercase tracking-wider mb-1 font-semibold">Search Queries or IDs</label>
                    <input
                        ref={searchInputRef}
                        type="text"
                        placeholder="Keyword search..."
                        value={searchQuery}
                        onChange={(e) => updateFilters({ q: e.target.value })}
                        aria-label="Search runs by query or ID"
                        className="w-full px-4 py-2 border border-gray-300 dark:border-gray-700 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-400 focus:ring-indigo-500 focus:border-indigo-500"
                    />
                </div>
                <FilterSelect
                    label="Status"
                    value={statusFilter}
                    options={STATUS_OPTIONS}
                    onChange={(val) => updateFilters({ status: val as string })}
                />
                <FilterSelect
                    label="Feedback"
                    value={thumbFilter}
                    options={THUMB_OPTIONS}
                    onChange={(val) => updateFilters({ feedback: val as string })}
                />
            </div>

            <div className="bg-white dark:bg-gray-900 shadow overflow-hidden sm:rounded-lg border border-gray-200 dark:border-gray-800">
                <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-800" aria-label="Run history table">
                    <thead className="bg-gray-50 dark:bg-gray-800 text-gray-500">
                        <tr>
                            <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">Run ID</th>
                            <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">Query</th>
                            <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">Status</th>
                            <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">Feedback</th>
                            <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">Created</th>
                            <th className="px-6 py-3 text-right text-xs font-medium uppercase tracking-wider">Actions</th>
                        </tr>
                    </thead>
                    <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-800">
                        {isLoading ? (
                            <tr>
                                <td colSpan={6} className="px-6 py-12 text-center">
                                    <LoadingState message="Loading runs..." />
                                </td>
                            </tr>
                        ) : filteredRuns.length === 0 ? (
                            <tr>
                                <td colSpan={6} className="px-6 py-12 text-center">
                                    <div className="flex flex-col items-center justify-center space-y-3">
                                        <p className="text-gray-500 italic">
                                            {statusFilter !== "All" || thumbFilter !== "All" || searchQuery !== ""
                                                ? "No historical runs found matching these filters."
                                                : "No runs recorded yet."}
                                        </p>
                                        {(statusFilter !== "All" || thumbFilter !== "All" || searchQuery !== "") && (
                                            <button
                                                onClick={clearFilters}
                                                className="text-sm text-indigo-600 hover:text-indigo-500 font-medium"
                                            >
                                                Clear all filters
                                            </button>
                                        )}
                                    </div>
                                </td>
                            </tr>
                        ) : (
                            filteredRuns.map((run) => (
                                <tr key={run.id} className="hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
                                    <td className="px-6 py-4 whitespace-nowrap text-sm font-mono text-gray-500">
                                        {run.id.slice(0, 8)}...
                                    </td>
                                    <td className="px-6 py-4 text-sm text-gray-900 dark:text-gray-100 max-w-xs truncate">
                                        {run.user_nlq_text}
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap">
                                        <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${run.execution_status === 'SUCCESS' || run.execution_status === 'APPROVED' ? 'bg-green-100 text-green-800' :
                                            run.execution_status === 'FAILED' || run.execution_status === 'REJECTED' ? 'bg-red-100 text-red-800' :
                                                'bg-gray-100 text-gray-800'
                                            }`}>
                                            {run.execution_status}
                                        </span>
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                        {run.thumb === 'UP' ? '👍' : run.thumb === 'DOWN' ? '👎' : '—'}
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                        {run.created_at ? new Date(run.created_at).toLocaleString() : '—'}
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium space-x-3">
                                        <Link
                                            to={`/admin/runs/${run.id}`}
                                            aria-label={`View details for run ${run.id}`}
                                            className="text-indigo-600 hover:text-indigo-900 dark:text-indigo-400 dark:hover:text-indigo-300"
                                        >
                                            Details
                                        </Link>
                                        <TraceLink
                                            traceId={run.trace_id}
                                            interactionId={run.id}
                                            variant="icon"
                                            showCopy={false}
                                        />
                                    </td>
                                </tr>
                            ))
                        )}
                    </tbody>
                </table>
            </div>

            <div className="mt-4 flex items-center justify-between">
                <button
                    onClick={() => updateFilters({ offset: Math.max(0, offset - limit) })}
                    disabled={offset === 0 || isLoading}
                    aria-label="Previous page"
                    className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
                >
                    Previous
                </button>
                <div className="text-sm text-gray-700 dark:text-gray-300" aria-live="polite">
                    Showing results {offset + 1} – {offset + runs.length}
                </div>
                <button
                    onClick={() => updateFilters({ offset: offset + limit })}
                    disabled={runs.length < limit || isLoading}
                    aria-label="Next page"
                    className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
                >
                    Next
                </button>
            </div>

            <KeyboardShortcutsModal
                isOpen={shortcutsOpen}
                onClose={() => setShortcutsOpen(false)}
                shortcuts={SHORTCUTS}
            />
        </div>
    );
}
