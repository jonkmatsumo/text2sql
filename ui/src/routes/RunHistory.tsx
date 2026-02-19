import React, { useEffect, useState, useCallback, useMemo } from "react";
import { useSearchParams, Link } from "react-router-dom";
import { Interaction, InteractionStatus, FeedbackThumb } from "../types/admin";
import { getInteractionStatusTone, STATUS_TONE_CLASSES } from "../utils/operatorUi";
import { OpsService, getErrorMessage, ApiError } from "../api";
import { makeToastDedupeKey } from "../utils/toastUtils";
import { useToast } from "../hooks/useToast";
import { useOperatorShortcuts } from "../hooks/useOperatorShortcuts";
import { LoadingState } from "../components/common/LoadingState";
import { KeyboardShortcutsModal } from "../components/ops/KeyboardShortcutsModal";
import TraceLink from "../components/common/TraceLink";
import FilterSelect from "../components/common/FilterSelect";
import { formatRunHistoryRange, hasRunHistoryNextPage } from "../constants/operatorUi";
import { RUN_HISTORY_PAGE_SIZE } from "../constants/pagination";
import { handleOperatorEscapeShortcut } from "../utils/operatorEscape";

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
    const { show: showToast } = useToast();
    const [searchParams, setSearchParams] = useSearchParams();
    const [runs, setRuns] = useState<Interaction[]>([]);
    const [hasMore, setHasMore] = useState<boolean | undefined>(undefined);
    const [totalCount, setTotalCount] = useState<number | undefined>(undefined);
    const [isLoading, setIsLoading] = useState(true);
    const [loadError, setLoadError] = useState<string | null>(null);
    const [shortcutsOpen, setShortcutsOpen] = useState(false);
    const [linkCopied, setLinkCopied] = useState(false);

    const copyLink = useCallback(() => {
        navigator.clipboard.writeText(window.location.href).then(() => {
            setLinkCopied(true);
            setTimeout(() => setLinkCopied(false), 2000);
        }).catch((err) => {
            showToast("Could not copy to clipboard", "error");
            console.error("Clipboard copy failed:", err);
        });
    }, [showToast]);

    const openShortcutsModal = useCallback(() => setShortcutsOpen(true), []);
    const closeShortcutsModal = useCallback(() => setShortcutsOpen(false), []);

    // Derived state from URL
    const statusFilter = (searchParams.get("status") as InteractionStatus | "All") || "All";
    const thumbFilter = (searchParams.get("feedback") as FeedbackThumb) || "All";
    const searchQuery = searchParams.get("q") || "";
    const offset = parseInt(searchParams.get("offset") || "0", 10);

    const limit = RUN_HISTORY_PAGE_SIZE;
    const searchInputRef = React.useRef<HTMLInputElement>(null);
    const recoveryAttemptedRef = React.useRef(false);
    const recoveryToastShownRef = React.useRef(false);

    useEffect(() => {
        recoveryAttemptedRef.current = false;
        recoveryToastShownRef.current = false;
    }, [statusFilter, thumbFilter, searchQuery]);

    useEffect(() => {
        if (offset === 0) {
            recoveryAttemptedRef.current = false;
            recoveryToastShownRef.current = false;
        }
    }, [offset]);

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
        setLoadError(null);
        try {
            const result = await OpsService.listRuns(limit, offset, statusFilter, thumbFilter);
            const data = result.runs;
            const more = result.has_more;
            const count = result.total_count;

            // Single-shot empty-page recovery for non-zero offsets.
            if (offset > 0 && data.length === 0) {
                if (!recoveryAttemptedRef.current) {
                    recoveryAttemptedRef.current = true;
                    const fallbackOffset = Math.max(0, offset - limit);
                    if (!recoveryToastShownRef.current) {
                        showToast("Requested page is out of range. Showing previous results.", "warning");
                        recoveryToastShownRef.current = true;
                    }
                    updateFilters({ offset: fallbackOffset });
                }
                return;
            }

            if (data.length > 0) {
                recoveryAttemptedRef.current = false;
                recoveryToastShownRef.current = false;
            }

            const seenIds = new Set<string>();
            const uniqueData = data.filter((run: Interaction) => {
                if (seenIds.has(run.id)) return false;
                seenIds.add(run.id);
                return true;
            });
            setRuns(uniqueData);
            setHasMore(more);
            setTotalCount(count);
        } catch (err) {
            const message = getErrorMessage(err);
            const category = err instanceof ApiError ? err.code : "UNKNOWN_ERROR";
            const dedupeKey = makeToastDedupeKey("run-history", category, message, {
                surface: "RunHistory.fetchRuns",
                identifiers: {
                    offset,
                    status: statusFilter,
                    feedback: thumbFilter,
                },
            });
            showToast(message, "error", { dedupeKey });
            setLoadError("Could not load run history. Refresh to retry.");
        } finally {
            setIsLoading(false);
        }
    }, [offset, statusFilter, thumbFilter, showToast, limit, updateFilters]);

    useEffect(() => {
        fetchRuns();
    }, [fetchRuns]);

    const clearFilters = useCallback(() => {
        setSearchParams({}, { replace: true });
    }, [setSearchParams]);

    const clearSearch = useCallback(() => {
        updateFilters({ q: "", offset: 0 });
    }, [updateFilters]);

    const handleEscapeShortcut = useCallback(() => {
        handleOperatorEscapeShortcut({
            isModalOpen: shortcutsOpen,
            closeModal: closeShortcutsModal,
            clearFilters,
        });
    }, [clearFilters, closeShortcutsModal, shortcutsOpen]);

    const SHORTCUTS = useMemo(() => [
        { key: "r", label: "Refresh list", handler: fetchRuns },
        { key: "/", label: "Focus search", handler: () => searchInputRef.current?.focus() },
        { key: "Escape", label: "Clear filters", handler: handleEscapeShortcut, allowInInput: true },
        { key: "?", label: "Show shortcuts", handler: openShortcutsModal },
    ], [fetchRuns, handleEscapeShortcut, openShortcutsModal]);

    useOperatorShortcuts({ shortcuts: SHORTCUTS, disabled: shortcutsOpen });

    const filteredRuns = useMemo(() =>
        runs.filter(run =>
            run.user_nlq_text.toLowerCase().includes(searchQuery.toLowerCase()) ||
            run.id.toLowerCase().includes(searchQuery.toLowerCase())
        ),
        [runs, searchQuery]
    );
    const showPageScopedSearchNote = searchQuery.trim() !== "";
    const canNavigateNext = hasRunHistoryNextPage(hasMore, runs.length, limit);
    const rangeSummary = formatRunHistoryRange(offset, runs.length, totalCount);

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
                        onClick={openShortcutsModal}
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
                    <div className="flex gap-2">
                        <div className="relative flex-grow">
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
                        {searchQuery && (
                            <button
                                onClick={clearSearch}
                                aria-label="Clear search query"
                                className="px-3 py-2 border border-gray-200 dark:border-gray-700 rounded-md text-sm font-medium text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 whitespace-nowrap"
                            >
                                Clear
                            </button>
                        )}
                        <button
                            disabled
                            title="Global search across all history is not yet supported by the backend."
                            className="px-3 py-2 border border-gray-200 dark:border-gray-700 rounded-md text-sm font-medium text-gray-400 bg-gray-50 dark:bg-gray-800 cursor-not-allowed opacity-60 whitespace-nowrap"
                        >
                            Search All
                        </button>
                    </div>
                    {showPageScopedSearchNote && (
                        <div
                            className="mt-2 p-2.5 bg-blue-50 dark:bg-blue-900/20 border border-blue-100 dark:border-blue-800/30 rounded-md flex items-start gap-2"
                            data-testid="runhistory-search-scope-note"
                        >
                            <span className="text-blue-500 mt-0.5">ℹ️</span>
                            <div className="flex-grow">
                                <p className="text-xs text-blue-700 dark:text-blue-400 mb-1" data-testid="runhistory-search-scope-inline-label">
                                    Search is limited to this page.
                                </p>
                                <p className="text-[11px] font-bold text-blue-800 dark:text-blue-300 uppercase tracking-tight">Search is limited to this page</p>
                                <div className="text-xs text-blue-700 dark:text-blue-400 leading-normal">
                                    <p>Results only include runs already loaded in the table below.</p>
                                    {(canNavigateNext && searchQuery) && (
                                        <p className="mt-1 font-semibold italic text-blue-800 dark:text-blue-300" data-testid="runhistory-more-runs-hint">
                                            More runs exist beyond this page; try Next.
                                        </p>
                                    )}
                                    <button
                                        onClick={() => updateFilters({ offset: offset + limit })}
                                        disabled={!canNavigateNext || isLoading}
                                        className="mt-1.5 font-bold underline hover:text-blue-900 dark:hover:text-blue-200 block text-[11px] uppercase tracking-wide"
                                    >
                                        Scan next page &rarr;
                                    </button>
                                </div>
                            </div>
                        </div>
                    )}
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
                        ) : loadError ? (
                            <tr>
                                <td colSpan={6} className="px-6 py-12 text-center">
                                    <div className="flex flex-col items-center justify-center space-y-3">
                                        <p className="text-gray-500 italic" data-testid="runhistory-load-error">
                                            {loadError}
                                        </p>
                                        <button
                                            onClick={fetchRuns}
                                            className="text-sm text-indigo-600 hover:text-indigo-500 font-medium"
                                        >
                                            Retry loading runs
                                        </button>
                                    </div>
                                </td>
                            </tr>
                        ) : filteredRuns.length === 0 ? (
                            <tr>
                                <td colSpan={6} className="px-6 py-12 text-center">
                                    <div className="flex flex-col items-center justify-center space-y-3">
                                        <p className="text-gray-500 italic">
                                            {statusFilter !== "All" || thumbFilter !== "All" || searchQuery !== ""
                                                ? (searchQuery !== ""
                                                    ? "No matches found on this page. Try Next to search older runs."
                                                    : "No historical runs found matching these filters.")
                                                : "No runs recorded yet."}
                                        </p>
                                        {searchQuery !== "" && (
                                            <div className="mt-2 p-2 bg-gray-50 dark:bg-gray-800/50 border border-gray-100 dark:border-gray-700/50 rounded text-left max-w-sm mx-auto" data-testid="runhistory-empty-search-scope-note">
                                                <p className="text-[10px] font-bold text-gray-500 dark:text-gray-400 uppercase tracking-tight mb-1">Search is limited to this page</p>
                                                <p className="text-xs text-gray-400 dark:text-gray-500 leading-tight">
                                                    Results only include runs already loaded in the table. Try clicking <strong>Next</strong> to scan older runs for matches.
                                                </p>
                                            </div>
                                        )}
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
                                        <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${STATUS_TONE_CLASSES[getInteractionStatusTone(run.execution_status)]}`}>
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
                    {rangeSummary}
                </div>
                <button
                    onClick={() => updateFilters({ offset: offset + limit })}
                    disabled={!canNavigateNext || isLoading}
                    aria-label="Next page"
                    title={searchQuery && !canNavigateNext ? "Search is limited to this page" : undefined}
                    className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
                >
                    Next
                </button>
            </div>

            <KeyboardShortcutsModal
                isOpen={shortcutsOpen}
                onClose={closeShortcutsModal}
                shortcuts={SHORTCUTS}
            />
        </div>
    );
}
