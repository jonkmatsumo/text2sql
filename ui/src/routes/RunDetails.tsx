import React, { useEffect, useState, useCallback, useMemo } from "react";
import { useParams, Link } from "react-router-dom";
import { getDiagnostics, getErrorMessage, ApiError } from "../api";
import { makeToastDedupeKey } from "../utils/toastUtils";
import { useToast } from "../hooks/useToast";
import { LoadingState } from "../components/common/LoadingState";
import { toPrettyJson, normalizeDecisionEvents, formatTimestamp } from "../utils/observability";
import { buildRunContextBundle } from "../utils/buildRunContextBundle";
import RunIdentifiers from "../components/common/RunIdentifiers";
import type { RunDiagnosticsResponse } from "../types/diagnostics";
import { getInteractionStatusTone, STATUS_TONE_CLASSES } from "../utils/operatorUi";

export default function RunDetails() {
    const { runId } = useParams<{ runId: string }>();
    const { show: showToast } = useToast();
    const [isLoading, setIsLoading] = useState(true);
    const [diagnostics, setDiagnostics] = useState<RunDiagnosticsResponse | null>(null);
    const [contextCopied, setContextCopied] = useState(false);

    const fetchDetails = useCallback(async () => {
        setIsLoading(true);
        try {
            if (runId) {
                const diag = await getDiagnostics(true, runId);
                setDiagnostics(diag);
            }
        } catch (err) {
            const message = getErrorMessage(err);
            const category = err instanceof ApiError ? err.code : "UNKNOWN_ERROR";
            const dedupeKey = makeToastDedupeKey("run-details", category, message);
            showToast(message, "error", { dedupeKey });
        } finally {
            setIsLoading(false);
        }
    }, [runId, showToast]);

    useEffect(() => {
        fetchDetails();
    }, [fetchDetails]);

    const normalizedEvents = useMemo(() =>
        normalizeDecisionEvents(diagnostics?.audit_events),
        [diagnostics?.audit_events]
    );

    const copyRunContext = useCallback(() => {
        const runData = diagnostics?.run_context || {};
        const validation = diagnostics?.validation || {};
        const completeness = diagnostics?.completeness || {};

        const bundle = buildRunContextBundle({
            runId,
            traceId: diagnostics?.trace_id,
            requestId: diagnostics?.request_id,
            userQuery: runData?.user_nlq_text,
            generatedSql: diagnostics?.generated_sql,
            validationStatus: validation?.ast_valid ? "PASSED" : (validation?.ast_valid === false ? "FAILED" : undefined),
            validationErrors: validation?.syntax_errors ?? [],
            executionStatus: runData?.execution_status,
            isComplete: !completeness?.is_truncated,
        });

        navigator.clipboard.writeText(bundle).then(() => {
            setContextCopied(true);
            setTimeout(() => setContextCopied(false), 2000);
        }).catch((err) => {
            showToast("Could not copy to clipboard", "error");
            console.error("Clipboard copy failed:", err);
        });
    }, [runId, diagnostics, showToast]);

    if (isLoading) {
        return (
            <div className="flex justify-center items-center h-64">
                <LoadingState message="Loading run details..." />
            </div>
        );
    }

    const runData = diagnostics?.run_context || {};
    const validation = diagnostics?.validation || {};
    const completeness = diagnostics?.completeness || {};
    const hasDiagnostics = diagnostics !== null;
    const hasEvents = normalizedEvents.length > 0;

    return (
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            <div className="mb-6 flex justify-between items-end">
                <div>
                    <Link
                        to="/admin/runs"
                        className="text-sm font-medium text-indigo-600 hover:text-indigo-500 underline mb-2 block"
                    >
                        &larr; Back to Run History
                    </Link>
                    <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Run Details</h1>
                </div>
                <div className="flex items-center gap-3">
                    <button
                        onClick={copyRunContext}
                        aria-label="Copy run context bundle"
                        title="Copy run context bundle to clipboard"
                        className="text-gray-500 hover:text-gray-700 dark:hover:text-gray-200 border border-gray-200 dark:border-gray-700 rounded px-2.5 py-1 text-xs font-medium transition-colors"
                    >
                        {contextCopied ? "✓ Copied!" : "Copy run context"}
                    </button>
                    <RunIdentifiers
                        traceId={diagnostics?.trace_id}
                        interactionId={runId}
                        requestId={diagnostics?.request_id}
                    />
                </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <div className="lg:col-span-2 space-y-6">
                    {/* Decision Log */}
                    <div className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg shadow-sm overflow-hidden">
                        <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-800 bg-gray-50 dark:bg-gray-800/50">
                            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Decision Log</h2>
                        </div>
                        <div className="p-6">
                            {!hasDiagnostics ? (
                                <p className="text-sm text-gray-400 italic" data-testid="no-diagnostics-message">
                                    No diagnostics snapshot available for this run.
                                </p>
                            ) : !hasEvents ? (
                                <p className="text-sm text-gray-500 italic" data-testid="no-events-message">
                                    No decision events recorded for this execution.
                                </p>
                            ) : (
                                <div className="flow-root">
                                    <ul className="-mb-8">
                                        {normalizedEvents.map((item, idx: number) => (
                                            <li key={item.key}>
                                                <div className="relative pb-8">
                                                    {idx !== normalizedEvents.length - 1 ? (
                                                        <span className="absolute top-4 left-4 -ml-px h-full w-0.5 bg-gray-200 dark:bg-gray-800" aria-hidden="true" />
                                                    ) : null}
                                                    <div className="relative flex space-x-3">
                                                        <div>
                                                            <span className="h-8 w-8 rounded-full bg-indigo-50 dark:bg-indigo-900/30 flex items-center justify-center ring-8 ring-white dark:ring-gray-900">
                                                                <span className="text-[10px] font-bold text-indigo-600 dark:text-indigo-400 uppercase tracking-tighter">
                                                                    {item.event.node?.slice(0, 2)}
                                                                </span>
                                                            </span>
                                                        </div>
                                                        <div className="min-w-0 flex-1 pt-1.5 flex justify-between space-x-4">
                                                            <div>
                                                                <p className="text-sm text-gray-900 dark:text-gray-100 font-medium">
                                                                    {item.event.decision}
                                                                </p>
                                                                {item.event.reason && (
                                                                    <p className="mt-1 text-sm text-gray-500 dark:text-gray-400 italic">
                                                                        {item.event.reason}
                                                                    </p>
                                                                )}
                                                            </div>
                                                            <div className="text-right text-xs whitespace-nowrap text-gray-500">
                                                                {formatTimestamp(item.timestampMs, { style: "time" })}
                                                            </div>
                                                        </div>
                                                    </div>
                                                </div>
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            )}
                        </div>
                    </div>

                    {/* Full Raw Snapshot — only shown when diagnostics exist */}
                    {hasDiagnostics && (
                        <div className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg shadow-sm">
                            <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-800 flex justify-between items-center">
                                <h2 className="text-lg font-medium text-gray-900 dark:text-gray-100">
                                    Raw Diagnostics Payload
                                </h2>
                            </div>
                            <div className="p-6">
                                <div className="bg-gray-50 dark:bg-black rounded p-4 font-mono text-xs overflow-auto max-h-96 border border-gray-200 dark:border-gray-800">
                                    <pre className="text-gray-800 dark:text-gray-300">{toPrettyJson(diagnostics)}</pre>
                                </div>
                            </div>
                        </div>
                    )}
                </div>

                <div className="space-y-6">
                    {/* Metadata Card */}
                    <div className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg shadow-sm">
                        <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-800">
                            <h2 className="text-sm font-semibold text-gray-900 dark:text-gray-100 uppercase tracking-wider">Run Info</h2>
                        </div>
                        <div className="p-6 space-y-4">
                            <div>
                                <label className="block text-[10px] font-bold text-gray-400 uppercase">Input Query</label>
                                <p className="text-sm text-gray-900 dark:text-gray-100 mt-1">
                                    {runData?.user_nlq_text || <span className="italic text-gray-400">No query recorded</span>}
                                </p>
                            </div>
                            <div>
                                <label className="block text-[10px] font-bold text-gray-400 uppercase">Status</label>
                                <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium mt-1 ${STATUS_TONE_CLASSES[getInteractionStatusTone(runData?.execution_status)]}`}>
                                    {runData?.execution_status || "UNKNOWN"}
                                </span>
                            </div>
                            <div>
                                <label className="block text-[10px] font-bold text-gray-400 uppercase">Created At</label>
                                <p className="text-sm text-gray-600 dark:text-gray-300 mt-1">{formatTimestamp(runData?.created_at)}</p>
                            </div>
                            <div className="pt-2 border-t border-gray-100 dark:border-gray-800">
                                <Link
                                    to={`/admin/jobs?runId=${runId}`}
                                    className="text-xs text-indigo-600 hover:text-indigo-500 font-medium flex items-center"
                                >
                                    View Background Jobs &rarr;
                                </Link>
                            </div>
                        </div>
                    </div>

                    {/* Validation Card — gated on diagnostics existing */}
                    {hasDiagnostics ? (
                        <div className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg shadow-sm">
                            <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-800">
                                <h2 className="text-sm font-semibold text-gray-900 dark:text-gray-100 uppercase tracking-wider">Validation</h2>
                            </div>
                            <div className="p-6 space-y-4">
                                <div className="flex justify-between items-center">
                                    <span className="text-sm text-gray-500">AST Valid</span>
                                    <span className={`text-sm font-mono ${validation?.ast_valid ? 'text-green-600' : 'text-red-600'}`}>
                                        {validation?.ast_valid !== undefined ? (validation.ast_valid ? 'YES' : 'NO') : '—'}
                                    </span>
                                </div>
                                <div className="flex justify-between items-center">
                                    <span className="text-sm text-gray-500">Syntax Errors</span>
                                    <span className="text-sm font-mono">{validation?.syntax_errors?.length ?? '—'}</span>
                                </div>
                                <div className="flex justify-between items-center">
                                    <span className="text-sm text-gray-500">Completeness</span>
                                    <span className="text-sm font-mono">
                                        {completeness?.is_truncated !== undefined
                                            ? (completeness.is_truncated ? 'Truncated' : 'Full')
                                            : '—'}
                                    </span>
                                </div>
                            </div>
                        </div>
                    ) : (
                        <div className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg shadow-sm p-6">
                            <p className="text-sm text-gray-400 italic" data-testid="no-validation-message">
                                No diagnostics snapshot available — validation data unavailable.
                            </p>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
