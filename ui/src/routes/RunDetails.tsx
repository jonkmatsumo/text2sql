import React, { useEffect, useState, useCallback } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { getDiagnostics } from "../api";
import { useToast } from "../hooks/useToast";
import { LoadingState } from "../components/common/LoadingState";
import { toPrettyJson } from "../utils/observability";

export default function RunDetails() {
    const { runId } = useParams<{ runId: string }>();
    const navigate = useNavigate();
    const { show: showToast } = useToast();
    const [isLoading, setIsLoading] = useState(true);
    const [diagnostics, setDiagnostics] = useState<any>(null);

    const fetchDetails = useCallback(async () => {
        setIsLoading(true);
        try {
            if (runId) {
                const diag = await getDiagnostics(true, runId);
                setDiagnostics(diag);
            }
        } catch (err) {
            showToast("Failed to fetch run details", "error");
        } finally {
            setIsLoading(false);
        }
    }, [runId, showToast]);

    useEffect(() => {
        fetchDetails();
    }, [fetchDetails]);

    if (isLoading) {
        return (
            <div className="flex justify-center items-center h-64">
                <LoadingState message="Loading run details..." />
            </div>
        );
    }

    return (
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            <div className="mb-6 flex justify-between items-center">
                <button
                    onClick={() => navigate(-1)}
                    className="text-sm font-medium text-indigo-600 hover:text-indigo-500 underline"
                >
                    &larr; Back to Dashboard
                </button>
                <div className="text-sm text-gray-500">
                    Run ID: <code className="bg-gray-100 dark:bg-gray-800 px-1 py-0.5 rounded border border-gray-200 dark:border-gray-700">{runId}</code>
                </div>
            </div>

            <div className="space-y-6">
                <div className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg shadow-sm">
                    <div className="px-6 py-5 border-b border-gray-200 dark:border-gray-800">
                        <h1 className="text-xl font-semibold text-gray-900 dark:text-gray-100">
                            Execution Summary
                        </h1>
                    </div>
                    <div className="p-6">
                        {diagnostics?.audit_events?.length > 0 ? (
                            <div className="space-y-4">
                                <p className="text-sm text-gray-600 dark:text-gray-400">
                                    Found {diagnostics.audit_events.length} audit events for this run.
                                </p>
                                <div className="bg-gray-50 dark:bg-black rounded p-4 font-mono text-xs overflow-auto max-h-96 border border-gray-200 dark:border-gray-800">
                                    <pre className="text-gray-800 dark:text-gray-300">{toPrettyJson(diagnostics.audit_events)}</pre>
                                </div>
                            </div>
                        ) : (
                            <p className="text-sm text-gray-500 italic">No audit events found for this run or runId mismatch.</p>
                        )}
                    </div>
                </div>

                <div className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg shadow-sm">
                    <div className="px-6 py-5 border-b border-gray-200 dark:border-gray-800 flex justify-between items-center">
                        <h2 className="text-lg font-medium text-gray-900 dark:text-gray-100">
                            System Diagnostics Snapshot
                        </h2>
                        <span className="text-xs text-gray-400">Snapshot retrieved with run context</span>
                    </div>
                    <div className="p-6">
                        <div className="bg-gray-50 dark:bg-black rounded p-4 font-mono text-xs overflow-auto max-h-screen border border-gray-200 dark:border-gray-800">
                            <pre className="text-gray-800 dark:text-gray-300">{toPrettyJson(diagnostics)}</pre>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
