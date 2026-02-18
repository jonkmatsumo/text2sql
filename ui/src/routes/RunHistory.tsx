import React, { useEffect, useState, useCallback } from "react";
import { OpsService } from "../api";
import { Interaction } from "../types/admin";
import { useToast } from "../hooks/useToast";
import { LoadingState } from "../components/common/LoadingState";
import { Link } from "react-router-dom";
import TraceLink from "../components/common/TraceLink";

export default function RunHistory() {
    const [runs, setRuns] = useState<Interaction[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [offset, setOffset] = useState(0);
    const limit = 50;
    const { show: showToast } = useToast();

    const fetchRuns = useCallback(async () => {
        setIsLoading(true);
        try {
            const data = await OpsService.listRuns(limit, offset);
            setRuns(data);
        } catch (err) {
            showToast("Failed to fetch run history", "error");
        } finally {
            setIsLoading(false);
        }
    }, [offset, showToast]);

    useEffect(() => {
        fetchRuns();
    }, [fetchRuns]);

    return (
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            <header className="mb-8">
                <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Run History</h1>
                <p className="mt-2 text-sm text-gray-600 dark:text-gray-400">
                    Browse and inspect historical agent execution runs.
                </p>
            </header>

            <div className="bg-white dark:bg-gray-900 shadow overflow-hidden sm:rounded-lg border border-gray-200 dark:border-gray-800">
                <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-800">
                    <thead className="bg-gray-50 dark:bg-gray-800">
                        <tr>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Run ID</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Query</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Created</th>
                            <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                        </tr>
                    </thead>
                    <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-800">
                        {isLoading ? (
                            <tr>
                                <td colSpan={5} className="px-6 py-12 text-center">
                                    <LoadingState message="Loading runs..." />
                                </td>
                            </tr>
                        ) : runs.length === 0 ? (
                            <tr>
                                <td colSpan={5} className="px-6 py-12 text-center text-gray-500 italic">
                                    No historical runs found.
                                </td>
                            </tr>
                        ) : (
                            runs.map((run) => (
                                <tr key={run.id} className="hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
                                    <td className="px-6 py-4 whitespace-nowrap text-sm font-mono text-gray-500">
                                        {run.id.slice(0, 8)}...
                                    </td>
                                    <td className="px-6 py-4 text-sm text-gray-900 dark:text-gray-100 max-w-xs truncate">
                                        {run.user_nlq_text}
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap">
                                        <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${run.execution_status === 'SUCCESS' ? 'bg-green-100 text-green-800' :
                                                run.execution_status === 'FAILED' ? 'bg-red-100 text-red-800' :
                                                    'bg-gray-100 text-gray-800'
                                            }`}>
                                            {run.execution_status}
                                        </span>
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                        {run.created_at ? new Date(run.created_at).toLocaleString() : '—'}
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium space-x-3">
                                        <Link
                                            to={`/admin/runs/${run.id}`}
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
                    onClick={() => setOffset(Math.max(0, offset - limit))}
                    disabled={offset === 0 || isLoading}
                    className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
                >
                    Previous
                </button>
                <div className="text-sm text-gray-700 dark:text-gray-300">
                    Showing results {offset + 1} - {offset + runs.length}
                </div>
                <button
                    onClick={() => setOffset(offset + limit)}
                    disabled={runs.length < limit || isLoading}
                    className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
                >
                    Next
                </button>
            </div>
        </div>
    );
}
