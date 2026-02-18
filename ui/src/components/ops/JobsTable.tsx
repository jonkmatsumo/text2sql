import React from "react";
import { OpsJobResponse, OpsJobStatus } from "../../types/admin";

interface JobsTableProps {
    jobs: OpsJobResponse[];
    onRefresh?: () => void;
    onCancel?: (jobId: string) => void;
    isLoading?: boolean;
    cancellingJobIds?: Set<string>;
}

const statusColors: Record<string, string> = {
    PENDING: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-300",
    RUNNING: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
    CANCELLING: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
    CANCELLED: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-300",
    COMPLETED: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
    FAILED: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200"
};

export const JobsTable: React.FC<JobsTableProps> = ({ jobs, onRefresh, onCancel, isLoading, cancellingJobIds = new Set() }) => {
    return (
        <div className="overflow-x-auto shadow ring-1 ring-black ring-opacity-5 sm:rounded-lg">
            <table className="min-w-full divide-y divide-gray-300 dark:divide-gray-700">
                <thead className="bg-gray-50 dark:bg-gray-900">
                    <tr>
                        <th scope="col" className="py-3.5 pl-4 pr-3 text-left text-sm font-semibold text-gray-900 dark:text-gray-200 sm:pl-6">
                            Job ID
                        </th>
                        <th scope="col" className="px-3 py-3.5 text-left text-sm font-semibold text-gray-900 dark:text-gray-200">
                            Type
                        </th>
                        <th scope="col" className="px-3 py-3.5 text-left text-sm font-semibold text-gray-900 dark:text-gray-200">
                            Status
                        </th>
                        <th scope="col" className="px-3 py-3.5 text-left text-sm font-semibold text-gray-900 dark:text-gray-200">
                            Started
                        </th>
                        <th scope="col" className="px-3 py-3.5 text-left text-sm font-semibold text-gray-900 dark:text-gray-200">
                            Duration
                        </th>
                        <th scope="col" className="relative py-3.5 pl-3 pr-4 sm:pr-6">
                            <span className="sr-only">Actions</span>
                            {onRefresh && (
                                <button
                                    onClick={onRefresh}
                                    disabled={isLoading}
                                    className="text-indigo-600 hover:text-indigo-900 dark:text-indigo-400 dark:hover:text-indigo-300 disabled:opacity-50"
                                >
                                    Refresh
                                </button>
                            )}
                        </th>
                    </tr>
                </thead>
                <tbody className="divide-y divide-gray-200 dark:divide-gray-800 bg-white dark:bg-black">
                    {jobs.length === 0 ? (
                        <tr>
                            <td colSpan={6} className="py-4 text-center text-sm text-gray-500 dark:text-gray-400">
                                No jobs found
                            </td>
                        </tr>
                    ) : (
                        jobs.map((job) => (
                            <tr key={job.id}>
                                <td className="whitespace-nowrap py-4 pl-4 pr-3 text-sm font-medium text-gray-900 dark:text-gray-200 sm:pl-6">
                                    {job.id.slice(0, 8)}...
                                </td>
                                <td className="whitespace-nowrap px-3 py-4 text-sm text-gray-500 dark:text-gray-400">
                                    {job.job_type}
                                </td>
                                <td className="whitespace-nowrap px-3 py-4 text-sm">
                                    <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${statusColors[job.status] || "bg-gray-100 text-gray-800"}`}>
                                        {job.status}
                                    </span>
                                </td>
                                <td className="whitespace-nowrap px-3 py-4 text-sm text-gray-500 dark:text-gray-400">
                                    {new Date(job.started_at).toLocaleString()}
                                </td>
                                <td className="whitespace-nowrap px-3 py-4 text-sm text-gray-500 dark:text-gray-400">
                                    {job.finished_at ? (
                                        `${((new Date(job.finished_at).getTime() - new Date(job.started_at).getTime()) / 1000).toFixed(1)}s`
                                    ) : (
                                        "-"
                                    )}
                                </td>
                                <td className="relative whitespace-nowrap py-4 pl-3 pr-4 text-right text-sm font-medium sm:pr-6">
                                    {job.status === "RUNNING" && onCancel && (
                                        <button
                                            onClick={() => onCancel(job.id)}
                                            disabled={cancellingJobIds.has(job.id)}
                                            className="text-red-600 hover:text-red-900 dark:text-red-400 dark:hover:text-red-300 disabled:opacity-50 disabled:cursor-not-allowed"
                                        >
                                            {cancellingJobIds.has(job.id) ? "Cancelling..." : "Cancel"}
                                        </button>
                                    )}
                                </td>
                            </tr>
                        ))
                    )}
                </tbody>
            </table>
        </div>
    );
};
