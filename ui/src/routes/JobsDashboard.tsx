import React, { useEffect, useState, useCallback } from "react";
import { OpsService } from "../api";
import { OpsJobResponse } from "../types/admin";
import { JobsTable } from "../components/ops/JobsTable";
import { useToast } from "../hooks/useToast";

export default function JobsDashboard() {
    const [jobs, setJobs] = useState<OpsJobResponse[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [filterType, setFilterType] = useState<string>("");
    const [filterStatus, setFilterStatus] = useState<string>("");
    const { show: showToast } = useToast();

    const fetchJobs = useCallback(async () => {
        setIsLoading(true);
        try {
            const data = await OpsService.listJobs(50, filterType || undefined, filterStatus || undefined);
            setJobs(data);
        } catch (err) {
            showToast("Failed to load jobs", "error");
        } finally {
            setIsLoading(false);
        }
    }, [filterType, filterStatus, showToast]);

    useEffect(() => {
        fetchJobs();
        const interval = setInterval(fetchJobs, 5000); // Poll every 5s
        return () => clearInterval(interval);
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
                <div className="mt-4 sm:mt-0 sm:ml-16 sm:flex-none">
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
                    <option value="CACHE_REINDEX">Cache Reindex</option>
                    <option value="PATTERN_ENRICHMENT">Pattern Enrichment</option>
                    {/* Add other types as needed */}
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
                <JobsTable jobs={jobs} isLoading={isLoading} />
            </div>
        </div>
    );
}
