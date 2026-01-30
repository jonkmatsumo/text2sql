import { useState, useEffect, useRef, useCallback } from "react";
import { OpsService, getErrorMessage } from "../api";
import { OpsJobResponse } from "../types/admin";

type JobStatus = "PENDING" | "RUNNING" | "COMPLETED" | "FAILED";

interface UseJobPollingOptions {
  jobId: string | null;
  enabled?: boolean;
  pollIntervalMs?: number;
  onComplete?: (job: OpsJobResponse) => void;
  onFailed?: (job: OpsJobResponse) => void;
}

interface UseJobPollingResult {
  job: OpsJobResponse | null;
  status: JobStatus | null;
  progress: { processed: number; total: number } | null;
  error: string | null;
  isPolling: boolean;
  refetch: () => void;
}

/**
 * Hook for polling job status with automatic cleanup.
 * Stops polling when job reaches COMPLETED or FAILED state.
 */
export function useJobPolling({
  jobId,
  enabled = true,
  pollIntervalMs = 2000,
  onComplete,
  onFailed,
}: UseJobPollingOptions): UseJobPollingResult {
  const [job, setJob] = useState<OpsJobResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isPolling, setIsPolling] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const onCompleteRef = useRef(onComplete);
  const onFailedRef = useRef(onFailed);

  // Keep callback refs up to date
  onCompleteRef.current = onComplete;
  onFailedRef.current = onFailed;

  const fetchJobStatus = useCallback(async () => {
    if (!jobId) return null;

    try {
      const status = await OpsService.getJobStatus(jobId);
      setJob(status);
      setError(null);
      return status;
    } catch (err: unknown) {
      const message = getErrorMessage(err);
      setError(message);
      console.error("Job polling error:", err);
      return null;
    }
  }, [jobId]);

  const stopPolling = useCallback(() => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
    setIsPolling(false);
  }, []);

  const refetch = useCallback(() => {
    fetchJobStatus();
  }, [fetchJobStatus]);

  useEffect(() => {
    // Clean up and reset when jobId or enabled changes
    stopPolling();
    setJob(null);
    setError(null);

    if (!jobId || !enabled) {
      return;
    }

    // Fetch immediately
    setIsPolling(true);
    fetchJobStatus().then((initialJob) => {
      // If already terminal, don't start polling
      if (initialJob?.status === "COMPLETED" || initialJob?.status === "FAILED") {
        setIsPolling(false);
        if (initialJob.status === "COMPLETED") {
          onCompleteRef.current?.(initialJob);
        } else {
          onFailedRef.current?.(initialJob);
        }
        return;
      }

      // Start polling
      intervalRef.current = setInterval(async () => {
        const status = await fetchJobStatus();
        if (status?.status === "COMPLETED") {
          stopPolling();
          onCompleteRef.current?.(status);
        } else if (status?.status === "FAILED") {
          stopPolling();
          onFailedRef.current?.(status);
        }
      }, pollIntervalMs);
    });

    return () => stopPolling();
  }, [jobId, enabled, pollIntervalMs, fetchJobStatus, stopPolling]);

  // Extract progress from job result if available
  const progress = job?.result
    ? {
        processed: Number(job.result.processed) || 0,
        total: Number(job.result.total) || 0,
      }
    : null;

  return {
    job,
    status: (job?.status as JobStatus) || null,
    progress,
    error,
    isPolling,
    refetch,
  };
}
