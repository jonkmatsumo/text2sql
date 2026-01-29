import { useCallback, useRef, useState } from "react";
import { useOtelHealth } from "./useOtelHealth";

interface RetryConfig {
  maxRetries?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
}

interface RetryState {
  retryCount: number;
  isRetrying: boolean;
  nextRetryIn: number | null;
}

const DEFAULT_CONFIG: Required<RetryConfig> = {
  maxRetries: 3,
  baseDelayMs: 1000,
  maxDelayMs: 10000
};

/**
 * Hook for managing fetch operations with exponential backoff retry logic.
 * Integrates with OTEL health context to report success/failure.
 */
export function useRetryFetch<T>(
  fetchFn: () => Promise<T>,
  config: RetryConfig = {}
): {
  execute: () => Promise<T | null>;
  retry: () => void;
  cancel: () => void;
  state: RetryState;
  isLoading: boolean;
  error: string | null;
  data: T | null;
} {
  const { maxRetries, baseDelayMs, maxDelayMs } = { ...DEFAULT_CONFIG, ...config };

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<T | null>(null);
  const [retryState, setRetryState] = useState<RetryState>({
    retryCount: 0,
    isRetrying: false,
    nextRetryIn: null
  });

  const retryTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const countdownIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const isCancelledRef = useRef(false);

  const { reportFailure, reportSuccess } = useOtelHealth();

  const clearTimers = useCallback(() => {
    if (retryTimeoutRef.current) {
      clearTimeout(retryTimeoutRef.current);
      retryTimeoutRef.current = null;
    }
    if (countdownIntervalRef.current) {
      clearInterval(countdownIntervalRef.current);
      countdownIntervalRef.current = null;
    }
  }, []);

  const calculateDelay = useCallback(
    (attempt: number) => {
      // Exponential backoff with jitter
      const exponentialDelay = baseDelayMs * Math.pow(2, attempt);
      const jitter = Math.random() * 0.3 * exponentialDelay;
      return Math.min(exponentialDelay + jitter, maxDelayMs);
    },
    [baseDelayMs, maxDelayMs]
  );

  const execute = useCallback(async (): Promise<T | null> => {
    setIsLoading(true);
    setError(null);
    isCancelledRef.current = false;
    clearTimers();

    try {
      const result = await fetchFn();
      if (isCancelledRef.current) return null;

      setData(result);
      setRetryState({ retryCount: 0, isRetrying: false, nextRetryIn: null });
      reportSuccess();
      return result;
    } catch (err: any) {
      if (isCancelledRef.current) return null;

      const errorMessage = err.message || "Request failed";
      setError(errorMessage);
      reportFailure(errorMessage);

      // Check if we should auto-retry
      if (retryState.retryCount < maxRetries) {
        const delay = calculateDelay(retryState.retryCount);

        setRetryState((prev) => ({
          retryCount: prev.retryCount + 1,
          isRetrying: true,
          nextRetryIn: Math.ceil(delay / 1000)
        }));

        // Countdown
        countdownIntervalRef.current = setInterval(() => {
          setRetryState((prev) => ({
            ...prev,
            nextRetryIn: prev.nextRetryIn ? prev.nextRetryIn - 1 : null
          }));
        }, 1000);

        // Schedule retry
        retryTimeoutRef.current = setTimeout(() => {
          clearTimers();
          if (!isCancelledRef.current) {
            execute();
          }
        }, delay);
      }

      return null;
    } finally {
      if (!isCancelledRef.current) {
        setIsLoading(false);
      }
    }
  }, [fetchFn, retryState.retryCount, maxRetries, calculateDelay, clearTimers, reportSuccess, reportFailure]);

  const retry = useCallback(() => {
    clearTimers();
    setRetryState({ retryCount: 0, isRetrying: false, nextRetryIn: null });
    execute();
  }, [execute, clearTimers]);

  const cancel = useCallback(() => {
    isCancelledRef.current = true;
    clearTimers();
    setRetryState((prev) => ({ ...prev, isRetrying: false, nextRetryIn: null }));
  }, [clearTimers]);

  return {
    execute,
    retry,
    cancel,
    state: retryState,
    isLoading,
    error,
    data
  };
}
