import React, { createContext, useCallback, useEffect, useRef, useState } from "react";
import { otelWorkerBaseUrl } from "../config";

export interface OtelHealthState {
  isHealthy: boolean;
  lastChecked: Date | null;
  lastError: string | null;
  consecutiveFailures: number;
}

export interface OtelHealthContextValue {
  health: OtelHealthState;
  checkHealth: () => Promise<boolean>;
  reportFailure: (error: string) => void;
  reportSuccess: () => void;
}

export const OtelHealthContext = createContext<OtelHealthContextValue | null>(null);

const HEALTH_CHECK_INTERVAL = 30000; // 30 seconds
const MAX_CONSECUTIVE_FAILURES = 3;

interface OtelHealthProviderProps {
  children: React.ReactNode;
}

export function OtelHealthProvider({ children }: OtelHealthProviderProps) {
  const [health, setHealth] = useState<OtelHealthState>({
    isHealthy: true,
    lastChecked: null,
    lastError: null,
    consecutiveFailures: 0
  });

  const healthCheckInterval = useRef<ReturnType<typeof setInterval> | null>(null);

  const checkHealth = useCallback(async (): Promise<boolean> => {
    try {
      // Simple health check - try to reach the OTEL worker base endpoint
      const response = await fetch(`${otelWorkerBaseUrl}/health`, {
        method: "GET",
        signal: AbortSignal.timeout(5000) // 5 second timeout
      });

      const isHealthy = response.ok;

      setHealth((prev) => ({
        isHealthy,
        lastChecked: new Date(),
        lastError: isHealthy ? null : `HTTP ${response.status}`,
        consecutiveFailures: isHealthy ? 0 : prev.consecutiveFailures + 1
      }));

      return isHealthy;
    } catch (error: any) {
      const errorMessage = error.name === "TimeoutError"
        ? "Connection timeout"
        : error.message || "Connection failed";

      setHealth((prev) => ({
        isHealthy: false,
        lastChecked: new Date(),
        lastError: errorMessage,
        consecutiveFailures: prev.consecutiveFailures + 1
      }));

      return false;
    }
  }, []);

  const reportFailure = useCallback((error: string) => {
    setHealth((prev) => ({
      ...prev,
      isHealthy: prev.consecutiveFailures + 1 >= MAX_CONSECUTIVE_FAILURES ? false : prev.isHealthy,
      lastError: error,
      consecutiveFailures: prev.consecutiveFailures + 1
    }));
  }, []);

  const reportSuccess = useCallback(() => {
    setHealth((prev) => ({
      ...prev,
      isHealthy: true,
      lastError: null,
      consecutiveFailures: 0
    }));
  }, []);

  // Periodic health checks
  useEffect(() => {
    // Initial check
    checkHealth();

    // Set up interval
    healthCheckInterval.current = setInterval(() => {
      checkHealth();
    }, HEALTH_CHECK_INTERVAL);

    return () => {
      if (healthCheckInterval.current) {
        clearInterval(healthCheckInterval.current);
      }
    };
  }, [checkHealth]);

  return (
    <OtelHealthContext.Provider value={{ health, checkHealth, reportFailure, reportSuccess }}>
      {children}
    </OtelHealthContext.Provider>
  );
}
