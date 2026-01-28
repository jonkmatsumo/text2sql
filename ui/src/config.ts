/**
 * Centralized configuration for the Text2SQL UI.
 * All values sourced from Vite environment variables.
 */

/** Base URL for the OTEL worker service. */
export const otelWorkerBaseUrl =
  import.meta.env.VITE_OTEL_WORKER_URL || "http://localhost:4320";

/** Base URL for the Agent service. */
export const agentServiceBaseUrl =
  import.meta.env.VITE_AGENT_SERVICE_URL || "http://localhost:8081";

/** Base URL for the UI API service. */
export const uiApiBaseUrl =
  import.meta.env.VITE_UI_API_URL || "http://localhost:8082";

/**
 * Optional Grafana base URL.
 * When set, enables "Open in Grafana" fallback links.
 * Example: "http://localhost:3001"
 */
export const grafanaBaseUrl: string | undefined =
  import.meta.env.VITE_GRAFANA_BASE_URL || undefined;

/** Returns true if Grafana integration is configured. */
export function isGrafanaConfigured(): boolean {
  return !!grafanaBaseUrl;
}

/**
 * Build a Grafana Tempo trace URL for the given trace ID.
 * Returns undefined if Grafana is not configured.
 */
export function buildGrafanaTraceUrl(traceId: string): string | undefined {
  if (!grafanaBaseUrl) return undefined;
  // Standard Grafana Tempo explore URL pattern
  return `${grafanaBaseUrl}/explore?orgId=1&left=%5B%22now-1h%22,%22now%22,%22Tempo%22,%7B%22queryType%22:%22traceql%22,%22query%22:%22${traceId}%22%7D%5D`;
}
